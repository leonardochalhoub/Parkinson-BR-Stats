"""
CNES EQ Bronze Ingestion

Stage 1: DBC -> Parquet (parallel, ordered progress)
Stage 2: Spark reads Parquet
Stage 3: Dynamic partition overwrite to Delta
"""

from __future__ import annotations

import argparse
import pathlib
import shutil
import tempfile
import multiprocessing as mp
import io
import contextlib
import time

import pandas as pd
from dbfread import DBF
from pyspark.sql import functions as F

from app.src.silver.common import LakehousePaths, build_delta_spark
from pysus.utilities.readdbc import dbc2dbf


# =========================
# Config
# =========================

DATA_DIR = pathlib.Path("data/cnes_eq")
TEMP_PARQUET_DIR = pathlib.Path("data/_tmp_cnes_eq_parquet")


# =========================
# File Listing (ASC Ordered)
# =========================

def list_dbc_files(start_estado: str | None, only_estado: str | None):
    files = sorted(DATA_DIR.glob("*.dbc"))  # ✅ Strict ASC order

    if only_estado:
        files = [f for f in files if f.name[2:4].upper() == only_estado.upper()]
    elif start_estado:
        files = [f for f in files if f.name[2:4].upper() >= start_estado.upper()]

    return files


# =========================
# Worker
# =========================

def dbc_to_parquet(dbc_path: pathlib.Path) -> tuple[str, int, bool]:

    try:
        with tempfile.TemporaryDirectory() as tmpdir:

            dbf_path = pathlib.Path(tmpdir) / dbc_path.with_suffix(".dbf").name

            with contextlib.redirect_stdout(io.StringIO()):
                dbc2dbf(str(dbc_path), str(dbf_path))

            df = pd.DataFrame(iter(DBF(str(dbf_path), encoding="latin-1")))

        if df.empty:
            return (dbc_path.name, 0, True)

        filename = dbc_path.stem
        estado = filename[2:4]
        yy = filename[4:6]
        mm = filename[6:8]
        ano = 2000 + int(yy)

        df["source_file"] = dbc_path.name
        df["estado"] = estado
        df["mes"] = mm
        df["ano"] = ano

        out_file = TEMP_PARQUET_DIR / f"{dbc_path.stem}.parquet"
        df.to_parquet(out_file, index=False)

        return (dbc_path.name, len(df), True)

    except Exception:
        return (dbc_path.name, 0, False)


# =========================
# Main
# =========================

def main(start_estado: str | None, only_estado: str | None):

    print("\n=== CNES EQ Bronze Ingestion ===\n")

    files = list_dbc_files(start_estado, only_estado)

    if not files:
        print("No files found.")
        return

    total_files = len(files)

    print(f"Stage 1/3  |  DBC → Parquet")
    print(f"Files: {total_files}\n")

    # Reset temp dir
    if TEMP_PARQUET_DIR.exists():
        shutil.rmtree(TEMP_PARQUET_DIR)

    TEMP_PARQUET_DIR.mkdir(parents=True, exist_ok=True)

    start_time = time.time()

    success_count = 0

    # ✅ Ordered processing
    with mp.Pool(mp.cpu_count()) as pool:
        for i, result in enumerate(pool.imap(dbc_to_parquet, files), 1):

            filename, rows, success = result
            percent = (i / total_files) * 100

            if success:
                success_count += 1
                print(
                    f"[{i:>4}/{total_files} | {percent:6.2f}%] "
                    f"{filename:<15} → parquet  ({rows:,} rows)"
                )
            else:
                print(
                    f"[{i:>4}/{total_files} | {percent:6.2f}%] "
                    f"{filename:<15} → FAILED"
                )

    elapsed = time.time() - start_time

    print(f"\nDBC → Parquet completed in {elapsed:,.1f}s")
    print(f"Successful files: {success_count}/{total_files}\n")

    if success_count == 0:
        print("No data converted. Exiting.")
        return

    # =========================
    # Spark Phase
    # =========================

    print("Stage 2/3  |  Spark Read Parquet")
    spark = build_delta_spark("bronze-cnes-eq")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    df = spark.read.parquet(str(TEMP_PARQUET_DIR))
    df = df.withColumn("ingested_at", F.current_timestamp())

    print("Stage 3/3  |  Write Delta (dynamic partition overwrite)")

    paths = LakehousePaths()
    out_path = paths.bronze_root / "cnes_eq"
    paths.bronze_root.mkdir(parents=True, exist_ok=True)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy("estado", "ano", "mes")
        .save(str(out_path))
    )

    print("\n✅ Ingestion complete.")
    print(f"Delta table: {out_path}")

    spark.stop()

    shutil.rmtree(TEMP_PARQUET_DIR)


# =========================
# CLI
# =========================

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--start", dest="start_estado", default=None)
    parser.add_argument("--only", dest="only_estado", default=None)

    args = parser.parse_args()

    main(args.start_estado, args.only_estado)
