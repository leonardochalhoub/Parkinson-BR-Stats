"""
Bronze ingestion: CNES EQ (Equipamentos) .dbc files -> Delta.

Reads all downloaded .dbc files from data/cnes_eq/ and writes them as a unified Delta table.

What this does:
- Discovers all .dbc files in data/cnes_eq/
- Converts each .dbc to pandas DataFrame
- Extracts estado, mes, ano from filename (e.g., EQTO2509.dbc -> estado='TO', ano=2025, mes='09')
- Adds source_file metadata column
- Unions all DataFrames
- Writes to Delta: lakehouse/bronze/cnes_eq

Output:
  lakehouse/bronze/cnes_eq (Delta table with estado, mes, ano columns)

Run:
  python app/src/bronze/cnes_eq_ingest_delta.py
"""

from __future__ import annotations

import pathlib
import tempfile
from typing import Iterator

import pandas as pd
from dbfread import DBF
from pyspark.sql import functions as F

from app.src.silver.common import LakehousePaths, build_delta_spark
from pysus.utilities.readdbc import dbc2dbf

# Config
DATA_DIR = pathlib.Path("data/cnes_eq")
BATCH_SIZE = 50  # Process files in batches to avoid memory issues


def list_dbc_files() -> list[pathlib.Path]:
    """Return list of all .dbc files in DATA_DIR, sorted."""
    if not DATA_DIR.exists():
        raise FileNotFoundError(f"Data directory not found: {DATA_DIR}")
    files = sorted(DATA_DIR.glob("*.dbc"))
    if not files:
        raise FileNotFoundError(f"No .dbc files found in {DATA_DIR}")
    return files


def read_dbc_as_pandas(dbc_path: pathlib.Path) -> pd.DataFrame:
    """
    Read a .dbc file into a pandas DataFrame.
    
    Process:
    1. Convert .dbc to .dbf using pysus.utilities.readdbc.dbc2dbf
    2. Read .dbf with latin-1 encoding (CNES standard)
    3. Extract estado, mes, ano from filename (e.g., EQTO2509.dbc)
    4. Add columns: source_file, estado, mes, ano
    """
    # Convert .dbc to .dbf (temporary file in system temp)
    with tempfile.TemporaryDirectory() as tmpdir:
        dbf_path = pathlib.Path(tmpdir) / dbc_path.with_suffix(".dbf").name
        dbc2dbf(str(dbc_path), str(dbf_path))
        
        # Read DBF with proper encoding
        df = pd.DataFrame(iter(DBF(str(dbf_path), encoding="latin-1")))
    
    # Extract metadata from filename: EQXX####.dbc -> XX=estado, ##=ano, ##=mes
    filename = dbc_path.stem  # e.g., "EQTO2509"
    estado = filename[2:4]  # e.g., "TO"
    yy = filename[4:6]  # e.g., "25"
    mm = filename[6:8]  # e.g., "09"
    
    ano = 2000 + int(yy)  # Convert YY to YYYY (25 -> 2025)
    
    df["source_file"] = dbc_path.name
    df["estado"] = estado
    df["mes"] = mm
    df["ano"] = ano
    return df


def process_dbc_files_batched(files: list[pathlib.Path]) -> Iterator[pd.DataFrame]:
    """
    Yield batches of concatenated DataFrames (idempotent, memory-efficient).
    
    Process files in batches to avoid loading all data at once.
    Skips files that fail to read.
    """
    for i in range(0, len(files), BATCH_SIZE):
        batch = files[i : i + BATCH_SIZE]
        dfs = []
        for fpath in batch:
            try:
                df = read_dbc_as_pandas(fpath)
                if len(df) > 0:
                    dfs.append(df)
                    print(f"  read {fpath.name} ({len(df)} rows)")
                else:
                    print(f"  skipped {fpath.name} (empty)")
            except Exception as e:
                print(f"  ERROR reading {fpath.name}: {e}")
                continue
        
        if dfs:
            batch_df = pd.concat(dfs, ignore_index=True)
            print(f"  batch has {len(batch_df)} total rows")
            yield batch_df


def main() -> None:
    print("=== CNES EQ DBC -> Delta Ingestion ===")
    
    files = list_dbc_files()
    print(f"Found {len(files)} .dbc files in {DATA_DIR}\n")
    
    spark = build_delta_spark("bronze-cnes-eq-dbc")
    paths = LakehousePaths()
    out_path = paths.bronze_root / "cnes_eq"
    
    # Create output directory
    paths.bronze_root.mkdir(parents=True, exist_ok=True)
    
    # Process files in batches and write to Delta incrementally
    first_batch = True
    total_rows = 0
    batch_count = 0
    
    for batch_df in process_dbc_files_batched(files):
        if batch_df.empty:
            print("  skipping empty batch")
            continue
        
        # Add timestamp column
        batch_df["ingested_at"] = pd.Timestamp.now()
        
        # Convert to Spark DataFrame
        try:
            spark_df = spark.createDataFrame(batch_df)
        except Exception as e:
            print(f"  ERROR converting batch to Spark DataFrame: {e}")
            continue
        
        # Add more metadata
        spark_df = spark_df.withColumn("ingested_batch_at", F.current_timestamp())
        
        # Write to Delta with schema merge enabled and partitioned by estado, ano, mes
        mode = "overwrite" if first_batch else "append"
        try:
            spark_df.write.format("delta").mode(mode).option("mergeSchema", "true").partitionBy("estado", "ano", "mes").save(str(out_path))
        except Exception as e:
            print(f"  ERROR writing batch to Delta: {e}")
            continue
        
        rows_in_batch = len(batch_df)
        total_rows += rows_in_batch
        batch_count += 1
        print(f"  ✓ batch {batch_count} written ({rows_in_batch} rows), total: {total_rows}\n")
        first_batch = False
    
    print(f"\n=== Ingestion Complete ===")
    print(f"Output Delta table: {out_path}")
    print(f"Batches written: {batch_count}")
    print(f"Total rows written: {total_rows}")
    
    spark.stop()


if __name__ == "__main__":
    main()
