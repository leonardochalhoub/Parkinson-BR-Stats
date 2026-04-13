"""
Silver job: MRI equipment indicators (CNES EQ) + population + per-capita metrics.

Goal
----
From the Bronze CNES equipment Delta table, filter MRI equipment (CODEQUIP = '42'),
then compute:

1) state-year aggregates for ALL equipment, SUS-only (IND_SUS='1'), and PRIVATE-only (IND_SUS='0'):
   - cnes_count / sus_cnes_count / priv_cnes_count
   - total_mri_avg / sus_total_mri_avg / priv_total_mri_avg
   - mri_per_capita_scaled / sus_mri_per_capita_scaled / priv_mri_per_capita_scaled

2) Join with Silver population (state-year) on a complete UF×Ano grid.

3) Export the result as a flat JSON array to the web dashboard data paths.

Inputs
------
Bronze Delta:
- lakehouse/bronze/cnes_eq
  columns used: estado, ano, mes, CNES, CODEQUIP, QT_EXIST, IND_SUS

Silver Delta:
- lakehouse/silver/population
  columns: uf, Ano, populacao

Outputs
-------
Silver Delta:
- lakehouse/silver/mri_br

JSON (for web):
- app/web/web/data/mri_br_state_year.json
- docs/data/mri_br_state_year.json

Run
---
  PYTHONPATH=. python app/src/silver/mri_br.py
"""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import DataFrame, functions as F

from app.src.silver.common import LakehousePaths, build_delta_spark

BRONZE_CNES_EQ_PATH = "lakehouse/bronze/cnes_eq"
SILVER_POPULATION_PATH = "lakehouse/silver/population"

MRI_CODEQUIP = "42"
PER_CAPITA_SCALE_POW10 = 6

JSON_OUT_PATHS = [
    "app/web/web/data/mri_br_state_year.json",
    "docs/data/mri_br_state_year.json",
]


def _agg_state_year(df_cnes_year: DataFrame, prefix: str = "") -> DataFrame:
    """
    Aggregate per-CNES yearly averages to state-year level.

    prefix=""    → columns: cnes_count, total_mri_avg
    prefix="sus_" → columns: sus_cnes_count, sus_total_mri_avg
    prefix="priv_" → columns: priv_cnes_count, priv_total_mri_avg
    """
    return df_cnes_year.groupBy("estado", "ano").agg(
        F.countDistinct("cnes").cast("long").alias(f"{prefix}cnes_count"),
        F.sum("avg_mri_cnes_year").alias(f"{prefix}total_mri_avg"),
    )


def build_mri_state_year(
    *,
    df_cnes_eq: DataFrame,
    df_population: DataFrame,
    per_capita_scale_pow10: int = PER_CAPITA_SCALE_POW10,
) -> DataFrame:
    """
    Build state-year MRI metrics (all / SUS / private) joined with population.

    Sector split
    ------------
    IND_SUS = '1'  → machine is available for SUS (public) patients
    IND_SUS = '0'  → machine is NOT available for SUS (private/non-SUS)

    Per-capita scaling
    ------------------
    Raw mri_per_capita is tiny (equipment per person).  Scaled by 10^6 to
    express "MRI per 1M inhabitants".

    Completeness
    ------------
    Population provides the complete UF×Ano grid.  Missing MRI rows (no
    equipment reported) are filled with 0 so all 27 UFs appear in the map.
    """
    # ── 1. Normalize types, filter MRI, keep IND_SUS ────────────────────────
    df_mri = (
        df_cnes_eq.select(
            F.col("estado").cast("string").alias("estado"),
            F.col("ano").cast("int").alias("ano"),
            F.col("mes").cast("string").alias("mes"),
            F.col("CNES").cast("string").alias("cnes"),
            F.col("CODEQUIP").cast("string").alias("codequip"),
            F.col("QT_EXIST").cast("double").alias("qt_exist"),
            F.col("IND_SUS").cast("string").alias("ind_sus"),
        )
        .where(F.col("codequip") == F.lit(MRI_CODEQUIP))
        .where(F.col("qt_exist").isNotNull())
    )

    # ── 2. Average monthly QT_EXIST per CNES per year ───────────────────────
    #
    # Step 1: sum QT_EXIST per (CNES, month) across all sectors.
    #   This handles the rare case where the same CNES has both a SUS row and
    #   a private row in the same month (2 SUS + 1 private → monthly total = 3).
    #
    # Step 2: count the months the CNES was active (denominator for all averages).
    #
    # Step 3: all-sector avg = avg(monthly_total) — same as before.
    #
    # Step 4: per-sector avg = sum(qt_exist for that sector) / n_months_all.
    #   Using the *total* month count (not just sector-active months) prevents
    #   overcounting for CNES that switch IND_SUS mid-year.  Example: a CNES
    #   with 10 machines that is SUS for Jan-Jun and private for Jul-Dec would
    #   otherwise get SUS avg=10 + priv avg=10 = 20, when the correct total is 10.
    #   With the shared denominator: SUS sum=60 / 12 = 5, priv sum=60 / 12 = 5,
    #   total = 10 ✓.

    # Monthly totals (all-sector)
    monthly_all = (
        df_mri.groupBy("estado", "ano", "cnes", "mes")
        .agg(F.sum("qt_exist").alias("monthly_total"))
    )

    # Number of active months per CNES-year (shared denominator)
    cnes_month_count = (
        monthly_all.groupBy("estado", "ano", "cnes")
        .agg(F.count("mes").alias("n_months"))
    )

    # All-sector annual average
    cnes_all = (
        monthly_all.groupBy("estado", "ano", "cnes")
        .agg(F.avg("monthly_total").alias("avg_mri_cnes_year"))
        .where(F.col("avg_mri_cnes_year").isNotNull())
    )

    def cnes_year_avg_sector(df_sector: DataFrame) -> DataFrame:
        """
        Per-sector annual average: sum(qt_exist over sector-active months) /
        n_months (total active months across both sectors).

        Dividing by the shared n_months instead of the sector-active month count
        guarantees that sus_avg + priv_avg == all_avg, even for CNES that change
        IND_SUS within the year.
        """
        sector_sum = (
            df_sector.groupBy("estado", "ano", "cnes")
            .agg(F.sum("qt_exist").alias("sector_sum"))
        )
        return (
            sector_sum
            .join(cnes_month_count, on=["estado", "ano", "cnes"], how="left")
            .withColumn(
                "avg_mri_cnes_year",
                F.col("sector_sum") / F.col("n_months"),
            )
            .where(F.col("avg_mri_cnes_year").isNotNull())
            .select("estado", "ano", "cnes", "avg_mri_cnes_year")
        )

    cnes_sus  = cnes_year_avg_sector(df_mri.where(F.col("ind_sus") == F.lit("1")))
    cnes_priv = cnes_year_avg_sector(df_mri.where(F.col("ind_sus") == F.lit("0")))

    # ── 3. State-year aggregates per sector ─────────────────────────────────
    # agg_cnes_count: only keep the distinct-facility count from cnes_all.
    agg_cnes_count = (
        cnes_all.groupBy("estado", "ano")
        .agg(F.countDistinct("cnes").cast("long").alias("cnes_count"))
    )
    agg_sus  = _agg_state_year(cnes_sus,  prefix="sus_")
    agg_priv = _agg_state_year(cnes_priv, prefix="priv_")

    # ── 4. Complete UF×Ano grid from population ──────────────────────────────
    df_grid = df_population.select(
        F.col("uf").cast("string").alias("estado"),
        F.col("Ano").cast("int").alias("ano"),
        F.col("populacao").cast("long").alias("populacao"),
    )

    # ── 5. Left-join all sectors onto the grid; fill missing with 0 ──────────
    fill_zeros = {
        "cnes_count": 0,
        "sus_cnes_count": 0, "sus_total_mri_avg": 0.0,
        "priv_cnes_count": 0, "priv_total_mri_avg": 0.0,
    }
    df_out = (
        df_grid
        .join(agg_cnes_count, on=["estado", "ano"], how="left")
        .join(agg_sus,  on=["estado", "ano"], how="left")
        .join(agg_priv, on=["estado", "ano"], how="left")
        .fillna(fill_zeros)
    )

    # ── 5b. Derive total_mri_avg as sus + priv ───────────────────────────────
    # Because both sector averages share the same monthly denominator (step 2),
    # sus + priv == all-sector total for every CNES, so the sum is exact.
    df_out = df_out.withColumn(
        "total_mri_avg",
        F.col("sus_total_mri_avg") + F.col("priv_total_mri_avg"),
    )

    # ── 6. Per-capita (scaled) for each sector ───────────────────────────────
    scale = F.pow(F.lit(10.0), F.lit(per_capita_scale_pow10).cast("double"))
    pop = F.col("populacao")

    def per_capita_scaled(total_col: str) -> F.Column:
        return F.when(
            pop.isNull() | (pop == 0), F.lit(0.0)
        ).otherwise(F.col(total_col) / pop * scale)

    df_out = (
        df_out
        .withColumn("mri_per_capita_scaled", per_capita_scaled("total_mri_avg"))
        .withColumn("sus_mri_per_capita_scaled", per_capita_scaled("sus_total_mri_avg"))
        .withColumn("priv_mri_per_capita_scaled", per_capita_scaled("priv_total_mri_avg"))
        .withColumn("mri_per_capita_scale_pow10", F.lit(per_capita_scale_pow10).cast("int"))
    )

    return df_out.select(
        "estado", "ano",
        # all
        "cnes_count", "total_mri_avg", "mri_per_capita_scaled",
        # SUS
        "sus_cnes_count", "sus_total_mri_avg", "sus_mri_per_capita_scaled",
        # private
        "priv_cnes_count", "priv_total_mri_avg", "priv_mri_per_capita_scaled",
        # population + scale metadata
        "populacao", "mri_per_capita_scale_pow10",
    ).orderBy("estado", "ano")


def write_delta(df_out: DataFrame, *, lakehouse_root: Path, mode: str = "overwrite") -> None:
    paths = LakehousePaths(lakehouse_root=lakehouse_root)
    paths.silver_root.mkdir(parents=True, exist_ok=True)
    out_path = paths.silver_root / "mri_br"

    (
        df_out.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .partitionBy("ano")
        .save(str(out_path))
    )

    stats = (
        df_out.agg(
            F.min("ano").alias("min_ano"),
            F.max("ano").alias("max_ano"),
            F.countDistinct("estado").alias("ufs"),
            F.count("*").alias("rows"),
        )
        .collect()[0]
    )
    print(
        "WROTE_SILVER_DELTA", str(out_path),
        "YEARS", int(stats["min_ano"]), int(stats["max_ano"]),
        "UFS", int(stats["ufs"]),
        "ROWS", int(stats["rows"]),
    )


def export_json(df_out: DataFrame) -> None:
    """Write the silver DataFrame as a flat JSON array to all web data paths."""
    # Collect to driver, serialize manually to control float precision.
    rows = df_out.collect()

    records = []
    for r in rows:
        rec = r.asDict()
        # Round scaled floats to avoid 15-digit noise
        for key in ("mri_per_capita_scaled", "sus_mri_per_capita_scaled",
                    "priv_mri_per_capita_scaled", "total_mri_avg",
                    "sus_total_mri_avg", "priv_total_mri_avg"):
            if rec.get(key) is not None:
                rec[key] = round(float(rec[key]), 10)
        records.append(rec)

    json_bytes = json.dumps(records, ensure_ascii=False).encode("utf-8")

    for out_path in JSON_OUT_PATHS:
        p = Path(out_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(json_bytes)
        print(f"JSON written → {out_path} ({p.stat().st_size / 1024:.1f} KB, {len(records)} rows)")


def run(lakehouse_root: Path = Path("lakehouse")) -> None:
    spark = build_delta_spark("silver-mri-br")

    df_cnes_eq = spark.read.format("delta").load(BRONZE_CNES_EQ_PATH)
    df_population = spark.read.format("delta").load(SILVER_POPULATION_PATH)

    df_out = build_mri_state_year(df_cnes_eq=df_cnes_eq, df_population=df_population)

    write_delta(df_out, lakehouse_root=lakehouse_root)
    export_json(df_out)

    spark.stop()


if __name__ == "__main__":
    run()
