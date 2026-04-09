"""
Silver job: MRI equipment indicators (CNES EQ) + population + per-capita metrics.

Goal
----
From the Bronze CNES equipment Delta table, filter MRI equipment (CODEQUIP = '42'),
then compute:

1) avg_mri_per_cnes_year_state:
   Average monthly MRI count per CNES, per year, per state.

2) mri_state_year:
   Aggregate to state-year and compute:
   - avg_mri_per_cnes: average across CNES (of their yearly average)
   - total_mri_avg: sum across CNES (of their yearly average)  [optional but useful]
   - cnes_count: number of CNES contributing

3) Join with Silver population (state-year) and compute:
   - mri_per_capita = avg_mri_per_cnes / populacao
     (as requested: division between average MRI per state per year and population)

Inputs
------
Bronze Delta:
- lakehouse/bronze/cnes_eq
  Expected columns at minimum:
  - estado (UF sigla)
  - ano (int)
  - mes (string or int)
  - CNES
  - CODEQUIP
  - QT_EXIST (equipment count)

Silver Delta:
- lakehouse/silver/population
  columns: uf, Ano, populacao

Outputs
-------
Silver Delta:
- lakehouse/silver/mri_br

Schema (output)
---------------
- estado (string)
- ano (int)
- cnes_count (long)
- avg_mri_per_cnes (double)
- total_mri_avg (double)
- populacao (long)
- mri_per_capita (double)

Run
---
  PYTHONPATH=. python app/src/silver/mri_br.py
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, Window, functions as F

from app.src.silver.common import LakehousePaths, build_delta_spark

BRONZE_CNES_EQ_PATH = "lakehouse/bronze/cnes_eq"
SILVER_POPULATION_PATH = "lakehouse/silver/population"

MRI_CODEQUIP = "42"


def build_mri_state_year(
    *,
    df_cnes_eq: DataFrame,
    df_population: DataFrame,
    per_capita_scale_pow10: int = 6,
) -> DataFrame:
    """
    Build state-year MRI metrics joined with population.

    Visual scaling
    --------------
    mri_per_capita is typically very small (equipment per person). For visualization,
    we also provide a scaled version:

      mri_per_capita_scaled = mri_per_capita * 10**per_capita_scale_pow10

    Default is per 1 million people (10^6), i.e., "MRI por 1M habitantes".

    Bulletproofing / completeness
    -----------------------------
    The CNES equipment table may have missing MRI rows for a given UF/year (e.g., no MRI equipment).
    For mapping, we still want a complete UF×Ano grid with zeros (not missing rows), so all 27 UFs
    appear in the choropleth.

    Approach:
    - Compute MRI metrics where MRI exists.
    - Build a complete (estado, ano) grid from population (which should already be complete).
    - Left join metrics onto that grid and fill missing metrics with 0.

    Definition notes
    ----------------
    - QT_EXIST is treated as the monthly count of equipment existing for that CNES.
    - First average across months within the year, for each (estado, ano, CNES).
    - Then compute the *average across CNES* for that (estado, ano).

    mri_per_capita = avg_mri_per_cnes / populacao
    """
    # Normalize types + filter MRI
    # In pysus CNES equipment, QT_EXIST is the *quantity of equipment existing* for that CNES in that month.
    # This is the column we want to average across months (within a year) because equipment count can vary
    # month-to-month (e.g., 2→3 MRIs).
    df_mri = (
        df_cnes_eq.select(
            F.col("estado").cast("string").alias("estado"),
            F.col("ano").cast("int").alias("ano"),
            F.col("mes").cast("string").alias("mes"),
            F.col("CNES").cast("string").alias("cnes"),
            F.col("CODEQUIP").cast("string").alias("codequip"),
            F.col("QT_EXIST").cast("double").alias("qt_exist"),
        )
        .where(F.col("codequip") == F.lit(MRI_CODEQUIP))
        .where(F.col("qt_exist").isNotNull())
    )

    # Average monthly MRI count for each CNES, state, year
    df_cnes_year = (
        df_mri.groupBy("estado", "ano", "cnes")
        .agg(F.avg("qt_exist").alias("avg_mri_cnes_year"))
        .where(F.col("avg_mri_cnes_year").isNotNull())
    )

    # State-year aggregates (only where MRI exists)
    #
    # Interpretation:
    # - avg_mri_cnes_year is the average monthly equipment count for a CNES in that year.
    # - Summing across CNES gives an estimate of the average total MRI equipment in the UF for that year.
    df_state_year_metrics = df_cnes_year.groupBy("estado", "ano").agg(
        F.countDistinct("cnes").cast("long").alias("cnes_count"),
        F.sum("avg_mri_cnes_year").alias("total_mri_avg"),
    )

    # For reference only: mean equipment per CNES in the UF-year
    df_state_year_metrics = df_state_year_metrics.withColumn(
        "avg_mri_per_cnes",
        F.when(F.col("cnes_count") == 0, F.lit(0.0)).otherwise(F.col("total_mri_avg") / F.col("cnes_count")),
    )

    # Population provides the complete UF×Ano grid we want to keep.
    # population uses columns (uf, Ano)
    df_grid = df_population.select(
        F.col("uf").cast("string").alias("estado"),
        F.col("Ano").cast("int").alias("ano"),
        F.col("populacao").cast("long").alias("populacao"),
    )

    # Left-join metrics into the complete grid and fill missing metrics with 0.
    df_out = df_grid.join(df_state_year_metrics, on=["estado", "ano"], how="left").fillna(
        {"cnes_count": 0, "avg_mri_per_cnes": 0.0, "total_mri_avg": 0.0}
    )

    # Per-capita should use the UF total (sum of CNES-year averages), not the mean per CNES.
    df_out = df_out.withColumn(
        "mri_per_capita",
        F.when(F.col("populacao").isNull() | (F.col("populacao") == 0), F.lit(None).cast("double")).otherwise(
            F.col("total_mri_avg") / F.col("populacao")
        ),
    )

    scale_factor = F.pow(F.lit(10.0), F.lit(per_capita_scale_pow10).cast("double"))
    df_out = df_out.withColumn("mri_per_capita_scaled", F.col("mri_per_capita") * scale_factor)

    return df_out.select(
        "estado",
        "ano",
        "cnes_count",
        "avg_mri_per_cnes",
        "total_mri_avg",
        "populacao",
        "mri_per_capita",
        "mri_per_capita_scaled",
        F.lit(per_capita_scale_pow10).cast("int").alias("mri_per_capita_scale_pow10"),
    ).orderBy("estado", "ano")


def write_mri_br(
    *,
    lakehouse_root: Path = Path("lakehouse"),
    mode: str = "overwrite",
    partition_by: tuple[str, ...] = ("ano",),
) -> None:
    spark = build_delta_spark("silver-mri-br")
    paths = LakehousePaths(lakehouse_root=lakehouse_root)

    df_cnes_eq = spark.read.format("delta").load(BRONZE_CNES_EQ_PATH)
    df_population = spark.read.format("delta").load(SILVER_POPULATION_PATH)

    df_out = build_mri_state_year(df_cnes_eq=df_cnes_eq, df_population=df_population)

    paths.silver_root.mkdir(parents=True, exist_ok=True)
    out_path = paths.silver_root / "mri_br"

    (
        df_out.write.format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .partitionBy(*partition_by)
        .save(str(out_path))
    )

    # Basic sanity log
    stats = (
        df_out.agg(
            F.min("ano").alias("min_ano"),
            F.max("ano").alias("max_ano"),
            F.countDistinct("estado").alias("ufs"),
            F.count("*").alias("rows"),
            F.sum(F.when(F.col("populacao").isNull(), 1).otherwise(0)).alias("null_pop_rows"),
        )
        .collect()[0]
    )
    print(
        "WROTE_SILVER_DELTA",
        str(out_path),
        "YEARS",
        int(stats["min_ano"]),
        int(stats["max_ano"]),
        "UFS",
        int(stats["ufs"]),
        "ROWS",
        int(stats["rows"]),
        "ROWS_WITH_NULL_POP",
        int(stats["null_pop_rows"]),
    )

    spark.stop()


if __name__ == "__main__":
    write_mri_br()
