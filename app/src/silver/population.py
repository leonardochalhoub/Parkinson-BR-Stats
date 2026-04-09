"""
Silver job: population (UF x Year) panel with linear interpolation/extrapolation.

This builds a complete UF×Year panel from IBGE/SIDRA Bronze JSON payload
and writes it as a Delta table in the Silver layer.

Origin:
- Logic adapted from https://github.com/leonardochalhoub/getPBFData
  (population UF×year panel with linear interpolation for missing years).

Input (Bronze Delta):
- lakehouse/bronze/ibge/sidra_agregados_6579_var_9324_populacao_uf_raw_json
  Expected schema includes: payload_json (string), containing SIDRA JSON response.

Output (Silver Delta):
- lakehouse/silver/population
Schema:
- Ano (int)
- uf (string, 2-letter)
- populacao (long)  # rounded integer

Run:
  PYTHONPATH=. python app/src/silver/population.py
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

from pyspark.sql import DataFrame, SparkSession, Window, functions as F, types as T

from app.src.silver.common import LakehousePaths, build_delta_spark

BRONZE_SIDRA_PATH = "lakehouse/bronze/ibge/sidra_agregados_6579_var_9324_populacao_uf_raw_json"


def _uf_id_to_sigla_map() -> dict[str, str]:
    # Same mapping used in getPBFData / IBGE UF codes (2-digit).
    return {
        "11": "RO",
        "12": "AC",
        "13": "AM",
        "14": "RR",
        "15": "PA",
        "16": "AP",
        "17": "TO",
        "21": "MA",
        "22": "PI",
        "23": "CE",
        "24": "RN",
        "25": "PB",
        "26": "PE",
        "27": "AL",
        "28": "SE",
        "29": "BA",
        "31": "MG",
        "32": "ES",
        "33": "RJ",
        "35": "SP",
        "41": "PR",
        "42": "SC",
        "43": "RS",
        "50": "MS",
        "51": "MT",
        "52": "GO",
        "53": "DF",
    }


def _extract_rows_from_sidra_json(ibge_json: list[dict]) -> list[tuple[int, str, float]]:
    """
    Extract (Ano, uf, populacao) rows from SIDRA response JSON.

    The Bronze payload is the full JSON response; we navigate:
    resultados -> series -> localidade{id} + serie{ano: valor}.
    """
    uf_id_to_sigla = _uf_id_to_sigla_map()

    out_rows: list[tuple[int, str, float]] = []
    for item in ibge_json or []:
        for res in item.get("resultados", []):
            for s in res.get("series", []):
                loc = s.get("localidade", {}) or {}
                uf_id = str(loc.get("id", "")).strip()
                uf = uf_id_to_sigla.get(uf_id)
                if not uf:
                    continue

                serie = s.get("serie", {}) or {}
                for ano_str, val_str in serie.items():
                    if val_str in (None, "", "..."):
                        continue
                    try:
                        ano = int(str(ano_str))
                        pop = float(str(val_str))
                    except Exception:
                        continue
                    out_rows.append((ano, uf, pop))

    return out_rows


def _rows_to_df(spark: SparkSession, rows: Iterable[tuple[int, str, float]]) -> DataFrame:
    schema = T.StructType(
        [
            T.StructField("Ano", T.IntegerType(), False),
            T.StructField("uf", T.StringType(), False),
            T.StructField("populacao", T.DoubleType(), True),
        ]
    )
    return spark.createDataFrame(list(rows), schema=schema)


def _infer_min_year_from_bronze_payload(payload: list[dict]) -> int:
    years: list[int] = []
    for item in payload or []:
        for res in item.get("resultados", []):
            for s in res.get("series", []):
                serie = s.get("serie", {}) or {}
                for ano_str, val_str in serie.items():
                    if val_str in (None, "", "..."):
                        continue
                    try:
                        years.append(int(str(ano_str)))
                    except Exception:
                        pass
    if not years:
        raise RuntimeError("Could not infer minimum year from Bronze payload JSON")
    return min(years)


def build_population(*, spark: SparkSession, end_year: int = 2025) -> DataFrame:
    """
    Build a complete UF×Year population panel up to end_year.

    Filling rules (as in getPBFData):
    - linear interpolation between known years
    - linear extrapolation for edge gaps (use nearest known value)
    """
    df_bronze = spark.read.format("delta").load(BRONZE_SIDRA_PATH)

    # Expect exactly one row with the JSON payload.
    payload_row = df_bronze.select("payload_json").limit(1).collect()
    if not payload_row:
        raise RuntimeError(f"No rows found in Bronze table at {BRONZE_SIDRA_PATH}")
    payload_str = payload_row[0][0]
    if payload_str is None or str(payload_str).strip() == "":
        raise RuntimeError(f"Empty payload_json in Bronze table at {BRONZE_SIDRA_PATH}")

    payload = json.loads(payload_str)

    start_year = _infer_min_year_from_bronze_payload(payload)

    rows = _extract_rows_from_sidra_json(payload)
    if not rows:
        raise RuntimeError("No (Ano, uf, populacao) rows extracted from SIDRA payload")

    df_pop_raw = _rows_to_df(spark, rows)

    # Keep within [start_year, end_year]
    df_pop_raw = df_pop_raw.where((F.col("Ano") >= F.lit(start_year)) & (F.col("Ano") <= F.lit(end_year)))

    # Full year grid for each UF
    df_years = spark.range(start_year, end_year + 1).select(F.col("id").cast("int").alias("Ano"))
    df_ufs = df_pop_raw.select("uf").distinct()
    df_grid = df_ufs.crossJoin(df_years)

    df_pop = df_grid.join(df_pop_raw, on=["uf", "Ano"], how="left")

    # For each missing value, look for nearest known value on the left and right.
    w_left = (
        Window.partitionBy("uf")
        .orderBy(F.col("Ano").asc())
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )
    w_right = (
        Window.partitionBy("uf")
        .orderBy(F.col("Ano").asc())
        .rowsBetween(Window.currentRow, Window.unboundedFollowing)
    )

    df_pop = df_pop.withColumn(
        "_ano_left",
        F.last(F.when(F.col("populacao").isNotNull(), F.col("Ano")), ignorenulls=True).over(w_left),
    ).withColumn("_pop_left", F.last("populacao", ignorenulls=True).over(w_left))

    df_pop = df_pop.withColumn(
        "_ano_right",
        F.first(F.when(F.col("populacao").isNotNull(), F.col("Ano")), ignorenulls=True).over(w_right),
    ).withColumn("_pop_right", F.first("populacao", ignorenulls=True).over(w_right))

    df_pop = df_pop.withColumn(
        "populacao_filled",
        F.when(F.col("populacao").isNotNull(), F.col("populacao"))
        .when(
            (F.col("_ano_left").isNotNull())
            & (F.col("_ano_right").isNotNull())
            & (F.col("_ano_right") != F.col("_ano_left")),
            F.col("_pop_left")
            + (F.col("Ano") - F.col("_ano_left"))
            * (F.col("_pop_right") - F.col("_pop_left"))
            / (F.col("_ano_right") - F.col("_ano_left")),
        )
        .when(F.col("_pop_left").isNotNull(), F.col("_pop_left"))
        .when(F.col("_pop_right").isNotNull(), F.col("_pop_right"))
        .otherwise(F.lit(None).cast("double")),
    )

    return (
        df_pop.select("Ano", "uf", F.col("populacao_filled").alias("populacao"))
        .withColumn("populacao", F.round(F.col("populacao"), 0).cast("long"))
        .orderBy("uf", "Ano")
    )


def write_population(
    *,
    lakehouse_root: Path = Path("lakehouse"),
    end_year: int = 2025,
    mode: str = "overwrite",
    partition_by: tuple[str, ...] = ("Ano",),
) -> None:
    spark = build_delta_spark("silver-population")
    paths = LakehousePaths(lakehouse_root=lakehouse_root)

    df_out = build_population(spark=spark, end_year=end_year)

    paths.silver_root.mkdir(parents=True, exist_ok=True)
    (
        df_out.write.format("delta")
        .mode(mode)
        .partitionBy(*partition_by)
        .save(str(paths.silver_root / "population"))
    )

    # Basic sanity log
    stats = (
        df_out.agg(
            F.min("Ano").alias("min_ano"),
            F.max("Ano").alias("max_ano"),
            F.countDistinct("uf").alias("ufs"),
            F.count("*").alias("rows"),
            F.sum(F.when(F.col("populacao").isNull(), 1).otherwise(0)).alias("null_pop"),
        )
        .collect()[0]
    )
    print(
        "WROTE_SILVER_DELTA",
        str(paths.silver_root / "population"),
        "YEARS",
        int(stats["min_ano"]),
        int(stats["max_ano"]),
        "UFS",
        int(stats["ufs"]),
        "ROWS",
        int(stats["rows"]),
        "NULL_POP",
        int(stats["null_pop"]),
    )

    spark.stop()


if __name__ == "__main__":
    write_population()
