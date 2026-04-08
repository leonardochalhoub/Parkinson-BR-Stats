"""
Shared helpers for Silver transformations.

Copied/adapted from https://github.com/leonardochalhoub/getPBFData (no dependency),
then simplified for this Parkinson-BR-Stats repository.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@dataclass(frozen=True)
class LakehousePaths:
    """
    Canonical lakehouse layout.

    All lakehouse data is local-only and gitignored.
    """

    lakehouse_root: Path = Path("lakehouse")

    @property
    def bronze_root(self) -> Path:
        return self.lakehouse_root / "bronze"

    @property
    def silver_root(self) -> Path:
        return self.lakehouse_root / "silver"

    @property
    def gold_root(self) -> Path:
        return self.lakehouse_root / "gold"


def build_delta_spark(app_name: str) -> SparkSession:
    """
    Build a local SparkSession configured for Delta Lake.

    Notes:
    - This assumes `delta` is available (delta-spark / delta pip package).
    - Tuned for local runs (driver memory + fewer shuffle partitions).
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.memory", "8g")
        .config("spark.sql.shuffle.partitions", "64")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
