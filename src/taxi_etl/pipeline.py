"""Filesystem orchestration for the four ETL stages."""

from __future__ import annotations

import logging
from collections.abc import Callable
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession

from taxi_etl.config import PipelineConfig, require_paths, spark_path
from taxi_etl.quality import ANALYTICS_CONTRACTS, assert_required_columns, validate_table
from taxi_etl.schemas import RAW_TRIP_COLUMNS, RAW_ZONE_COLUMNS, TAXI_ZONE_SCHEMA
from taxi_etl.spark import create_spark, stop_spark
from taxi_etl.transformations import (
    build_analytics_tables,
    clean_taxi_trips,
    clean_taxi_zones,
    enrich_trips_with_zones,
)

LOGGER = logging.getLogger(__name__)
MAX_RECORDS_PER_FILE = 250_000


def _write_parquet(
    dataframe: DataFrame,
    destination: Path,
    partition_columns: tuple[str, ...] = (),
) -> None:
    writer = (
        dataframe.write.mode("overwrite")
        .option("compression", "snappy")
        .option("maxRecordsPerFile", MAX_RECORDS_PER_FILE)
    )
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.parquet(spark_path(destination))


def _run_with_spark(
    config: PipelineConfig,
    stage_name: str,
    stage: Callable[[SparkSession], None],
) -> None:
    spark = create_spark(config, stage_name)
    try:
        stage(spark)
    finally:
        stop_spark(spark)


def run_ingest(config: PipelineConfig) -> None:
    """Read source files and write a stable raw Parquet layer."""

    paths = config.paths
    require_paths(
        (paths.taxi_trips_source, paths.taxi_zones_source),
        config.project_root,
    )

    def ingest(spark: SparkSession) -> None:
        trips = spark.read.parquet(spark_path(paths.taxi_trips_source))
        zones = (
            spark.read.option("header", True)
            .schema(TAXI_ZONE_SCHEMA)
            .csv(spark_path(paths.taxi_zones_source))
        )
        assert_required_columns(trips, RAW_TRIP_COLUMNS, "source taxi trips")
        assert_required_columns(zones, RAW_ZONE_COLUMNS, "source taxi zones")

        trips.persist(StorageLevel.MEMORY_AND_DISK)
        zones.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            trip_count = trips.count()
            zone_count = zones.count()
            _write_parquet(trips, paths.ingest_dir / "yellow_taxi_trips")
            _write_parquet(zones, paths.ingest_dir / "taxi_zones")
        finally:
            trips.unpersist()
            zones.unpersist()

        LOGGER.info("Ingested %s taxi trip rows", f"{trip_count:,}")
        LOGGER.info("Ingested %s taxi zone rows", f"{zone_count:,}")

    _run_with_spark(config, "Ingest", ingest)


def run_clean(config: PipelineConfig) -> None:
    """Apply type, completeness, range, and reporting-period rules."""

    paths = config.paths
    trip_input = paths.ingest_dir / "yellow_taxi_trips"
    zone_input = paths.ingest_dir / "taxi_zones"
    require_paths((trip_input, zone_input), config.project_root)

    def clean(spark: SparkSession) -> None:
        raw_trips = spark.read.parquet(spark_path(trip_input))
        raw_zones = spark.read.parquet(spark_path(zone_input))
        trips = clean_taxi_trips(
            raw_trips,
            config.period_start,
            config.next_period_start,
        )
        zones = clean_taxi_zones(raw_zones)

        trips.persist(StorageLevel.MEMORY_AND_DISK)
        zones.persist(StorageLevel.MEMORY_AND_DISK)
        try:
            trip_count = trips.count()
            zone_count = zones.count()
            if trip_count == 0:
                raise ValueError("Cleaning removed every taxi trip; check the source period")
            if zone_count == 0:
                raise ValueError("Cleaning removed every taxi zone")
            _write_parquet(
                trips,
                paths.clean_dir / "yellow_taxi_trips",
                partition_columns=("year", "month"),
            )
            _write_parquet(zones, paths.clean_dir / "taxi_zones")
        finally:
            trips.unpersist()
            zones.unpersist()

        LOGGER.info("Retained %s clean taxi trip rows", f"{trip_count:,}")
        LOGGER.info("Retained %s clean taxi zone rows", f"{zone_count:,}")

    _run_with_spark(config, "Clean", clean)


def run_transform(config: PipelineConfig) -> None:
    """Enrich clean trips and write the gold analytics tables."""

    paths = config.paths
    trip_input = paths.clean_dir / "yellow_taxi_trips"
    zone_input = paths.clean_dir / "taxi_zones"
    require_paths((trip_input, zone_input), config.project_root)

    def transform(spark: SparkSession) -> None:
        trips = spark.read.parquet(spark_path(trip_input))
        zones = spark.read.parquet(spark_path(zone_input))
        enriched_trips = enrich_trips_with_zones(trips, zones)
        enriched_trips.persist(StorageLevel.MEMORY_AND_DISK)

        try:
            tables = build_analytics_tables(spark, enriched_trips)
            for table_name, dataframe in tables.items():
                partitions = (
                    ("year", "month") if {"year", "month"}.issubset(dataframe.columns) else ()
                )
                _write_parquet(
                    dataframe,
                    paths.analytics_dir / table_name,
                    partition_columns=partitions,
                )
                LOGGER.info("Wrote analytics table: %s", table_name)
        finally:
            enriched_trips.unpersist()

    _run_with_spark(config, "Transform", transform)


def run_validate(config: PipelineConfig) -> None:
    """Read persisted analytics tables and enforce their data contracts."""

    paths = config.paths
    table_paths = tuple(paths.analytics_dir / table_name for table_name in ANALYTICS_CONTRACTS)
    require_paths(table_paths, config.project_root)

    def validate(spark: SparkSession) -> None:
        for table_name, contract in ANALYTICS_CONTRACTS.items():
            dataframe = spark.read.parquet(spark_path(paths.analytics_dir / table_name))
            result = validate_table(table_name, dataframe, contract)
            LOGGER.info(
                "Validated %s: %s rows",
                result.table_name,
                f"{result.row_count:,}",
            )

    _run_with_spark(config, "Validate", validate)


PIPELINE_STAGES: dict[str, Callable[[PipelineConfig], None]] = {
    "ingest": run_ingest,
    "clean": run_clean,
    "transform": run_transform,
    "validate": run_validate,
}


def run_pipeline(config: PipelineConfig, selected_stage: str = "all") -> None:
    """Run one stage or the complete pipeline in dependency order."""

    stages = PIPELINE_STAGES.items()
    if selected_stage != "all":
        stages = ((selected_stage, PIPELINE_STAGES[selected_stage]),)

    for stage_name, stage in stages:
        LOGGER.info("Starting %s stage", stage_name)
        stage(config)
        LOGGER.info("Completed %s stage", stage_name)
