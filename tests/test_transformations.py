"""Behavior tests for the side-effect-free Spark transformations."""

from __future__ import annotations

from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from taxi_etl.transformations import (
    build_analytics_tables,
    clean_taxi_trips,
    clean_taxi_zones,
    enrich_trips_with_zones,
)

TRIP_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType()),
        StructField("tpep_pickup_datetime", TimestampType()),
        StructField("tpep_dropoff_datetime", TimestampType()),
        StructField("passenger_count", IntegerType()),
        StructField("trip_distance", DoubleType()),
        StructField("RatecodeID", IntegerType()),
        StructField("store_and_fwd_flag", StringType()),
        StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()),
        StructField("payment_type", IntegerType()),
        StructField("fare_amount", DoubleType()),
        StructField("extra", DoubleType()),
        StructField("mta_tax", DoubleType()),
        StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()),
        StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("congestion_surcharge", DoubleType()),
        StructField("Airport_fee", DoubleType()),
    ]
)

ZONE_SCHEMA = "LocationID int, Borough string, Zone string, service_zone string"


def trip_row(**overrides):
    row = {
        "VendorID": 1,
        "tpep_pickup_datetime": datetime(2024, 1, 15, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 15, 8, 20),
        "passenger_count": 2,
        "trip_distance": 4.0,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "PULocationID": 10,
        "DOLocationID": 20,
        "payment_type": 1,
        "fare_amount": 20.0,
        "extra": 1.0,
        "mta_tax": 0.5,
        "tip_amount": 4.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": 26.5,
        "congestion_surcharge": 0.0,
        "Airport_fee": 0.0,
    }
    row.update(overrides)
    return row


def test_clean_taxi_trips_applies_defaults_and_quality_rules(spark: SparkSession) -> None:
    valid = trip_row(passenger_count=None)
    source = spark.createDataFrame(
        [
            valid,
            valid,
            trip_row(passenger_count=1),
            trip_row(trip_distance=0.0),
            trip_row(tpep_pickup_datetime=datetime(2024, 2, 1, 8, 0)),
            trip_row(
                tpep_pickup_datetime=datetime(2024, 1, 15, 9, 0),
                tpep_dropoff_datetime=datetime(2024, 1, 15, 8, 0),
            ),
        ],
        TRIP_SCHEMA,
    )

    result = clean_taxi_trips(source, date(2024, 1, 1), date(2024, 2, 1)).collect()

    assert len(result) == 1
    assert result[0].passenger_count == 1
    assert result[0].pickup_date == date(2024, 1, 15)
    assert result[0].trip_duration_minutes == 20.0
    assert result[0].year == 2024
    assert result[0].month == 1


def test_clean_taxi_trips_explains_missing_source_columns(spark: SparkSession) -> None:
    source = spark.createDataFrame([(1,)], "VendorID int")

    with pytest.raises(ValueError, match="raw taxi trips is missing required column"):
        clean_taxi_trips(source, date(2024, 1, 1), date(2024, 2, 1))


def test_clean_taxi_zones_trims_deduplicates_and_removes_blanks(
    spark: SparkSession,
) -> None:
    source = spark.createDataFrame(
        [
            (10, " Queens ", " JFK Airport ", "Airports"),
            (10, " Queens ", " JFK Airport ", "Airports"),
            (20, "Manhattan", "Midtown", "Yellow Zone"),
            (30, "", "Unknown", "Boro Zone"),
        ],
        ZONE_SCHEMA,
    )

    result = {row.location_id: row for row in clean_taxi_zones(source).collect()}

    assert set(result) == {10, 20}
    assert result[10].borough == "Queens"
    assert result[10].zone == "JFK Airport"


def test_enrichment_and_analytics_tables_have_clear_business_outputs(
    spark: SparkSession,
) -> None:
    raw_trips = spark.createDataFrame(
        [
            trip_row(),
            trip_row(
                tpep_pickup_datetime=datetime(2024, 1, 15, 10, 0),
                tpep_dropoff_datetime=datetime(2024, 1, 15, 10, 10),
                passenger_count=1,
                trip_distance=0.5,
                payment_type=2,
                fare_amount=8.0,
                tip_amount=0.0,
                total_amount=9.5,
            ),
        ],
        TRIP_SCHEMA,
    )
    raw_zones = spark.createDataFrame(
        [
            (10, "Queens", "JFK Airport", "Airports"),
            (20, "Manhattan", "Midtown", "Yellow Zone"),
        ],
        ZONE_SCHEMA,
    )
    clean_trips = clean_taxi_trips(
        raw_trips,
        date(2024, 1, 1),
        date(2024, 2, 1),
    )
    clean_zones = clean_taxi_zones(raw_zones)

    enriched = enrich_trips_with_zones(clean_trips, clean_zones)
    tables = build_analytics_tables(spark, enriched)

    assert set(tables) == {
        "daily_revenue",
        "pickup_zone_summary",
        "payment_method_summary",
        "distance_band_summary",
        "monthly_summary",
        "borough_trip_summary",
    }
    daily = tables["daily_revenue"].first()
    assert daily is not None
    assert daily.trip_count == 2
    assert daily.total_revenue == 36.0
    assert {row.payment_method for row in tables["payment_method_summary"].collect()} == {
        "Cash",
        "Credit card",
    }
    assert [row.distance_band_order for row in tables["distance_band_summary"].collect()] == [
        1,
        3,
    ]
