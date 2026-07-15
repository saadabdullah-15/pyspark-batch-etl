"""Reusable Spark transformations with no filesystem side effects.

Keeping business rules in functions makes the pipeline easier to test and lets a
reader follow the data one step at a time.
"""

from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_etl.quality import assert_required_columns
from taxi_etl.schemas import RAW_TRIP_COLUMNS, RAW_ZONE_COLUMNS


def clean_taxi_trips(
    trips: DataFrame,
    period_start: date,
    next_period_start: date,
) -> DataFrame:
    """Standardize raw taxi trips and remove records outside our quality rules."""

    assert_required_columns(trips, RAW_TRIP_COLUMNS, "raw taxi trips")

    standardized = trips.select(
        F.col("VendorID").cast("int").alias("vendor_id"),
        F.col("tpep_pickup_datetime").cast("timestamp").alias("pickup_at"),
        F.col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_at"),
        F.col("passenger_count").cast("int").alias("passenger_count"),
        F.col("trip_distance").cast("double").alias("trip_distance"),
        F.col("RatecodeID").cast("int").alias("rate_code_id"),
        F.col("store_and_fwd_flag").cast("string").alias("store_and_fwd_flag"),
        F.col("PULocationID").cast("int").alias("pickup_location_id"),
        F.col("DOLocationID").cast("int").alias("dropoff_location_id"),
        F.col("payment_type").cast("int").alias("payment_type"),
        F.col("fare_amount").cast("double").alias("fare_amount"),
        F.col("extra").cast("double").alias("extra_amount"),
        F.col("mta_tax").cast("double").alias("mta_tax"),
        F.col("tip_amount").cast("double").alias("tip_amount"),
        F.col("tolls_amount").cast("double").alias("tolls_amount"),
        F.col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        F.col("total_amount").cast("double").alias("total_amount"),
        F.col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
        F.col("Airport_fee").cast("double").alias("airport_fee"),
    )

    required_values = [
        "pickup_at",
        "dropoff_at",
        "pickup_location_id",
        "dropoff_location_id",
        "trip_distance",
        "fare_amount",
        "total_amount",
    ]

    with_derived_columns = (
        standardized.dropna(subset=required_values)
        .withColumn(
            "passenger_count",
            F.coalesce(F.col("passenger_count"), F.lit(1)),
        )
        .dropDuplicates()
        .withColumn("pickup_date", F.to_date("pickup_at"))
        .withColumn(
            "trip_duration_minutes",
            F.round(
                (F.col("dropoff_at").cast("long") - F.col("pickup_at").cast("long")) / F.lit(60),
                2,
            ),
        )
        .withColumn("year", F.year("pickup_date"))
        .withColumn("month", F.month("pickup_date"))
        .withColumn("day", F.dayofmonth("pickup_date"))
    )

    return with_derived_columns.filter(
        (F.col("pickup_date") >= F.lit(period_start))
        & (F.col("pickup_date") < F.lit(next_period_start))
        & (F.col("dropoff_at") > F.col("pickup_at"))
        & (F.col("trip_duration_minutes") <= F.lit(24 * 60))
        & F.col("passenger_count").between(1, 6)
        & (F.col("trip_distance") > F.lit(0))
        & (F.col("trip_distance") <= F.lit(100))
        & (F.col("fare_amount") >= F.lit(0))
        & (F.col("total_amount") >= F.lit(0))
    )


def clean_taxi_zones(zones: DataFrame) -> DataFrame:
    """Normalize taxi zone names and keep one complete row per location ID."""

    assert_required_columns(zones, RAW_ZONE_COLUMNS, "raw taxi zones")
    return (
        zones.select(
            F.col("LocationID").cast("int").alias("location_id"),
            F.trim(F.col("Borough")).alias("borough"),
            F.trim(F.col("Zone")).alias("zone"),
            F.trim(F.col("service_zone")).alias("service_zone"),
        )
        .dropna(subset=["location_id", "borough", "zone"])
        .filter((F.length("borough") > 0) & (F.length("zone") > 0))
        .dropDuplicates(["location_id"])
    )


def add_payment_method(trips: DataFrame) -> DataFrame:
    """Translate TLC payment codes into readable labels."""

    return trips.withColumn(
        "payment_method",
        F.when(F.col("payment_type") == 1, "Credit card")
        .when(F.col("payment_type") == 2, "Cash")
        .when(F.col("payment_type") == 3, "No charge")
        .when(F.col("payment_type") == 4, "Dispute")
        .when(F.col("payment_type") == 5, "Unknown")
        .when(F.col("payment_type") == 6, "Voided trip")
        .otherwise("Missing"),
    )


def enrich_trips_with_zones(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Attach pickup and drop-off zone descriptions to each clean trip."""

    trip_rows = add_payment_method(trips).alias("trip")
    pickup_zones = zones.alias("pickup")
    dropoff_zones = zones.alias("dropoff")

    return (
        trip_rows.join(
            pickup_zones,
            F.col("trip.pickup_location_id") == F.col("pickup.location_id"),
            "left",
        )
        .join(
            dropoff_zones,
            F.col("trip.dropoff_location_id") == F.col("dropoff.location_id"),
            "left",
        )
        .select(
            "trip.*",
            F.col("pickup.borough").alias("pickup_borough"),
            F.col("pickup.zone").alias("pickup_zone"),
            F.col("dropoff.borough").alias("dropoff_borough"),
            F.col("dropoff.zone").alias("dropoff_zone"),
        )
    )


def _add_distance_band(trips: DataFrame) -> DataFrame:
    return trips.withColumn(
        "distance_band_order",
        F.when(F.col("trip_distance") < 1, 1)
        .when(F.col("trip_distance") < 3, 2)
        .when(F.col("trip_distance") < 10, 3)
        .otherwise(4),
    ).withColumn(
        "distance_band",
        F.when(F.col("trip_distance") < 1, "Under 1 mile")
        .when(F.col("trip_distance") < 3, "1 to under 3 miles")
        .when(F.col("trip_distance") < 10, "3 to under 10 miles")
        .otherwise("10 miles or more"),
    )


def build_daily_revenue(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy("pickup_date", "year", "month")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("total_amount"), 2).alias("average_trip_revenue"),
            F.round(F.avg("trip_distance"), 2).alias("average_trip_distance"),
        )
        .orderBy("pickup_date")
    )


def build_pickup_zone_summary(trips: DataFrame) -> DataFrame:
    return (
        trips.filter(F.col("pickup_zone").isNotNull())
        .groupBy("pickup_borough", "pickup_zone")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("tip_amount"), 2).alias("average_tip_amount"),
            F.countDistinct("dropoff_location_id").alias("unique_dropoff_zones"),
        )
        .orderBy(F.col("total_revenue").desc())
    )


def build_payment_method_summary(trips: DataFrame) -> DataFrame:
    return (
        trips.groupBy("payment_method")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("tip_amount"), 2).alias("average_tip_amount"),
        )
        .orderBy(F.col("trip_count").desc())
    )


def build_distance_band_summary(trips: DataFrame) -> DataFrame:
    return (
        _add_distance_band(trips)
        .groupBy("distance_band_order", "distance_band")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("average_duration_minutes"),
        )
        .orderBy("distance_band_order")
    )


def build_sql_summaries(
    spark: SparkSession,
    trips: DataFrame,
) -> tuple[DataFrame, DataFrame]:
    """Build two tables with Spark SQL to demonstrate both Spark APIs."""

    _add_distance_band(trips).createOrReplaceTempView("enriched_taxi_trips")

    monthly_summary = spark.sql(
        """
        SELECT
            year,
            month,
            COUNT(*) AS trip_count,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(total_amount), 2) AS average_trip_revenue,
            ROUND(
                SUM(COALESCE(tip_amount, 0))
                / NULLIF(SUM(COALESCE(fare_amount, 0)), 0),
                4
            ) AS tip_to_fare_ratio
        FROM enriched_taxi_trips
        GROUP BY year, month
        ORDER BY year, month
        """
    )

    borough_trip_summary = spark.sql(
        """
        SELECT
            pickup_borough,
            distance_band_order,
            distance_band,
            COUNT(*) AS trip_count,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(passenger_count), 2) AS average_passenger_count
        FROM enriched_taxi_trips
        WHERE pickup_borough IS NOT NULL
        GROUP BY pickup_borough, distance_band_order, distance_band
        ORDER BY pickup_borough, distance_band_order
        """
    )

    return monthly_summary, borough_trip_summary


def build_analytics_tables(
    spark: SparkSession,
    enriched_trips: DataFrame,
) -> dict[str, DataFrame]:
    """Return every named gold-layer table produced by the transform stage."""

    monthly_summary, borough_trip_summary = build_sql_summaries(spark, enriched_trips)
    return {
        "daily_revenue": build_daily_revenue(enriched_trips),
        "pickup_zone_summary": build_pickup_zone_summary(enriched_trips),
        "payment_method_summary": build_payment_method_summary(enriched_trips),
        "distance_band_summary": build_distance_band_summary(enriched_trips),
        "monthly_summary": monthly_summary,
        "borough_trip_summary": borough_trip_summary,
    }
