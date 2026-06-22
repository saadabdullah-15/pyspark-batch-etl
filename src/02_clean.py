from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    dayofmonth,
    lit,
    month,
    round as spark_round,
    to_date,
    when,
    year,
)

from pipeline_utils import (
    CLEAN_DIR,
    INGEST_DIR,
    as_posix,
    create_spark,
    require_files,
    stop_spark,
)


def clean_trips(trips: DataFrame) -> DataFrame:
    typed = trips.select(
        col("VendorID").cast("int").alias("vendor_id"),
        col("tpep_pickup_datetime").cast("timestamp").alias("pickup_at"),
        col("tpep_dropoff_datetime").cast("timestamp").alias("dropoff_at"),
        col("passenger_count").cast("int").alias("passenger_count"),
        col("trip_distance").cast("double").alias("trip_distance"),
        col("RatecodeID").cast("int").alias("rate_code_id"),
        col("store_and_fwd_flag").alias("store_and_fwd_flag"),
        col("PULocationID").cast("int").alias("pickup_location_id"),
        col("DOLocationID").cast("int").alias("dropoff_location_id"),
        col("payment_type").cast("int").alias("payment_type"),
        col("fare_amount").cast("double").alias("fare_amount"),
        col("extra").cast("double").alias("extra_amount"),
        col("mta_tax").cast("double").alias("mta_tax"),
        col("tip_amount").cast("double").alias("tip_amount"),
        col("tolls_amount").cast("double").alias("tolls_amount"),
        col("improvement_surcharge").cast("double").alias("improvement_surcharge"),
        col("total_amount").cast("double").alias("total_amount"),
        col("congestion_surcharge").cast("double").alias("congestion_surcharge"),
        col("Airport_fee").cast("double").alias("airport_fee"),
    )

    with_dates = (
        typed.dropDuplicates()
        .dropna(
            subset=[
                "pickup_at",
                "dropoff_at",
                "pickup_location_id",
                "dropoff_location_id",
                "trip_distance",
                "fare_amount",
                "total_amount",
            ]
        )
        .withColumn(
            "passenger_count",
            when(col("passenger_count").isNull(), lit(1)).otherwise(col("passenger_count")),
        )
        .withColumn("pickup_date", to_date(col("pickup_at")))
        .withColumn(
            "trip_duration_minutes",
            spark_round((col("dropoff_at").cast("long") - col("pickup_at").cast("long")) / 60, 2),
        )
        .withColumn("year", year(col("pickup_date")))
        .withColumn("month", month(col("pickup_date")))
        .withColumn("day", dayofmonth(col("pickup_date")))
    )

    return with_dates.filter(
        (col("pickup_date") >= lit("2024-01-01"))
        & (col("pickup_date") < lit("2024-02-01"))
        & (col("dropoff_at") > col("pickup_at"))
        & (col("trip_duration_minutes") <= 24 * 60)
        & (col("passenger_count").between(1, 6))
        & (col("trip_distance") > 0)
        & (col("trip_distance") <= 100)
        & (col("fare_amount") >= 0)
        & (col("total_amount") >= 0)
    )


def clean_zones(zones: DataFrame) -> DataFrame:
    return (
        zones.select(
            col("LocationID").cast("int").alias("location_id"),
            col("Borough").alias("borough"),
            col("Zone").alias("zone"),
            col("service_zone").alias("service_zone"),
        )
        .dropna(subset=["location_id", "borough", "zone"])
        .dropDuplicates(["location_id"])
    )


def main() -> None:
    require_files(INGEST_DIR / "yellow_taxi_trips", INGEST_DIR / "taxi_zones")
    spark = create_spark("TaxiBatchETLClean")

    try:
        trips = spark.read.parquet(as_posix(INGEST_DIR / "yellow_taxi_trips"))
        zones = spark.read.parquet(as_posix(INGEST_DIR / "taxi_zones"))

        clean_trips_df = clean_trips(trips)
        clean_zones_df = clean_zones(zones)

        trip_count = clean_trips_df.count()
        zone_count = clean_zones_df.count()

        (
            clean_trips_df.write.mode("overwrite")
            .option("maxRecordsPerFile", 250000)
            .partitionBy("year", "month")
            .parquet(as_posix(CLEAN_DIR / "yellow_taxi_trips"))
        )
        clean_zones_df.write.mode("overwrite").parquet(as_posix(CLEAN_DIR / "taxi_zones"))

        print(f"Cleaned taxi trip rows: {trip_count:,}")
        print(f"Cleaned taxi zone rows: {zone_count:,}")
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
