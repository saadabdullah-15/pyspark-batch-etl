from pipeline_utils import (
    INGEST_DIR,
    TAXI_TRIPS_FILE,
    TAXI_ZONES_FILE,
    as_posix,
    create_spark,
    require_files,
    stop_spark,
)


def main() -> None:
    require_files(TAXI_TRIPS_FILE, TAXI_ZONES_FILE)
    spark = create_spark("TaxiBatchETLIngest")

    try:
        trips = spark.read.parquet(as_posix(TAXI_TRIPS_FILE))
        zones = (
            spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(as_posix(TAXI_ZONES_FILE))
        )

        trip_count = trips.count()
        zone_count = zones.count()

        trips.write.mode("overwrite").parquet(as_posix(INGEST_DIR / "yellow_taxi_trips"))
        zones.write.mode("overwrite").parquet(as_posix(INGEST_DIR / "taxi_zones"))

        print(f"Ingested {trip_count:,} taxi trip rows.")
        print(f"Ingested {zone_count:,} taxi zone rows.")
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
