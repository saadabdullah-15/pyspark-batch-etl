from pipeline_utils import INGEST_DIR, RAW_DIR, as_posix, create_spark


def main() -> None:
    spark = create_spark("TaxiBatchETLIngest")

    trips = spark.read.parquet(as_posix(RAW_DIR / "yellow_tripdata_2024-01.parquet"))
    zones = (
        spark.read.option("header", True)
        .option("inferSchema", True)
        .csv(as_posix(RAW_DIR / "taxi_zone_lookup.csv"))
    )

    (
        trips.write.mode("overwrite")
        .parquet(as_posix(INGEST_DIR / "yellow_taxi_trips"))
    )
    zones.write.mode("overwrite").parquet(as_posix(INGEST_DIR / "taxi_zones"))

    print(f"Ingested {trips.count():,} taxi trip rows.")
    print(f"Ingested {zones.count():,} taxi zone rows.")

    spark.stop()


if __name__ == "__main__":
    main()
