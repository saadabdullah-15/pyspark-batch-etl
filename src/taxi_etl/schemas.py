"""Input schemas and column contracts for raw source data."""

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

RAW_TRIP_COLUMNS = frozenset(
    {
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "RatecodeID",
        "store_and_fwd_flag",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "Airport_fee",
    }
)

RAW_ZONE_COLUMNS = frozenset({"LocationID", "Borough", "Zone", "service_zone"})

TAXI_ZONE_SCHEMA = StructType(
    [
        StructField("LocationID", IntegerType(), nullable=True),
        StructField("Borough", StringType(), nullable=True),
        StructField("Zone", StringType(), nullable=True),
        StructField("service_zone", StringType(), nullable=True),
    ]
)
