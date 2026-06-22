from pyspark.sql import DataFrame

from pipeline_utils import ANALYTICS_DIR, as_posix, create_spark, require_files, stop_spark


EXPECTED_TABLE_COLUMNS = {
    "daily_revenue": {"pickup_date", "year", "month", "trip_count", "total_revenue"},
    "pickup_zone_performance": {"pickup_borough", "pickup_zone", "trip_count", "total_revenue"},
    "payment_type_performance": {"payment_method", "trip_count", "total_revenue"},
    "trip_distance_segments": {"distance_segment", "trip_count", "total_revenue"},
    "monthly_orders": {"year", "month", "trip_count", "total_revenue"},
    "customer_segments": {"market_segment", "trip_segment", "trip_count", "total_revenue"},
}


def validate_table(table_name: str, dataframe: DataFrame) -> int:
    missing_columns = EXPECTED_TABLE_COLUMNS[table_name] - set(dataframe.columns)
    if missing_columns:
        columns = ", ".join(sorted(missing_columns))
        raise ValueError(f"{table_name} is missing expected column(s): {columns}")

    row_count = dataframe.count()
    if row_count == 0:
        raise ValueError(f"{table_name} is empty")

    return row_count


def main() -> None:
    table_paths = [ANALYTICS_DIR / table_name for table_name in EXPECTED_TABLE_COLUMNS]
    require_files(*table_paths)

    spark = create_spark("TaxiBatchETLValidate")

    try:
        for table_name in EXPECTED_TABLE_COLUMNS:
            dataframe = spark.read.parquet(as_posix(ANALYTICS_DIR / table_name))
            row_count = validate_table(table_name, dataframe)
            print(f"Validated {table_name}: {row_count:,} rows")
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
