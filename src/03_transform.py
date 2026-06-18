from pyspark.sql.functions import (
    avg,
    col,
    count,
    countDistinct,
    expr,
    round as spark_round,
    sum as spark_sum,
    when,
)

from pipeline_utils import ANALYTICS_DIR, CLEAN_DIR, as_posix, create_spark


def with_payment_labels(trips):
    return trips.withColumn(
        "payment_method",
        when(col("payment_type") == 1, "Credit card")
        .when(col("payment_type") == 2, "Cash")
        .when(col("payment_type") == 3, "No charge")
        .when(col("payment_type") == 4, "Dispute")
        .when(col("payment_type") == 5, "Unknown")
        .when(col("payment_type") == 6, "Voided trip")
        .otherwise("Missing"),
    )


def main() -> None:
    spark = create_spark("TaxiBatchETLTransform")

    trips = with_payment_labels(spark.read.parquet(as_posix(CLEAN_DIR / "yellow_taxi_trips")))
    zones = spark.read.parquet(as_posix(CLEAN_DIR / "taxi_zones"))

    enriched = (
        trips.join(zones.alias("pu"), trips.pickup_location_id == col("pu.location_id"), "left")
        .join(zones.alias("do"), trips.dropoff_location_id == col("do.location_id"), "left")
        .select(
            trips["*"],
            col("pu.borough").alias("pickup_borough"),
            col("pu.zone").alias("pickup_zone"),
            col("do.borough").alias("dropoff_borough"),
            col("do.zone").alias("dropoff_zone"),
        )
    )

    daily_revenue = (
        enriched.groupBy("pickup_date", "year", "month")
        .agg(
            count("*").alias("trip_count"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("total_amount"), 2).alias("avg_trip_revenue"),
            spark_round(avg("trip_distance"), 2).alias("avg_trip_distance"),
        )
        .orderBy("pickup_date")
    )

    pickup_zone_performance = (
        enriched.groupBy("pickup_borough", "pickup_zone")
        .agg(
            count("*").alias("trip_count"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("tip_amount"), 2).alias("avg_tip_amount"),
            countDistinct("dropoff_zone").alias("unique_dropoff_zones"),
        )
        .filter(col("pickup_zone").isNotNull())
        .orderBy(col("total_revenue").desc())
    )

    payment_type_performance = (
        enriched.groupBy("payment_method")
        .agg(
            count("*").alias("trip_count"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("tip_amount"), 2).alias("avg_tip_amount"),
        )
        .orderBy(col("trip_count").desc())
    )

    trip_distance_segments = (
        enriched.withColumn(
            "distance_segment",
            when(col("trip_distance") < 1, "00 under 1 mile")
            .when(col("trip_distance") < 3, "01 1 to 3 miles")
            .when(col("trip_distance") < 10, "02 3 to 10 miles")
            .otherwise("03 10+ miles"),
        )
        .groupBy("distance_segment")
        .agg(
            count("*").alias("trip_count"),
            spark_round(spark_sum("total_amount"), 2).alias("total_revenue"),
            spark_round(avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
        )
        .orderBy("distance_segment")
    )

    enriched.createOrReplaceTempView("trips_enriched")
    monthly_orders = spark.sql(
        """
        SELECT
            year,
            month,
            COUNT(*) AS trip_count,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(total_amount), 2) AS avg_trip_revenue,
            ROUND(SUM(tip_amount) / NULLIF(SUM(fare_amount), 0), 4) AS tip_to_fare_ratio
        FROM trips_enriched
        GROUP BY year, month
        ORDER BY year, month
        """
    )

    customer_segments = spark.sql(
        """
        SELECT
            pickup_borough AS market_segment,
            CASE
                WHEN trip_distance < 1 THEN 'short'
                WHEN trip_distance < 3 THEN 'medium'
                ELSE 'long'
            END AS trip_segment,
            COUNT(*) AS trip_count,
            ROUND(SUM(total_amount), 2) AS total_revenue,
            ROUND(AVG(passenger_count), 2) AS avg_passenger_count
        FROM trips_enriched
        WHERE pickup_borough IS NOT NULL
        GROUP BY pickup_borough,
            CASE
                WHEN trip_distance < 1 THEN 'short'
                WHEN trip_distance < 3 THEN 'medium'
                ELSE 'long'
            END
        ORDER BY market_segment, trip_segment
        """
    )

    outputs = {
        "daily_revenue": daily_revenue,
        "pickup_zone_performance": pickup_zone_performance,
        "payment_type_performance": payment_type_performance,
        "trip_distance_segments": trip_distance_segments,
        "monthly_orders": monthly_orders,
        "customer_segments": customer_segments,
    }

    for table_name, dataframe in outputs.items():
        writer = dataframe.write.mode("overwrite")
        if "year" in dataframe.columns and "month" in dataframe.columns:
            writer = writer.partitionBy("year", "month")
        writer.parquet(as_posix(ANALYTICS_DIR / table_name))
        print(f"Wrote analytics table: {table_name}")

    spark.stop()


if __name__ == "__main__":
    main()
