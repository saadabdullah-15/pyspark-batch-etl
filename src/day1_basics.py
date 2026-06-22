from pyspark.sql.functions import col, to_date, year, month, sum as spark_sum, count

from pipeline_utils import (
    CUSTOMERS_FILE,
    ORDERS_FILE,
    as_posix,
    create_spark,
    require_files,
    stop_spark,
)


def main() -> None:
    require_files(ORDERS_FILE, CUSTOMERS_FILE)
    spark = create_spark("Day1PySparkBasics")

    try:
        # 1. Read CSV files
        orders_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(as_posix(ORDERS_FILE))
        )

        customers_df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(as_posix(CUSTOMERS_FILE))
        )

        print("Orders schema:")
        orders_df.printSchema()

        print("Orders data:")
        orders_df.show()

        # 2. select()
        selected_orders = orders_df.select(
            "order_id",
            "customer_id",
            "product",
            "quantity",
            "unit_price"
        )

        print("Selected columns:")
        selected_orders.show()

        # 3. filter()
        laptop_orders = orders_df.filter(col("product") == "Laptop")

        print("Laptop orders:")
        laptop_orders.show()

        # 4. withColumn()
        orders_with_revenue = (
            orders_df
            .withColumn("order_date", to_date(col("order_date")))
            .withColumn("revenue", col("quantity") * col("unit_price"))
            .withColumn("year", year(col("order_date")))
            .withColumn("month", month(col("order_date")))
        )

        print("Orders with revenue:")
        orders_with_revenue.show()

        # 5. groupBy().agg()
        product_revenue = (
            orders_with_revenue
            .groupBy("product")
            .agg(
                spark_sum("revenue").alias("total_revenue"),
                count("order_id").alias("number_of_orders")
            )
            .orderBy(col("total_revenue").desc())
        )

        print("Product revenue:")
        product_revenue.show()

        # 6. join()
        enriched_orders = (
            orders_with_revenue
            .join(customers_df, on="customer_id", how="left")
        )

        print("Enriched orders:")
        enriched_orders.show()

        # 7. write.parquet()
        (
            enriched_orders
            .write
            .mode("overwrite")
            .partitionBy("year", "month")
            .parquet("data/processed/enriched_orders")
        )

        (
            product_revenue
            .write
            .mode("overwrite")
            .parquet("data/processed/product_revenue")
        )

        print("Day 1 completed successfully.")

    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
