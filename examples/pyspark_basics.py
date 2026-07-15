"""Small PySpark exercise using the repository's orders and customers CSV files.

This example is intentionally separate from the taxi pipeline. It introduces the
DataFrame operations that are later combined in the full ETL project.
"""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from taxi_etl.config import PipelineConfig, require_paths, spark_path
from taxi_etl.spark import create_spark, stop_spark

LOGGER = logging.getLogger(__name__)


def read_csv(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("header", True).option("inferSchema", True).csv(path)


def add_order_revenue(orders: DataFrame) -> DataFrame:
    return (
        orders.withColumn("order_date", F.to_date("order_date"))
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
        .withColumn("year", F.year("order_date"))
        .withColumn("month", F.month("order_date"))
    )


def summarize_product_revenue(orders: DataFrame) -> DataFrame:
    return (
        orders.groupBy("product")
        .agg(
            F.round(F.sum("revenue"), 2).alias("total_revenue"),
            F.count("order_id").alias("number_of_orders"),
        )
        .orderBy(F.col("total_revenue").desc())
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
    config = PipelineConfig.from_environment()
    paths = config.paths
    require_paths(
        (paths.orders_source, paths.customers_source),
        config.project_root,
    )

    spark = create_spark(config, "PySparkBasics")
    try:
        orders = read_csv(spark, spark_path(paths.orders_source))
        customers = read_csv(spark, spark_path(paths.customers_source))

        print("Orders schema")
        orders.printSchema()
        print("Laptop orders")
        orders.filter(F.col("product") == "Laptop").show(truncate=False)

        orders_with_revenue = add_order_revenue(orders)
        product_revenue = summarize_product_revenue(orders_with_revenue)
        enriched_orders = orders_with_revenue.join(
            customers,
            on="customer_id",
            how="left",
        )

        print("Product revenue")
        product_revenue.show(truncate=False)
        print("Orders enriched with customer details")
        enriched_orders.show(truncate=False)

        example_output = paths.processed_dir / "examples"
        (
            enriched_orders.write.mode("overwrite")
            .partitionBy("year", "month")
            .parquet(spark_path(example_output / "enriched_orders"))
        )
        product_revenue.write.mode("overwrite").parquet(
            spark_path(example_output / "product_revenue")
        )
        LOGGER.info("Wrote example outputs under %s", example_output)
    finally:
        stop_spark(spark)


if __name__ == "__main__":
    main()
