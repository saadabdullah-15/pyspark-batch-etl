"""Tests for persisted analytics table contracts."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from taxi_etl.quality import TableContract, assert_required_columns, validate_table

CONTRACT = TableContract(
    required_columns=frozenset({"business_key", "trip_count", "total_revenue"}),
    key_columns=("business_key",),
)


def test_validate_table_accepts_a_valid_table(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame(
        [("A", 3, 12.5), ("B", 2, 8.0)],
        "business_key string, trip_count long, total_revenue double",
    )

    result = validate_table("example", dataframe, CONTRACT)

    assert result.table_name == "example"
    assert result.row_count == 2


def test_validate_table_rejects_duplicate_keys(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame(
        [("A", 3, 12.5), ("A", 2, 8.0)],
        "business_key string, trip_count long, total_revenue double",
    )

    with pytest.raises(ValueError, match="duplicate business key"):
        validate_table("example", dataframe, CONTRACT)


def test_validate_table_rejects_negative_metrics(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame(
        [("A", 3, -1.0)],
        "business_key string, trip_count long, total_revenue double",
    )

    with pytest.raises(ValueError, match="negative value in total_revenue"):
        validate_table("example", dataframe, CONTRACT)


def test_required_columns_error_lists_missing_columns(spark: SparkSession) -> None:
    dataframe = spark.createDataFrame([(1,)], "present int")

    with pytest.raises(ValueError, match="missing_one, missing_two"):
        assert_required_columns(
            dataframe,
            {"present", "missing_two", "missing_one"},
            "example",
        )
