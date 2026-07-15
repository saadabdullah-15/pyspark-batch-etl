"""Shared local Spark session for transformation tests."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from taxi_etl.spark import configure_python_worker


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    configure_python_worker()
    session = (
        SparkSession.builder.master("local[1]")
        .appName("TaxiETLTests")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
