"""Data contracts used before transformations and after analytics writes."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass(frozen=True)
class TableContract:
    """The minimum schema and value rules expected from one analytics table."""

    required_columns: frozenset[str]
    key_columns: tuple[str, ...]
    non_negative_columns: tuple[str, ...] = ("trip_count", "total_revenue")


@dataclass(frozen=True)
class ValidationResult:
    table_name: str
    row_count: int


ANALYTICS_CONTRACTS: dict[str, TableContract] = {
    "daily_revenue": TableContract(
        required_columns=frozenset(
            {
                "pickup_date",
                "year",
                "month",
                "trip_count",
                "total_revenue",
                "average_trip_revenue",
                "average_trip_distance",
            }
        ),
        key_columns=("pickup_date",),
    ),
    "pickup_zone_summary": TableContract(
        required_columns=frozenset(
            {
                "pickup_borough",
                "pickup_zone",
                "trip_count",
                "total_revenue",
                "average_tip_amount",
                "unique_dropoff_zones",
            }
        ),
        key_columns=("pickup_borough", "pickup_zone"),
    ),
    "payment_method_summary": TableContract(
        required_columns=frozenset(
            {"payment_method", "trip_count", "total_revenue", "average_tip_amount"}
        ),
        key_columns=("payment_method",),
    ),
    "distance_band_summary": TableContract(
        required_columns=frozenset(
            {
                "distance_band_order",
                "distance_band",
                "trip_count",
                "total_revenue",
                "average_duration_minutes",
            }
        ),
        key_columns=("distance_band_order",),
    ),
    "monthly_summary": TableContract(
        required_columns=frozenset(
            {
                "year",
                "month",
                "trip_count",
                "total_revenue",
                "average_trip_revenue",
                "tip_to_fare_ratio",
            }
        ),
        key_columns=("year", "month"),
    ),
    "borough_trip_summary": TableContract(
        required_columns=frozenset(
            {
                "pickup_borough",
                "distance_band_order",
                "distance_band",
                "trip_count",
                "total_revenue",
                "average_passenger_count",
            }
        ),
        key_columns=("pickup_borough", "distance_band_order"),
    ),
}


def assert_required_columns(
    dataframe: DataFrame,
    required_columns: frozenset[str] | set[str],
    dataset_name: str,
) -> None:
    """Fail early with a readable list of missing source columns."""

    missing_columns = set(required_columns) - set(dataframe.columns)
    if missing_columns:
        formatted = ", ".join(sorted(missing_columns))
        raise ValueError(f"{dataset_name} is missing required column(s): {formatted}")


def validate_table(
    table_name: str,
    dataframe: DataFrame,
    contract: TableContract,
) -> ValidationResult:
    """Validate schema, row presence, keys, uniqueness, and non-negative metrics."""

    assert_required_columns(dataframe, contract.required_columns, table_name)

    row_count = dataframe.count()
    if row_count == 0:
        raise ValueError(f"{table_name} is empty")

    null_key_filter = None
    for key_column in contract.key_columns:
        condition = F.col(key_column).isNull()
        null_key_filter = condition if null_key_filter is None else null_key_filter | condition
    if null_key_filter is not None and dataframe.filter(null_key_filter).limit(1).count():
        raise ValueError(f"{table_name} contains a null business key")

    has_duplicate_key = (
        dataframe.groupBy(*contract.key_columns).count().filter(F.col("count") > 1).limit(1).count()
    )
    if has_duplicate_key:
        keys = ", ".join(contract.key_columns)
        raise ValueError(f"{table_name} contains a duplicate business key: {keys}")

    for metric in contract.non_negative_columns:
        if dataframe.filter(F.col(metric) < 0).limit(1).count():
            raise ValueError(f"{table_name} contains a negative value in {metric}")

    return ValidationResult(table_name=table_name, row_count=row_count)
