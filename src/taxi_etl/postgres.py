"""Load selected Spark analytics CSV exports into PostgreSQL."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
from sqlalchemy import Date, Integer, Numeric, Text, create_engine, text
from sqlalchemy.engine import Engine

from taxi_etl.config import PipelineConfig

LOGGER = logging.getLogger(__name__)

POSTGRES_TABLES: tuple[str, ...] = (
    "daily_revenue",
    "pickup_zone_summary",
    "payment_method_summary",
)

TABLE_COLUMN_TYPES = {
    "daily_revenue": {
        "pickup_date": Date(),
        "year": Integer(),
        "month": Integer(),
        "trip_count": Integer(),
        "total_revenue": Numeric(14, 2),
        "average_trip_revenue": Numeric(14, 2),
        "average_trip_distance": Numeric(10, 2),
    },
    "pickup_zone_summary": {
        "pickup_borough": Text(),
        "pickup_zone": Text(),
        "trip_count": Integer(),
        "total_revenue": Numeric(14, 2),
        "average_tip_amount": Numeric(10, 2),
        "unique_dropoff_zones": Integer(),
    },
    "payment_method_summary": {
        "payment_method": Text(),
        "trip_count": Integer(),
        "total_revenue": Numeric(14, 2),
        "average_tip_amount": Numeric(10, 2),
    },
}


@dataclass(frozen=True)
class PostgresConfig:
    user: str = "etl_user"
    password: str = "etl_password"
    host: str = "localhost"
    port: str = "5432"
    database: str = "etl_db"

    @classmethod
    def from_environment(cls) -> PostgresConfig:
        return cls(
            user=os.environ.get("POSTGRES_USER", "etl_user"),
            password=os.environ.get("POSTGRES_PASSWORD", "etl_password"),
            host=os.environ.get("POSTGRES_HOST", "localhost"),
            port=os.environ.get("POSTGRES_PORT", "5432"),
            database=os.environ.get("POSTGRES_DB", "etl_db"),
        )

    @property
    def sqlalchemy_url(self) -> str:
        return (
            f"postgresql+psycopg://{self.user}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )


def create_postgres_engine(config: PostgresConfig) -> Engine:
    return create_engine(config.sqlalchemy_url)


def find_spark_csv_file(csv_folder: Path) -> Path:
    csv_files = sorted(csv_folder.glob("part-*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No Spark CSV part file found in {csv_folder}")
    return csv_files[0]


def load_csv_export(engine: Engine, csv_folder: Path, table_name: str) -> int:
    csv_file = find_spark_csv_file(csv_folder)
    dataframe = pd.read_csv(csv_file, keep_default_na=False)

    with engine.begin() as connection:
        connection.execute(text("CREATE SCHEMA IF NOT EXISTS analytics;"))
        connection.execute(text(f'TRUNCATE TABLE analytics."{table_name}";'))

        dataframe.to_sql(
            name=table_name,
            con=connection,
            schema="analytics",
            if_exists="append",
            index=False,
            dtype=TABLE_COLUMN_TYPES[table_name],
            method="multi",
        )

    return len(dataframe)


def load_postgres_tables(
    pipeline_config: PipelineConfig,
    postgres_config: PostgresConfig,
) -> None:
    engine = create_postgres_engine(postgres_config)
    export_root = pipeline_config.paths.postgres_export_dir

    for table_name in POSTGRES_TABLES:
        row_count = load_csv_export(engine, export_root / table_name, table_name)
        LOGGER.info("Loaded %s rows into analytics.%s", f"{row_count:,}", table_name)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )
    load_postgres_tables(
        PipelineConfig.from_environment(),
        PostgresConfig.from_environment(),
    )


if __name__ == "__main__":
    main()
