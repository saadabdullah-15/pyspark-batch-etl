"""Configuration and filesystem paths for the taxi ETL pipeline."""

from __future__ import annotations

import os
from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from pathlib import Path

DEFAULT_DATA_YEAR = 2024
DEFAULT_DATA_MONTH = 1


def _project_root() -> Path:
    """Return the repository root when the package is used from source."""

    return Path(__file__).resolve().parents[2]


def _integer_from_environment(name: str, default: int) -> int:
    raw_value = os.environ.get(name, str(default))
    try:
        return int(raw_value)
    except ValueError as error:
        raise ValueError(f"{name} must be an integer; received {raw_value!r}") from error


@dataclass(frozen=True)
class PipelinePaths:
    """All input and output locations used by the pipeline."""

    project_root: Path
    data_year: int
    data_month: int

    @property
    def raw_dir(self) -> Path:
        return self.project_root / "data" / "raw"

    @property
    def processed_dir(self) -> Path:
        return self.project_root / "data" / "processed"

    @property
    def ingest_dir(self) -> Path:
        return self.processed_dir / "raw"

    @property
    def clean_dir(self) -> Path:
        return self.processed_dir / "clean"

    @property
    def analytics_dir(self) -> Path:
        return self.processed_dir / "analytics"

    @property
    def hadoop_dir(self) -> Path:
        return self.project_root / "hadoop"

    @property
    def taxi_trips_source(self) -> Path:
        period = f"{self.data_year:04d}-{self.data_month:02d}"
        return self.raw_dir / f"yellow_tripdata_{period}.parquet"

    @property
    def taxi_zones_source(self) -> Path:
        return self.raw_dir / "taxi_zone_lookup.csv"

    @property
    def orders_source(self) -> Path:
        return self.raw_dir / "orders.csv"

    @property
    def customers_source(self) -> Path:
        return self.raw_dir / "customers.csv"


@dataclass(frozen=True)
class PipelineConfig:
    """Runtime settings shared by every pipeline stage.

    Defaults are intentionally small enough for a laptop. Every runtime value can
    be overridden with an environment variable, which also makes the code easy to
    reuse from Airflow or a CI job.
    """

    project_root: Path
    data_year: int = DEFAULT_DATA_YEAR
    data_month: int = DEFAULT_DATA_MONTH
    spark_master: str = "local[2]"
    driver_memory: str = "2g"
    shuffle_partitions: int = 4
    spark_log_level: str = "WARN"
    spark_time_zone: str = "America/New_York"

    def __post_init__(self) -> None:
        if not 1 <= self.data_month <= 12:
            raise ValueError("data_month must be between 1 and 12")
        if self.data_year < 2000:
            raise ValueError("data_year must be 2000 or later")
        if self.shuffle_partitions < 1:
            raise ValueError("shuffle_partitions must be at least 1")

    @classmethod
    def from_environment(cls, project_root: Path | None = None) -> PipelineConfig:
        """Build configuration from environment variables and safe defaults."""

        return cls(
            project_root=(project_root or _project_root()).resolve(),
            data_year=_integer_from_environment("TAXI_DATA_YEAR", DEFAULT_DATA_YEAR),
            data_month=_integer_from_environment("TAXI_DATA_MONTH", DEFAULT_DATA_MONTH),
            spark_master=os.environ.get("SPARK_MASTER", "local[2]"),
            driver_memory=os.environ.get("SPARK_DRIVER_MEMORY", "2g"),
            shuffle_partitions=_integer_from_environment("SPARK_SQL_SHUFFLE_PARTITIONS", 4),
            spark_log_level=os.environ.get("SPARK_LOG_LEVEL", "WARN").upper(),
            spark_time_zone=os.environ.get("SPARK_TIME_ZONE", "America/New_York"),
        )

    @property
    def paths(self) -> PipelinePaths:
        return PipelinePaths(self.project_root, self.data_year, self.data_month)

    @property
    def period_start(self) -> date:
        return date(self.data_year, self.data_month, 1)

    @property
    def period_end(self) -> date:
        last_day = monthrange(self.data_year, self.data_month)[1]
        return date(self.data_year, self.data_month, last_day)

    @property
    def next_period_start(self) -> date:
        if self.data_month == 12:
            return date(self.data_year + 1, 1, 1)
        return date(self.data_year, self.data_month + 1, 1)


def require_paths(paths: list[Path] | tuple[Path, ...], project_root: Path) -> None:
    """Raise one readable error containing every missing input path."""

    missing_paths = [path for path in paths if not path.exists()]
    if not missing_paths:
        return

    display_paths: list[str] = []
    for path in missing_paths:
        try:
            display_paths.append(str(path.relative_to(project_root)))
        except ValueError:
            display_paths.append(str(path))

    formatted_paths = "\n".join(f"- {path}" for path in display_paths)
    raise FileNotFoundError(f"Missing required input path(s):\n{formatted_paths}")


def spark_path(path: Path) -> str:
    """Return a cross-platform path string accepted by Spark."""

    return path.resolve().as_posix()
