"""Tests for environment-driven pipeline configuration."""

from __future__ import annotations

from datetime import date

import pytest

from taxi_etl.config import PipelineConfig


def test_configuration_builds_period_and_source_path(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TAXI_DATA_YEAR", "2025")
    monkeypatch.setenv("TAXI_DATA_MONTH", "2")
    monkeypatch.setenv("SPARK_SQL_SHUFFLE_PARTITIONS", "8")

    config = PipelineConfig.from_environment(tmp_path)

    assert config.period_start == date(2025, 2, 1)
    assert config.period_end == date(2025, 2, 28)
    assert config.next_period_start == date(2025, 3, 1)
    assert config.shuffle_partitions == 8
    assert config.paths.taxi_trips_source.name == "yellow_tripdata_2025-02.parquet"


def test_configuration_handles_december_boundary(tmp_path) -> None:
    config = PipelineConfig(project_root=tmp_path, data_year=2024, data_month=12)

    assert config.next_period_start == date(2025, 1, 1)


@pytest.mark.parametrize("month", [0, 13])
def test_configuration_rejects_invalid_month(tmp_path, month) -> None:
    with pytest.raises(ValueError, match="data_month"):
        PipelineConfig(project_root=tmp_path, data_month=month)


def test_configuration_reports_non_integer_environment_value(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TAXI_DATA_MONTH", "January")

    with pytest.raises(ValueError, match="TAXI_DATA_MONTH must be an integer"):
        PipelineConfig.from_environment(tmp_path)
