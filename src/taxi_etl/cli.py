"""Command-line interface shared by local runs and Airflow tasks."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from taxi_etl.config import PipelineConfig
from taxi_etl.pipeline import PIPELINE_STAGES, run_pipeline


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the NYC taxi PySpark batch ETL pipeline.",
    )
    parser.add_argument(
        "stage",
        nargs="?",
        default="all",
        choices=("all", *PIPELINE_STAGES),
        help="Pipeline stage to run (default: all).",
    )
    return parser


def main(arguments: Sequence[str] | None = None) -> None:
    args = build_parser().parse_args(arguments)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    config = PipelineConfig.from_environment()
    run_pipeline(config, args.stage)
    logging.getLogger(__name__).info("Requested pipeline work completed successfully")
