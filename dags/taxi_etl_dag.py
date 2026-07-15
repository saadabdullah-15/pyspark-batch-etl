"""Airflow orchestration for the local PySpark taxi ETL pipeline.

The source dataset represents a fixed month, so this DAG is manually triggered.
Each task runs exactly one CLI stage and Airflow records its logs and retry state.
"""

from __future__ import annotations

import os
import shlex
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT_DIR = Path(
    os.environ.get("TAXI_ETL_PROJECT_DIR", Path(__file__).resolve().parents[1])
).resolve()


def default_python_bin() -> str:
    """Return the project-local Python used by Airflow tasks."""

    return str(PROJECT_DIR / ".venv" / "bin" / "python")


PYTHON_BIN = os.environ.get(
    "TAXI_ETL_PYTHON_BIN",
    default_python_bin(),
)
PIPELINE_RUNNER = PROJECT_DIR / "run_pipeline.py"

DEFAULT_ARGS = {
    "owner": "data-engineering-student",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def pipeline_command(stage_name: str) -> str:
    """Build a safely quoted command for one stage."""

    return " ".join(
        [
            "cd",
            shlex.quote(str(PROJECT_DIR)),
            "&&",
            shlex.quote(PYTHON_BIN),
            shlex.quote(str(PIPELINE_RUNNER)),
            shlex.quote(stage_name),
        ]
    )


with DAG(
    dag_id="pyspark_batch_etl_pipeline",
    default_args=DEFAULT_ARGS,
    description="Build and validate NYC taxi analytics tables with PySpark",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["pyspark", "airflow", "etl", "learning-project"],
    doc_md=__doc__,
) as dag:
    ingest = BashOperator(
        task_id="ingest_raw_data",
        bash_command=pipeline_command("ingest"),
    )

    clean = BashOperator(
        task_id="clean_data",
        bash_command=pipeline_command("clean"),
    )

    transform = BashOperator(
        task_id="transform_data",
        bash_command=pipeline_command("transform"),
    )

    validate = BashOperator(
        task_id="run_data_quality_checks",
        bash_command=pipeline_command("validate"),
    )

    ingest >> clean >> transform >> validate
