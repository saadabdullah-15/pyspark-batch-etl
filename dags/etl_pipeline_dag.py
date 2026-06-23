from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


PROJECT_DIR = Path(__file__).resolve().parents[1]
SPARK_SUBMIT = PROJECT_DIR / ".venv" / "bin" / "spark-submit"


default_args = {
    "owner": "saad",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def spark_submit_command(script_name: str) -> str:
    return f'cd "{PROJECT_DIR}" && "{SPARK_SUBMIT}" "src/{script_name}"'


with DAG(
    dag_id="pyspark_batch_etl_pipeline",
    default_args=default_args,
    description="Orchestrates a PySpark batch ETL pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["pyspark", "airflow", "etl", "portfolio"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_raw_data",
        bash_command=spark_submit_command("01_ingest.py"),
    )

    clean = BashOperator(
        task_id="clean_data",
        bash_command=spark_submit_command("02_clean.py"),
    )

    transform = BashOperator(
        task_id="transform_data",
        bash_command=spark_submit_command("03_transform.py"),
    )

    validate = BashOperator(
        task_id="run_data_quality_checks",
        bash_command=spark_submit_command("04_validate.py"),
    )

    ingest >> clean >> transform >> validate
