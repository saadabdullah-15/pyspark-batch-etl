"""Creation and cleanup of local Spark sessions."""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

from pyspark.sql import SparkSession

from taxi_etl.config import PipelineConfig

LOGGER = logging.getLogger(__name__)

LINUX_JAVA_17_CANDIDATES = (
    Path("/usr/lib/jvm/java-17-openjdk-amd64"),
    Path("/usr/lib/jvm/java-17-openjdk"),
    Path("/usr/lib/jvm/temurin-17-jdk-amd64"),
)


def configure_java_home() -> None:
    """Find a common Java 17 installation when JAVA_HOME is not already set."""

    if os.environ.get("JAVA_HOME"):
        return

    candidates = list(LINUX_JAVA_17_CANDIDATES)
    linux_jvm_dir = Path("/usr/lib/jvm")
    if linux_jvm_dir.exists():
        candidates.extend(sorted(linux_jvm_dir.glob("*17*")))

    for candidate in candidates:
        if (candidate / "bin" / "java").exists():
            os.environ["JAVA_HOME"] = str(candidate)
            return


def configure_windows_hadoop(config: PipelineConfig) -> None:
    """Configure the local Hadoop helpers required by native Windows Spark."""

    hadoop_bin = config.paths.hadoop_dir / "bin"
    if os.name != "nt":
        return

    required_helpers = (hadoop_bin / "winutils.exe", hadoop_bin / "hadoop.dll")
    missing_helpers = [path for path in required_helpers if not path.exists()]
    if missing_helpers:
        formatted = "\n".join(f"- {path}" for path in missing_helpers)
        raise FileNotFoundError(
            "Native Windows PySpark requires local Hadoop helper files before "
            f"it can write Parquet:\n{formatted}\n"
            "Add trusted copies of these files or run the project in WSL."
        )

    hadoop_home = str(config.paths.hadoop_dir)
    current_path = os.environ.get("PATH", "")
    path_entries = [entry.casefold() for entry in current_path.split(os.pathsep)]

    os.environ.setdefault("HADOOP_HOME", hadoop_home)
    os.environ.setdefault("hadoop.home.dir", hadoop_home)
    if str(hadoop_bin).casefold() not in path_entries:
        os.environ["PATH"] = str(hadoop_bin) + os.pathsep + current_path


def configure_python_worker() -> None:
    """Make Spark workers use the same Python interpreter as the driver."""

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


def create_spark(config: PipelineConfig, stage_name: str) -> SparkSession:
    """Create a consistently configured local Spark session."""

    configure_java_home()
    configure_windows_hadoop(config)
    configure_python_worker()

    spark = (
        SparkSession.builder.appName(f"TaxiBatchETL-{stage_name}")
        .master(config.spark_master)
        .config("spark.driver.memory", config.driver_memory)
        .config("spark.sql.shuffle.partitions", str(config.shuffle_partitions))
        .config("spark.sql.parquet.compression.codec", "snappy")
        .config("spark.sql.session.timeZone", config.spark_time_zone)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(config.spark_log_level)
    return spark


def stop_spark(spark: SparkSession) -> None:
    """Stop Spark while preserving the original stage error, if there is one."""

    try:
        spark.stop()
    except Exception:
        LOGGER.warning("Spark did not shut down cleanly", exc_info=True)
