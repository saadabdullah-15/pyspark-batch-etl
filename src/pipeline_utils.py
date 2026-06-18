from pathlib import Path
import os

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROJECT_HADOOP_DIR = PROJECT_ROOT / "hadoop"
RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
INGEST_DIR = PROCESSED_DIR / "raw"
CLEAN_DIR = PROCESSED_DIR / "clean"
ANALYTICS_DIR = PROCESSED_DIR / "analytics"


def configure_local_hadoop() -> None:
    winutils_path = PROJECT_HADOOP_DIR / "bin" / "winutils.exe"
    if not winutils_path.exists():
        return

    hadoop_home = str(PROJECT_HADOOP_DIR)
    hadoop_bin = str(PROJECT_HADOOP_DIR / "bin")
    current_path = os.environ.get("PATH", "")

    os.environ.setdefault("HADOOP_HOME", hadoop_home)
    os.environ.setdefault("hadoop.home.dir", hadoop_home)
    if hadoop_bin.lower() not in current_path.lower().split(os.pathsep):
        os.environ["PATH"] = hadoop_bin + os.pathsep + current_path


def create_spark(app_name: str) -> SparkSession:
    configure_local_hadoop()

    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def as_posix(path: Path) -> str:
    return path.as_posix()
