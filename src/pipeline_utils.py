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
TAXI_TRIPS_FILE = RAW_DIR / "yellow_tripdata_2024-01.parquet"
TAXI_ZONES_FILE = RAW_DIR / "taxi_zone_lookup.csv"
ORDERS_FILE = RAW_DIR / "orders.csv"
CUSTOMERS_FILE = RAW_DIR / "customers.csv"


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

    spark_master = os.environ.get("SPARK_MASTER", "local[2]")
    driver_memory = os.environ.get("SPARK_DRIVER_MEMORY", "2g")
    shuffle_partitions = os.environ.get("SPARK_SQL_SHUFFLE_PARTITIONS", "4")

    return (
        SparkSession.builder.appName(app_name)
        .master(spark_master)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def stop_spark(spark: SparkSession) -> None:
    try:
        spark.stop()
    except Exception as error:
        print(f"Warning: Spark shutdown failed: {error}")


def as_posix(path: Path) -> str:
    return path.as_posix()


def require_files(*paths: Path) -> None:
    missing_files = [path for path in paths if not path.exists()]
    if not missing_files:
        return

    formatted_paths = "\n".join(f"- {path.relative_to(PROJECT_ROOT)}" for path in missing_files)
    raise FileNotFoundError(f"Missing required input file(s):\n{formatted_paths}")
