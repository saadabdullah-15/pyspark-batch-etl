# PySpark Batch ETL Pipeline with Airflow

Batch data engineering project that uses PySpark to turn raw NYC Yellow Taxi data into analytics-ready Parquet tables, with Apache Airflow orchestrating the pipeline stages.

The repository shows an end-to-end local workflow:

- ingest raw taxi trip and zone lookup files
- clean and validate trip records
- enrich trips with pickup and dropoff zone metadata
- build analytics tables with PySpark DataFrame APIs and Spark SQL
- orchestrate the stages with an Airflow DAG

## Why This Project Exists

Raw operational data often contains inconsistent types, missing values, duplicate rows, invalid records, and fields that are not immediately useful for reporting. This project demonstrates how to move from raw local files to a small analytics layer that can answer questions about revenue, payment methods, trip distances, and pickup-zone performance.

## Dataset

The main pipeline expects these files in `data/raw/`:

- `yellow_tripdata_2024-01.parquet`: January 2024 NYC Yellow Taxi trip records.
- `taxi_zone_lookup.csv`: NYC taxi zone metadata used to enrich pickup and dropoff locations.

The taxi trip Parquet file is ignored by Git because it is a large raw dataset. Download it from the NYC Taxi & Limousine Commission trip record data source before running the full pipeline:

```bash
curl -L --fail --create-dirs \
  -o data/raw/yellow_tripdata_2024-01.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

The small `orders.csv` and `customers.csv` files are included for the optional PySpark basics exercise in `src/day1_basics.py`.

## Architecture

```text
data/raw/
   |
   v
src/01_ingest.py
   |
   v
data/processed/raw/
   |
   v
src/02_clean.py
   |
   v
data/processed/clean/
   |
   v
src/03_transform.py
   |
   v
data/processed/analytics/
   |
   v
src/04_validate.py
```

Airflow runs the same stage scripts through `spark-submit` and manages task order, retries, scheduling, and logs.

```text
ingest_raw_data -> clean_data -> transform_data -> run_data_quality_checks
```

The DAG is defined in `dags/etl_pipeline_dag.py` with DAG ID `pyspark_batch_etl_pipeline`.

## Project Structure

```text
.
|-- dags/
|   `-- etl_pipeline_dag.py
|-- data/
|   |-- raw/
|   |   |-- customers.csv
|   |   |-- orders.csv
|   |   `-- taxi_zone_lookup.csv
|   `-- processed/
|       `-- README.md
|-- docs/
|   `-- screenshots/
|-- hadoop/
|   `-- bin/
|       `-- README.md
|-- src/
|   |-- 01_ingest.py
|   |-- 02_clean.py
|   |-- 03_transform.py
|   |-- 04_validate.py
|   |-- day1_basics.py
|   `-- pipeline_utils.py
|-- run_pipeline.py
|-- requirements.txt
`-- README.md
```

Generated Spark output under `data/processed/` is ignored by Git and can be recreated by running the pipeline.

## Pipeline Stages

### 1. Ingest

`src/01_ingest.py` reads the raw taxi trip Parquet file and taxi zone CSV file, then writes both datasets as Parquet under `data/processed/raw/`.

Outputs:

- `data/processed/raw/yellow_taxi_trips`
- `data/processed/raw/taxi_zones`

### 2. Clean

`src/02_clean.py` standardizes column names and types, removes duplicate rows, fills missing passenger counts, derives date columns, and filters invalid trips.

Important cleaning rules:

- require valid pickup and dropoff timestamps
- require pickup and dropoff location IDs
- keep only January 2024 pickup dates
- require dropoff time after pickup time
- keep trips up to 24 hours long
- keep passenger counts from 1 to 6
- keep positive trip distances up to 100 miles
- remove negative fare and total amounts
- write clean trips partitioned by `year` and `month`

Outputs:

- `data/processed/clean/yellow_taxi_trips`
- `data/processed/clean/taxi_zones`

### 3. Transform

`src/03_transform.py` enriches clean trips with pickup and dropoff borough/zone names, labels payment types, and creates analytics tables.

Analytics outputs:

- `daily_revenue`
- `pickup_zone_performance`
- `payment_type_performance`
- `trip_distance_segments`
- `monthly_orders`
- `customer_segments`

Tables are written as Parquet to `data/processed/analytics/`. Tables with `year` and `month` columns are partitioned by those columns.

### 4. Validate

`src/04_validate.py` checks that each expected analytics table exists, contains required columns, and has at least one row. This gives the pipeline a lightweight data quality gate after transformation.

## Requirements

- Python 3.10+
- Java 17
- PySpark
- Apache Airflow

Install the Python dependencies from `requirements.txt`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

If your shell does not provide a `python` command, use `python3` for the setup and run commands.

PySpark needs Java available on your `PATH`. Java 17 is recommended for this project because newer Java releases can fail in local Hadoop/Spark paths with errors such as `getSubject is not supported`.

On Linux or WSL, set `JAVA_HOME` if it is not already configured:

```bash
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

`src/pipeline_utils.py` also tries to detect common Java 17 install locations automatically.

## Run Locally

From the project root, run the full pipeline:

```bash
python run_pipeline.py
```

Or run each stage manually:

```bash
python src/01_ingest.py
python src/02_clean.py
python src/03_transform.py
python src/04_validate.py
```

Optional local Spark settings:

```bash
export SPARK_MASTER="local[4]"
export SPARK_DRIVER_MEMORY="4g"
export SPARK_SQL_SHUFFLE_PARTITIONS="4"
python run_pipeline.py
```

After a successful run, analytics tables are available under:

```text
data/processed/analytics/daily_revenue/
data/processed/analytics/pickup_zone_performance/
data/processed/analytics/payment_type_performance/
data/processed/analytics/trip_distance_segments/
data/processed/analytics/monthly_orders/
data/processed/analytics/customer_segments/
```

Inspect one output table with PySpark:

```bash
python -c "from pyspark.sql import SparkSession; spark=SparkSession.builder.master('local[*]').getOrCreate(); spark.read.parquet('data/processed/analytics/daily_revenue').show(); spark.stop()"
```

## Run with Airflow

The Airflow DAG expects the project virtual environment to exist at `.venv/` because each task calls:

```text
.venv/bin/spark-submit
```

Start Airflow from the project root:

```bash
source .venv/bin/activate
export AIRFLOW_HOME="$(pwd)"
airflow standalone
```

Then open the Airflow UI, enable the `pyspark_batch_etl_pipeline` DAG, and trigger it manually. You can also trigger it from the command line:

```bash
airflow dags trigger pyspark_batch_etl_pipeline
```

The DAG has a daily schedule and `catchup=False`, but this project uses a static January 2024 sample file, so manual triggering is usually the clearest way to demonstrate the pipeline.

## Windows Notes

On Windows, local PySpark Parquet writes may require Hadoop helper files:

```text
hadoop/bin/winutils.exe
hadoop/bin/hadoop.dll
```

These binaries are ignored by Git. If `hadoop/bin/winutils.exe` exists, the project automatically sets `HADOOP_HOME` and updates `PATH` for the local Spark session.

## Optional PySpark Basics Exercise

Run the small learning exercise that uses `orders.csv` and `customers.csv`:

```bash
python src/day1_basics.py
```

This writes sample outputs to:

```text
data/processed/enriched_orders/
data/processed/product_revenue/
```

## Airflow Screenshots

The `docs/screenshots/` folder contains screenshots from a successful local Airflow run:

- DAG list: `docs/screenshots/airflow-dag-list.jpg`
- DAG graph: `docs/screenshots/pyspark_batch_etl_pipeline-graph.png`
- Successful DAG run: `docs/screenshots/airflow-successful-dag-run.jpg`
- Task logs: `docs/screenshots/airflow_task_logs.jpg`

![Airflow DAG list](docs/screenshots/airflow-dag-list.jpg)

![PySpark ETL DAG graph](docs/screenshots/pyspark_batch_etl_pipeline-graph.png)

![Successful Airflow DAG run](docs/screenshots/airflow-successful-dag-run.jpg)

![Airflow task logs](docs/screenshots/airflow_task_logs.jpg)

## What This Demonstrates

- Reading CSV and Parquet files with Spark.
- Building a multi-stage batch ETL pipeline.
- Cleaning data with DataFrame operations such as `select`, `withColumn`, `dropna`, `dropDuplicates`, and `filter`.
- Enriching datasets with joins.
- Creating aggregate analytics tables with DataFrame APIs and Spark SQL.
- Writing partitioned Parquet outputs.
- Running a local data quality validation step.
- Orchestrating PySpark jobs with Airflow tasks, dependencies, retries, and logs.

## Portfolio Summary

Built a local batch ETL pipeline using PySpark and Apache Airflow to ingest NYC Yellow Taxi data, clean and validate records, enrich trips with zone metadata, create analytics-ready Parquet tables, and orchestrate the workflow with task-level retries and logs.
