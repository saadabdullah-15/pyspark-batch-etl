# PySpark Batch ETL Pipeline for Analytics-Ready Data

This project builds a batch data pipeline with PySpark. It ingests raw NYC Yellow Taxi data, cleans and validates trip records, enriches trips with taxi zone metadata, and writes analytics-ready Parquet tables.

## Problem

Raw operational data usually contains inconsistent types, missing values, duplicates, invalid records, and fields that are not directly useful for reporting. The goal is to turn raw taxi trip files into clean, partitioned tables that can support analysis such as revenue trends, payment performance, and pickup-zone performance.

## Dataset

The pipeline uses two local source files:

- `data/raw/yellow_tripdata_2024-01.parquet`: January 2024 NYC Yellow Taxi trip records.
- `data/raw/taxi_zone_lookup.csv`: taxi location metadata used to enrich pickup and dropoff locations.

Small `orders.csv` and `customers.csv` files are also included for the Day 1 PySpark basics exercise in `src/day1_basics.py`.

The taxi trip Parquet file is intentionally ignored by Git because it is a large raw dataset. Download the January 2024 Yellow Taxi trip Parquet file from the NYC Taxi & Limousine Commission trip record data page and place it at `data/raw/yellow_tripdata_2024-01.parquet` before running the full taxi pipeline.

## Pipeline Architecture

```text
Raw Taxi Files -> PySpark Ingest -> Cleaning and Validation -> Transformations -> Partitioned Parquet Analytics Tables
```

## Project Structure

```text
data/
  raw/
  processed/
src/
  01_ingest.py
  02_clean.py
  03_transform.py
  day1_basics.py
  pipeline_utils.py
README.md
requirements.txt
```

## Raw Layer

`src/01_ingest.py` reads the source Parquet and CSV files from `data/raw/` and writes them as Parquet under `data/processed/raw/`.

Outputs:

- `data/processed/raw/yellow_taxi_trips`
- `data/processed/raw/taxi_zones`

## Clean Layer

`src/02_clean.py` standardizes columns, casts data types, parses dates, removes duplicates, fills basic null values, and filters invalid records.

Cleaning rules include:

- Drop rows missing required timestamps, locations, distance, fare, or total amount.
- Cast numeric and timestamp fields to explicit Spark types.
- Parse `pickup_date`, `year`, `month`, and `day`.
- Fill missing passenger count with `1`.
- Remove invalid trips with negative fares, non-positive distance, impossible passenger counts, or pickup/dropoff dates outside January 2024.
- Write clean trips partitioned by `year` and `month`.

Outputs:

- `data/processed/clean/yellow_taxi_trips`
- `data/processed/clean/taxi_zones`

## Analytics Layer

`src/03_transform.py` enriches clean taxi trips with pickup and dropoff zone names, then writes analytics tables using both PySpark DataFrame operations and Spark SQL.

Analytics outputs:

- `daily_revenue`: daily trip count, revenue, average revenue, and average trip distance.
- `pickup_zone_performance`: pickup borough and zone revenue performance.
- `payment_type_performance`: trip and revenue metrics by payment method.
- `trip_distance_segments`: trips grouped into short, medium, and long distance bands.
- `monthly_orders`: monthly trip and revenue summary built with Spark SQL.
- `customer_segments`: borough and trip-length segments built with Spark SQL.

Tables are written to `data/processed/analytics/` as Parquet. Tables containing `year` and `month` are partitioned by those columns.

## How to Run

Create and activate a virtual environment, then install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

PySpark also needs Java available on your `PATH`.

On Windows, local Parquet writes may fail if Hadoop's Windows helper files are not configured. Install Hadoop-compatible Windows helper files and place them here:

```powershell
pyspark-batch-etl\hadoop\bin\winutils.exe
pyspark-batch-etl\hadoop\bin\hadoop.dll
```

The project automatically sets `HADOOP_HOME` to `pyspark-batch-etl\hadoop` when `winutils.exe` exists. These binaries are ignored by Git because they are local Windows runtime helpers, not pipeline source code.

Run each pipeline stage from the project root:

```powershell
python src\01_ingest.py
python src\02_clean.py
python src\03_transform.py
```

Optional Day 1 basics exercise:

```powershell
python src\day1_basics.py
```

## Example Outputs

After running the full pipeline, the generated tables are available under:

```text
data/processed/analytics/daily_revenue/
data/processed/analytics/pickup_zone_performance/
data/processed/analytics/payment_type_performance/
data/processed/analytics/trip_distance_segments/
data/processed/analytics/monthly_orders/
data/processed/analytics/customer_segments/
```

You can inspect one table with PySpark:

```powershell
python -c "from pyspark.sql import SparkSession; spark=SparkSession.builder.master('local[*]').getOrCreate(); spark.read.parquet('data/processed/analytics/daily_revenue').show(); spark.stop()"
```

## What I Learned

- How to read CSV and Parquet files with Spark.
- How to clean data using DataFrame operations such as `select`, `withColumn`, `dropna`, `dropDuplicates`, `filter`, `groupBy`, and `join`.
- How to use Spark SQL for analytics transformations.
- How to write analytics-ready Parquet tables.
- How partitioning by `year` and `month` improves the layout for batch analytics workloads.

## CV / LinkedIn Summary

Built a batch ETL pipeline using PySpark to ingest raw taxi trip data, clean and validate records, perform SQL-style transformations, and write analytics-ready partitioned Parquet tables.
