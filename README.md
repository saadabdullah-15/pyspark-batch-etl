# PySpark and Airflow Batch ETL Pipeline

A learning-focused, production-shaped batch data pipeline that turns raw NYC
Yellow Taxi records into validated analytics tables. PySpark performs the data
work, Apache Airflow orchestrates the stages, and PostgreSQL stores selected
analytics outputs for SQL validation.

This repository is designed to be readable in two ways:

- A student can follow the pipeline from source file to analytics table.
- A reviewer can see clear module boundaries, configuration, tests, logging, and
  data-quality contracts.

## What the pipeline does

```text
NYC taxi Parquet + zone CSV
            |
            v
  1. ingest  -> stable raw Parquet copies
            |
            v
  2. clean   -> typed, deduplicated, valid trips
            |
            v
  3. transform -> zone-enriched business summaries
            |
            v
  4. validate  -> schema, key, row, and metric checks
            |
            v
  5. load PostgreSQL -> queryable analytics tables
```

Every stage can run independently, while `all` runs them in dependency order.
Outputs use overwrite mode, so rerunning the same reporting month is idempotent.

## Project structure

```text
.
|-- dags/
|   `-- taxi_etl_dag.py          # Airflow task graph
|-- data/
|   |-- raw/                     # Downloaded and sample source files
|   `-- processed/               # Generated raw, clean, and analytics layers
|-- sql/
|   |-- create_tables.sql        # PostgreSQL analytics schema
|   `-- validation_queries.sql   # SQL checks for loaded tables
|-- docs/
|   |-- architecture.md          # Design decisions and extension points
|   |-- learning-guide.md        # Guided code-reading path
|   `-- screenshots/             # Airflow run evidence
|-- examples/
|   `-- pyspark_basics.py        # Small orders/customers exercise
|-- hadoop/bin/                  # Optional Windows Hadoop helpers
|-- src/taxi_etl/
|   |-- cli.py                   # Command-line interface
|   |-- config.py                # Paths and environment configuration
|   |-- pipeline.py              # Stage I/O and execution order
|   |-- postgres.py              # PostgreSQL loading step
|   |-- quality.py               # Analytics data contracts
|   |-- schemas.py               # Raw source expectations
|   |-- spark.py                 # Spark session configuration
|   `-- transformations.py       # Side-effect-free business logic
|-- docker-compose.yml           # Local PostgreSQL service
|-- tests/                       # Configuration, transformation, and quality tests
|-- pyproject.toml               # Package and tool configuration
|-- run_pipeline.py              # Simple repository entry point
`-- requirements*.txt            # Core, Airflow, and development installs
```

The most important design rule is that `transformations.py` does not read or write
files. It receives DataFrames and returns DataFrames. Filesystem work stays in
`pipeline.py`, which makes the business rules much easier to test.

## Dataset

The full taxi pipeline expects two files under `data/raw/`:

- `yellow_tripdata_2024-01.parquet` — January 2024 Yellow Taxi trips.
- `taxi_zone_lookup.csv` — the zone names used for pickup and drop-off enrichment.

The zone lookup is committed because it is small. The trip file is intentionally
ignored by Git and must be downloaded once.

PowerShell:

```powershell
New-Item -ItemType Directory -Force data/raw | Out-Null
Invoke-WebRequest `
  -Uri "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet" `
  -OutFile "data/raw/yellow_tripdata_2024-01.parquet"
```

Bash:

```bash
curl -L --fail --create-dirs \
  -o data/raw/yellow_tripdata_2024-01.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet
```

The small `orders.csv` and `customers.csv` files belong only to the optional
PySpark basics example.

## Quick start

### 1. Prerequisites

- Python 3.10 or newer
- Java 17
- At least 4 GB of free memory for a comfortable local run
- WSL/Linux, or the two local Hadoop helper files described below for native Windows

Confirm the first two before installing anything:

```text
python --version
java -version
```

### 2. Create the environment

PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Bash:

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

The editable install provides both the Python package and a `taxi-etl` command.
Install the development and Airflow extras into the same environment when you
want to run tests, lint, and local orchestration:

```bash
python -m pip install -r requirements-dev.txt
python -m pip install -r requirements-airflow.txt
```

#### Native Windows versus WSL

Use `.venv` from PowerShell when the trusted Windows Hadoop helpers are present.
If those binaries are unavailable, use the existing Ubuntu WSL installation and a
separate Linux environment:

```bash
# Run these commands inside Ubuntu WSL.
cd /mnt/g/pyspark-batch-etl
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
python run_pipeline.py
```

Windows and WSL environments should stay separate because Windows executables
cannot run as Linux executables. If the same checkout already has a Windows
`.venv`, recreate it inside WSL before using the Bash commands. The virtual
environment directory is ignored by Git, and it does not change the system Python
packages.

### 3. Download the taxi Parquet file

Use one of the commands in [Dataset](#dataset), then confirm this path exists:

```text
data/raw/yellow_tripdata_2024-01.parquet
```

### 4. Run the pipeline

```text
python run_pipeline.py
```

The equivalent installed command is:

```text
taxi-etl all
```

Run one stage while learning or debugging:

```text
python run_pipeline.py ingest
python run_pipeline.py clean
python run_pipeline.py transform
python run_pipeline.py validate
```

Stages depend on the output of the previous stage. For example, `transform` can
only run after clean data exists.

## Pipeline stages

### 1. Ingest

Ingest reads the source Parquet and CSV, checks that their expected columns exist,
and writes stable Parquet copies to:

```text
data/processed/raw/yellow_taxi_trips/
data/processed/raw/taxi_zones/
```

The zone CSV uses an explicit schema. This avoids accidental type changes caused
by schema inference.

### 2. Clean

Cleaning standardizes column names and types, removes full-row duplicates, fills a
missing passenger count with `1`, derives reporting columns, and applies these
rules:

- pickup time, drop-off time, locations, distance, fare, and total are required;
- pickup must fall inside the configured reporting month;
- drop-off must be later than pickup and no more than 24 hours later;
- passenger count must be from 1 through 6;
- distance must be greater than 0 and no more than 100 miles;
- fare and total amount cannot be negative.

Clean trips are partitioned by `year` and `month`:

```text
data/processed/clean/yellow_taxi_trips/
data/processed/clean/taxi_zones/
```

### 3. Transform

Transform joins trips to the zone lookup twice: once for pickup and once for
drop-off. It also converts payment codes into readable labels and builds six gold
tables. Four demonstrate the DataFrame API and two demonstrate Spark SQL.

| Table | Grain | Main question |
| --- | --- | --- |
| `daily_revenue` | one row per pickup date | How many trips and how much revenue occurred each day? |
| `pickup_zone_summary` | one row per pickup borough and zone | Which pickup zones generate trips and revenue? |
| `payment_method_summary` | one row per payment method | How do payment methods compare? |
| `distance_band_summary` | one row per distance band | How do trip length, revenue, and duration relate? |
| `monthly_summary` | one row per year and month | What is the reporting month's overall performance? |
| `borough_trip_summary` | one row per borough and distance band | How does trip length vary by pickup borough? |

The tables are written under `data/processed/analytics/<table-name>/`.
The three tables loaded into PostgreSQL are also exported as headered CSV folders
under `data/processed/postgres_exports/<table-name>/`.

### 4. Validate

Validation reads the persisted Parquet outputs rather than trusting in-memory
DataFrames. For every table it checks:

- all contract columns exist;
- at least one row exists;
- business key columns are not null;
- business keys are unique;
- trip counts and revenue are non-negative.

A failed rule raises an error, which also marks the Airflow task as failed.

### 5. Load PostgreSQL

The PostgreSQL loader reads selected Spark CSV exports and appends them into
pre-created tables in the `analytics` schema:

```text
analytics.daily_revenue
analytics.pickup_zone_summary
analytics.payment_method_summary
```

The loader truncates each target table first, so rerunning the pipeline replaces
the current reporting month instead of duplicating rows.

## Generated outputs

Local runs create overwriteable runtime outputs under `data/processed/`:

- `data/processed/raw/` contains stable Parquet copies of the source trips and
  zone lookup.
- `data/processed/clean/` contains cleaned trips partitioned by `year` and
  `month`, plus cleaned zones.
- `data/processed/analytics/` contains the six validated analytics tables.
- `data/processed/postgres_exports/` contains CSV mirrors of the selected tables
  loaded into PostgreSQL.
- `data/processed/examples/` is created only by the optional PySpark basics
  exercise.

Airflow also creates local metadata and logs under `.airflow/`. These generated
directories, raw taxi Parquet files, logs, caches, and virtual environments are
ignored by Git.

## Configuration

Defaults are suitable for a laptop and can be overridden without editing code.

| Environment variable | Default | Purpose |
| --- | --- | --- |
| `TAXI_DATA_YEAR` | `2024` | Source filename and reporting year |
| `TAXI_DATA_MONTH` | `1` | Source filename and reporting month |
| `SPARK_MASTER` | `local[2]` | Spark execution master |
| `SPARK_DRIVER_MEMORY` | `2g` | Local driver memory |
| `SPARK_SQL_SHUFFLE_PARTITIONS` | `4` | Number of local shuffle partitions |
| `SPARK_LOG_LEVEL` | `WARN` | Spark's log verbosity |
| `SPARK_TIME_ZONE` | `America/New_York` | Timestamp interpretation for taxi data |
| `POSTGRES_USER` | `etl_user` | PostgreSQL user for the analytics loader |
| `POSTGRES_PASSWORD` | `etl_password` | PostgreSQL password for the analytics loader |
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_DB` | `etl_db` | PostgreSQL database |

PowerShell example:

```powershell
$env:SPARK_MASTER = "local[4]"
$env:SPARK_DRIVER_MEMORY = "4g"
python run_pipeline.py
```

Bash example:

```bash
SPARK_MASTER='local[4]' SPARK_DRIVER_MEMORY='4g' python run_pipeline.py
```

Changing the reporting period also changes the expected source filename. Setting
year `2025` and month `2`, for example, expects
`data/raw/yellow_tripdata_2025-02.parquet`.

## PostgreSQL with Docker Compose

Week 3 adds PostgreSQL as a local analytics database. Only PostgreSQL is
containerized; PySpark and Airflow still run from the project virtual environment.

Start PostgreSQL:

```bash
docker compose up -d
docker ps
```

The first startup creates the `analytics` schema and tables from
`sql/create_tables.sql`. Confirm the tables:

```bash
docker exec -it etl_postgres psql -U etl_user -d etl_db
```

Inside `psql`:

```sql
SELECT current_database();
\dt analytics.*
\q
```

After running the transform stage, load the selected analytics tables:

```bash
python run_pipeline.py transform
python -m taxi_etl.postgres
```

Validate the loaded tables with SQL:

```bash
docker exec -i etl_postgres psql -U etl_user -d etl_db < sql/validation_queries.sql
```

Expected row counts for the default January 2024 data are:

```text
analytics.daily_revenue: 31 rows
analytics.pickup_zone_summary: 257 rows
analytics.payment_method_summary: 5 rows
```

If `sql/create_tables.sql` changes after the volume already exists, recreate the
database volume:

```bash
docker compose down -v
docker compose up -d
```

## Tests and code quality

Tests use small in-memory DataFrames; the large taxi download is not required.

```text
python -m pip install -r requirements-dev.txt
python -m ruff check .
python -m pytest -q
```

The tests cover configuration boundaries, cleaning rules, zone normalization,
enrichment, all analytics table names, and both successful and failed data
contracts.

## Airflow orchestration

The DAG uses Bash commands and is intended for Linux or WSL. Airflow should run
from the project-local `.venv`. Pipeline tasks call one stage with
`.venv/bin/python run_pipeline.py <stage>`, and the final database task calls
`.venv/bin/python -m taxi_etl.postgres`.

```bash
source .venv/bin/activate
python -m pip install -r requirements-airflow.txt

export AIRFLOW_HOME=/home/saad_abdullah/projects/pyspark-airflow-etl-project/.airflow
export AIRFLOW__CORE__DAGS_FOLDER=/home/saad_abdullah/projects/pyspark-airflow-etl-project/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False

airflow standalone
```

Open the Airflow UI, find `pyspark_batch_etl_pipeline`, and trigger it manually.
The data is a fixed monthly snapshot, so the DAG deliberately has no automatic
schedule.

The tasks run in this order:

```text
ingest_raw_data -> clean_data -> transform_data -> run_data_quality_checks -> load_to_postgres
```

Airflow can point to a different checkout or Python executable with:

```bash
export TAXI_ETL_PROJECT_DIR=/path/to/pyspark-batch-etl
export TAXI_ETL_PYTHON_BIN=/path/to/.venv/bin/python
```

Useful local checks before starting the scheduler:

```bash
AIRFLOW_HOME=/home/saad_abdullah/projects/pyspark-airflow-etl-project/.airflow \
AIRFLOW__CORE__DAGS_FOLDER=/home/saad_abdullah/projects/pyspark-airflow-etl-project/dags \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
airflow dags list

AIRFLOW_HOME=/home/saad_abdullah/projects/pyspark-airflow-etl-project/.airflow \
AIRFLOW__CORE__DAGS_FOLDER=/home/saad_abdullah/projects/pyspark-airflow-etl-project/dags \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
airflow tasks list pyspark_batch_etl_pipeline
```

Run the complete DAG locally without starting the scheduler:

```bash
AIRFLOW_HOME=/home/saad_abdullah/projects/pyspark-airflow-etl-project/.airflow \
AIRFLOW__CORE__DAGS_FOLDER=/home/saad_abdullah/projects/pyspark-airflow-etl-project/dags \
AIRFLOW__CORE__LOAD_EXAMPLES=False \
airflow dags test pyspark_batch_etl_pipeline 2024-01-03
```

This executes all five Airflow tasks in order: ingest, clean, transform,
validate, and load PostgreSQL.

## Optional PySpark basics exercise

After installing the project, run:

```text
python examples/pyspark_basics.py
```

It demonstrates `select`, `filter`, `withColumn`, `groupBy`, `join`, and Parquet
writes with the small orders and customers files. Its output is isolated under
`data/processed/examples/` and is not part of the taxi pipeline.

## Recommended reading order

1. [Learning guide](docs/learning-guide.md)
2. [`config.py`](src/taxi_etl/config.py)
3. [`transformations.py`](src/taxi_etl/transformations.py)
4. [`pipeline.py`](src/taxi_etl/pipeline.py)
5. [`quality.py`](src/taxi_etl/quality.py)
6. [`postgres.py`](src/taxi_etl/postgres.py)
7. [`taxi_etl_dag.py`](dags/taxi_etl_dag.py)

For deeper design context, see [Architecture](docs/architecture.md).

## Troubleshooting

### Missing required input path

Download the taxi Parquet file and check that its year-month matches the configured
period. The error lists every missing path relative to the repository root.

### Java gateway or unsupported Java error

Confirm Java 17 is active and `JAVA_HOME` points to it. The project detects common
Linux Java 17 locations, but an explicit `JAVA_HOME` is the most reliable setup.

### Windows Hadoop write error

With the pinned Spark runtime, native Windows writes need
`hadoop/bin/winutils.exe` and `hadoop/bin/hadoop.dll`. These machine-specific
binaries are ignored by Git and must come from a source you trust. The pipeline
fails early with a readable list when they are absent. WSL is usually the simpler
environment when Airflow is also required.

### Airflow cannot find Python

The DAG defaults to `.venv/bin/python` and runs `run_pipeline.py` directly. Set
`TAXI_ETL_PYTHON_BIN` if Airflow should use a different Python executable.

### PostgreSQL loader cannot connect

Confirm the container is healthy with `docker ps`. If a local PostgreSQL service
already uses port `5432`, stop that service or change the published port in
`docker-compose.yml` and set `POSTGRES_PORT` to match.

### WSL Spark runs out of memory

Local Spark runs are more reliable when WSL has enough memory assigned. If Spark
workers exit unexpectedly under WSL, increase the WSL 2 memory limit in your
Windows user `.wslconfig`, then restart WSL.

## Airflow screenshots

The screenshots under `docs/screenshots/` show the DAG list, task graph, successful
run, and task logs from a local execution.

![Airflow DAG list](docs/screenshots/airflow-dag-list.jpg)

![PySpark ETL DAG graph](docs/screenshots/pyspark_batch_etl_pipeline-graph.png)

![Successful Airflow DAG run](docs/screenshots/airflow-successful-dag-run.jpg)

![Airflow task logs](docs/screenshots/airflow_task_logs.jpg)

## Ready for the next learning week

This version provides a clean Week 3 foundation: reusable transformations,
repeatable local runs, Airflow orchestration, Dockerized PostgreSQL, SQL
validation, tests, and explicit data contracts. Likely Week 4 extensions can now
be added without rewriting the foundation—for example cloud storage, dbt models,
incremental loads, or stronger observability.
