# Architecture

This document explains why the repository is organized the way it is. Start with
the root `README.md` if you only want to install and run the project.

## Data flow

```text
Source files
    |
    |  ingest: verify columns and normalize file format
    v
Raw layer (Parquet)
    |
    |  clean: standardize, derive columns, enforce row rules
    v
Clean layer (Parquet, partitioned by year/month)
    |
    |  transform: join zones and aggregate business metrics
    v
Analytics layer (six Parquet tables)
    |
    |  validate: enforce persisted table contracts
    v
Trusted local outputs
```

The layers have intentionally simple names—`raw`, `clean`, and `analytics`—so a
student can understand the progression without first learning platform-specific
terminology. They correspond broadly to bronze, silver, and gold layers.

## Code boundaries

### Configuration: `config.py`

`PipelineConfig` owns runtime values such as reporting month, Spark master, and
timezone. `PipelinePaths` derives every source and destination from the repository
root. No stage contains a hard-coded absolute path.

### Runtime: `spark.py`

Spark session creation lives in one place. Every stage therefore uses the same
memory, shuffle, compression, timezone, Java, and local Hadoop behavior.

### Business logic: `transformations.py`

Transformation functions accept DataFrames and return DataFrames. They do not
open files, stop Spark, or depend on a particular directory. This is the key
testability boundary in the project.

### Orchestration: `pipeline.py`

Stage functions read inputs, call transformations, write outputs, and manage the
Spark session. `run_pipeline` defines the canonical order. Both the CLI and Airflow
reuse these same functions instead of duplicating ETL logic.

### Contracts: `quality.py`

An analytics contract records required columns, business keys, and non-negative
metrics. Validation happens after Parquet writes, so it also proves the outputs can
be found and read successfully.

### External scheduling: `dags/taxi_etl_dag.py`

Airflow owns dependencies, retries, task state, and logs. Each Airflow task calls
one CLI stage through `spark-submit`. The DAG contains no business logic.

## Important design decisions

### One reporting period per run

The year and month come from environment variables. The input filename and date
filter use the same configuration, preventing a January filename from silently
producing February rows.

### End-exclusive date filtering

Cleaning keeps pickup dates greater than or equal to the first day of the selected
month and less than the first day of the next month. This works for every month,
including December, without timestamp edge cases.

### Explicit zone schema

The small CSV is read with known types. Schema inference is convenient for an
exercise, but a pipeline should not change behavior because a future file happens
to contain different-looking values.

### Persist only reused DataFrames

Ingested and cleaned data is counted and written, while enriched trips feed six
aggregations. These reused DataFrames are persisted with `MEMORY_AND_DISK` and
released after the stage.

### Stable business labels and sort keys

Distance bands have a readable label and a separate numeric order. Presentation
text no longer needs artificial prefixes such as `00` and `01` merely to sort in
the expected order.

### Manual Airflow schedule

The source is a fixed monthly snapshot and every run overwrites the selected
month's local outputs. A daily schedule would repeat identical work, so the
learning DAG is manually triggered.

## Extension points

Future work can fit into existing boundaries:

- Add a source by extending paths and the ingest stage.
- Add a business table by writing one transformation and one analytics contract.
- Add a quality rule inside `TableContract` and `validate_table`.
- Add run metadata around stage calls in `pipeline.py`.
- Replace local paths with object-storage URIs behind a configuration layer.
- Add incremental writes by changing stage I/O without rewriting transformations.

These are natural directions for a later learning week because the current code
already separates what changes from what should remain stable.
