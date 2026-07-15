# Learning Guide

Use this guide to understand the project before changing it. The goal is to trace
one record through the system, then connect that path to the orchestration layer.

## 1. Start with configuration

Open `src/taxi_etl/config.py` and find `PipelineConfig.from_environment`.

Notice three ideas:

1. Defaults make the project runnable on a laptop.
2. Environment variables allow Airflow or a terminal to change runtime behavior.
3. `PipelinePaths` derives paths instead of scattering string literals across jobs.

Follow `taxi_trips_source` and see how year and month produce the expected raw
filename.

## 2. Follow a taxi row through cleaning

Open `clean_taxi_trips` in `src/taxi_etl/transformations.py`.

Read it in four blocks:

1. `assert_required_columns` gives an early, readable source-schema failure.
2. `select` renames and casts source columns.
3. `withColumn` creates passenger defaults, dates, duration, and partitions.
4. `filter` expresses the accepted business ranges.

Spark DataFrames are lazy. These calls build a plan; they do not process all rows
immediately. Actions such as `count`, `write`, `collect`, and `show` start the work.

## 3. Understand the two zone joins

Open `enrich_trips_with_zones`.

A trip contains two location IDs, so the same zone table is given two aliases:
`pickup` and `dropoff`. Each alias is joined with a different key. The final
`select` keeps every trip field and adds four readable location fields.

This alias pattern is common when one dimension plays multiple roles.

## 4. Compare DataFrame code with SQL

Read `build_daily_revenue`, then read `build_sql_summaries`.

Both approaches build Spark execution plans. The DataFrame API composes Python
methods; Spark SQL expresses the same kinds of grouping and aggregation in SQL.
Choosing between them is usually about team readability and the transformation's
shape, not a different execution engine.

## 5. Separate logic from I/O

Open `src/taxi_etl/pipeline.py` and locate `run_clean`.

The stage does the operational work:

```text
read raw Parquet
    -> call clean transformation functions
    -> count and reject empty results
    -> write clean Parquet
```

The transformation file does not know these directories exist. That is why tests
can create tiny DataFrames and exercise the same rules without downloading the
large taxi file.

## 6. Read a data contract

Open `ANALYTICS_CONTRACTS` in `src/taxi_etl/quality.py`.

Choose `daily_revenue` and compare its contract with
`build_daily_revenue`. The producer and validator agree on columns and the row's
business key. Then read `validate_table` to see how null keys, duplicate keys, and
negative measures fail the pipeline.

## 7. Finish at the entry points

The execution path for a local run is:

```text
run_pipeline.py
    -> taxi_etl.cli.main
    -> taxi_etl.pipeline.run_pipeline
    -> run_ingest / run_clean / run_transform / run_validate
```

Airflow calls the same `run_pipeline.py` file one stage at a time. This is an
important professional habit: orchestration should invoke the pipeline, not hold a
second copy of its business rules.

## Practice exercises

Try these in order:

1. Run `python run_pipeline.py --help` and one individual stage.
2. Add a test trip with seven passengers and prove cleaning removes it.
3. Add `maximum_trip_revenue` to `daily_revenue` and its contract.
4. Create a summary grouped by pickup weekday.
5. Intentionally duplicate a business key in a quality test and inspect the error.

Each exercise touches one boundary at a time, which keeps experimentation safe and
easy to understand.
