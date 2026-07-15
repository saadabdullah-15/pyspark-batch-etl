# Generated data

The pipeline recreates this directory, so generated Parquet files are ignored by
Git. Keep this README as a description of the expected layers.

```text
data/processed/
|-- raw/          # Stable Parquet copies of source inputs
|-- clean/        # Standardized trips and taxi zones
|-- analytics/    # Validated aggregate business tables
`-- examples/     # Optional orders/customers exercise output
```

From the repository root, generate all taxi layers with:

```text
python run_pipeline.py
```

It is safe to delete generated layer directories. The pipeline uses overwrite mode
and can recreate them from the files under `data/raw/`.
