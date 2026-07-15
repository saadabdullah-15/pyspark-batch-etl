# Optional Windows Hadoop helpers

The project's pinned PySpark runtime needs these local files before it can write
Parquet on native Windows:

```text
hadoop/bin/winutils.exe
hadoop/bin/hadoop.dll
```

The project checks both files, sets `HADOOP_HOME`, and adds this directory to the
process `PATH`. The binaries are machine-specific and intentionally ignored by
Git. Obtain them only from a source you trust. If they are missing, the pipeline
stops before Spark starts and explains the two available choices: add trusted
copies or run in WSL.

Linux and WSL runs do not use this directory.
