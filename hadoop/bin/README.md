# Windows Hadoop Helpers

On Windows, local PySpark Parquet writes need Hadoop helper files in this folder:

```text
hadoop/bin/winutils.exe
hadoop/bin/hadoop.dll
```

These binaries are ignored by Git because they are local runtime dependencies, not project source code.
