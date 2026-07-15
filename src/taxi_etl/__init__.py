"""A small, production-shaped PySpark batch ETL project.

The package keeps Spark transformations separate from filesystem orchestration so
that each part can be read, tested, and changed independently.
"""

from taxi_etl.config import PipelineConfig, PipelinePaths

__all__ = ["PipelineConfig", "PipelinePaths"]
