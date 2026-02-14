from dataclasses import dataclass

from pydantic import BaseModel, Field

from databao_context_engine.pluginlib.build_plugin import AbstractConfigFile
from databao_context_engine.pluginlib.config import DuckDBSecret

parquet_type = "parquet"


class ParquetConfigFile(BaseModel, AbstractConfigFile):
    name: str
    type: str = Field(default="parquet")
    url: str = Field(
        description="Parquet resource location. Should be a valid URL or a path to a local file. "
        "Examples: s3://your_bucket/file.parquet, s3://your-bucket/*.parquet, https://some.url/some_file.parquet, ~/path_to/file.parquet",
    )
    duckdb_secret: DuckDBSecret | None = None


@dataclass
class ParquetColumn:
    name: str
    type: str
    row_groups: int
    num_values: int
    stats_min: str
    stats_max: str
    stats_null_count: int | None
    stats_distinct_count: int | None


@dataclass
class ParquetFile:
    name: str
    columns: list[ParquetColumn]


@dataclass
class ParquetIntrospectionResult:
    files: list[ParquetFile]
