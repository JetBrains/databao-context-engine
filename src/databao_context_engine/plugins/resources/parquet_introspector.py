import contextlib
import logging
import uuid
from collections import defaultdict
from dataclasses import replace
from urllib.parse import urlparse

import duckdb
from duckdb import DuckDBPyConnection

from databao_context_engine.pluginlib.config import DuckDBSecret
from databao_context_engine.plugins.duckdb_tools import fetchall_dicts, generate_create_secret_sql
from databao_context_engine.plugins.resources.types import (
    ParquetColumn,
    ParquetConfigFile,
    ParquetFile,
    ParquetIntrospectionResult,
)

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def _create_secret(conn: DuckDBPyConnection, duckdb_secret: DuckDBSecret):
    secret_name = duckdb_secret.name or "gen_secret_" + str(uuid.uuid4()).replace("-", "_")
    create_secret_sql = generate_create_secret_sql(secret_name, duckdb_secret)
    try:
        logger.debug(f"About to create duckdb secret '{secret_name}' with type {duckdb_secret.type}")
        conn.sql(create_secret_sql)
        yield conn
    finally:
        logger.debug(f"Dropping duckdb secret '{secret_name}'")
        conn.sql(f"DROP SECRET IF EXISTS {secret_name};")


def _resolve_url(file_config: ParquetConfigFile) -> str:
    parquet_url = urlparse(file_config.url)
    if parquet_url.scheme == "file":
        return parquet_url.netloc
    return file_config.url


class ParquetIntrospector:
    @contextlib.contextmanager
    def _connect(self, file_config: ParquetConfigFile):
        duckdb_secret = file_config.duckdb_secret
        with duckdb.connect() as conn:
            if duckdb_secret is not None:
                if duckdb_secret.type == "s3":
                    conn.execute("INSTALL httpfs;")
                    conn.execute("LOAD httpfs;")
                    conn.execute("INSTALL s3;")
                    conn.execute("LOAD s3;")
                with _create_secret(conn, duckdb_secret):
                    yield conn
            else:
                yield conn

    def check_connection(self, file_config: ParquetConfigFile) -> None:
        with self._connect(file_config) as conn:
            with conn.cursor() as cur:
                resolved_url = _resolve_url(file_config)
                parquet_file_metadata = fetchall_dicts(
                    cur, f"SELECT * FROM parquet_file_metadata('{resolved_url}') LIMIT 1"
                )
                if not parquet_file_metadata:
                    raise ValueError(f"No parquet files found by url {resolved_url}")
                if not parquet_file_metadata or not parquet_file_metadata[0]["file_name"]:
                    raise ValueError("Parquet resource introspection failed")

    def introspect(self, file_config: ParquetConfigFile) -> ParquetIntrospectionResult:
        with self._connect(file_config) as conn:
            with conn.cursor() as cur:
                resolved_url = _resolve_url(file_config)
                file_metas = fetchall_dicts(cur, f"SELECT * from parquet_metadata('{resolved_url}')")

                columns_per_file: dict[str, dict[int, ParquetColumn]] = defaultdict(defaultdict)
                for file_meta in file_metas:
                    file_name = file_meta["file_name"]
                    column_id = file_meta["column_id"]
                    column_name = file_meta["path_in_schema"]
                    column_type = file_meta.get("type") or ""
                    num_values: int = file_meta["num_values"]
                    stats_min = file_meta.get("stats_min") or ""
                    stats_max = file_meta.get("stats_max") or ""
                    stats_null_count: int | None = file_meta.get("stats_null_count")
                    stats_distinct_count: int | None = file_meta.get("stats_distinct_count")

                    columns: dict[int, ParquetColumn] = columns_per_file[file_name]
                    column: ParquetColumn | None = columns.get(column_id)
                    if column:
                        columns[column_id] = replace(
                            column, num_values=column.num_values + num_values, row_groups=column.row_groups + 1
                        )
                    else:
                        columns[column_id] = ParquetColumn(
                            name=column_name,
                            type=column_type,
                            row_groups=1,
                            num_values=num_values,
                            stats_min=stats_min,
                            stats_max=stats_max,
                            stats_null_count=stats_null_count,
                            stats_distinct_count=stats_distinct_count,
                        )

                return ParquetIntrospectionResult(
                    files=[
                        ParquetFile(file_name, columns=list(columns.values()))
                        for (file_name, columns) in columns_per_file.items()
                    ]
                )
