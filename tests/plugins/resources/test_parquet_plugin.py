from pathlib import Path
from unittest.mock import ANY

import duckdb

from nemory.pluginlib.config import DuckDBSecret
from nemory.plugins.parquet_plugin import ParquetPlugin
from nemory.plugins.resources.parquet_introspector import (
    ParquetColumn,
    ParquetConfigFile,
    ParquetFile,
    ParquetIntrospectionResult,
    generate_create_secret_sql,
    parquet_type,
)


def test_duckdb_secret_generation():
    generated_sql = generate_create_secret_sql(
        "test_secret",
        DuckDBSecret(
            name="test_secret",
            type="s3",
            properties={"provider": "credential_chain", "profile": "sandbox", "chain": "sso"},
        ),
    )
    assert (
        generated_sql.strip()
        == """CREATE SECRET test_secret (
    type s3, provider credential_chain, profile sandbox, chain sso
);"""
    )


def test_glob_parquet_files(request, tmp_path: Path):
    name = request.function.__name__
    parquet_file_with_row_groups = tmp_path / f"{name}_with_row_groups.parquet"
    parquet_file = tmp_path / f"{name}.parquet"

    with duckdb.connect() as conn:
        conn.sql(
            f"""COPY (FROM 
(SELECT i as id, CAST(i AS VARCHAR) || '_test_name' AS name FROM generate_series(5_000) tbl(i))
)
TO '{parquet_file_with_row_groups}'
(FORMAT parquet, ROW_GROUP_SIZE 4000);"""
        )

        conn.sql(
            f"""COPY (FROM 
(SELECT i::DOUBLE as doubles FROM generate_series(100) tbl(i))
)
TO '{parquet_file}'
(FORMAT parquet);"""
        )

    plugin = ParquetPlugin()
    config = ParquetConfigFile(type=parquet_type, url=f"{tmp_path}/*.parquet")
    result = plugin.execute("resources/parquet", "test", file_config=config)
    # noinspection PyTypeChecker
    assert result.result == ParquetIntrospectionResult(
        files=[
            ParquetFile(
                name=ANY,
                columns=[
                    ParquetColumn(
                        name="doubles",
                        type="DOUBLE",
                        row_groups=1,
                        num_values=101,
                        stats_min="0.0",
                        stats_max="100.0",
                        stats_null_count=0,
                        stats_distinct_count=None,
                    )
                ],
            ),
            ParquetFile(
                name=ANY,
                columns=[
                    ParquetColumn(
                        name="id",
                        type="INT64",
                        row_groups=2,
                        num_values=5001,
                        stats_min="0",
                        stats_max="4095",
                        stats_null_count=0,
                        stats_distinct_count=None,
                    ),
                    ParquetColumn(
                        name="name",
                        type="BYTE_ARRAY",
                        row_groups=2,
                        num_values=5001,
                        stats_min="0_test_name",
                        stats_max="9_test_name",
                        stats_null_count=0,
                        stats_distinct_count=None,
                    ),
                ],
            ),
        ]
    )
