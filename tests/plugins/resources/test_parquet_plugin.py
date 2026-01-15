from dataclasses import dataclass
from pathlib import Path
from unittest.mock import ANY

import duckdb
import pytest

from nemory.pluginlib.build_plugin import DatasourceType, EmbeddableChunk
from nemory.pluginlib.config import DuckDBSecret
from nemory.pluginlib.plugin_utils import execute_datasource_plugin
from nemory.plugins.parquet_plugin import ParquetPlugin
from nemory.plugins.resources.parquet_chunker import ParquetColumnChunkContent
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


@dataclass(frozen=True)
class TestParquetFiles:
    path: Path
    file: Path
    file_with_row_groups: Path


@pytest.fixture
def _test_parquet_files(request, tmp_path: Path):
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
    return TestParquetFiles(path=tmp_path, file=parquet_file, file_with_row_groups=parquet_file_with_row_groups)


def test_glob_parquet_files(_test_parquet_files: TestParquetFiles):
    plugin = ParquetPlugin()
    config = ParquetConfigFile(type=parquet_type, url=f"{_test_parquet_files.path}/*.parquet")
    result = plugin.build_context("resources/parquet", "test", file_config=config)
    # noinspection PyTypeChecker
    assert result == ParquetIntrospectionResult(
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


def test_parquet_files_chunks(_test_parquet_files: TestParquetFiles):
    plugin = ParquetPlugin()
    filename = str(_test_parquet_files.file)
    config = {
        "type": "resources/parquet",
        "url": filename,
    }

    result = execute_datasource_plugin(plugin, DatasourceType(full_type=config["type"]), config, "file_name")
    chunks = plugin.divide_context_into_chunks(result)
    assert chunks == [
        EmbeddableChunk(
            embeddable_text=f"Column [name = doubles, type = DOUBLE, number of values = 101] in parquet file {filename}",
            content=ParquetColumnChunkContent(
                file_name=f"{filename}",
                column=ParquetColumn(
                    name="doubles",
                    type="DOUBLE",
                    row_groups=1,
                    num_values=101,
                    stats_min="0.0",
                    stats_max="100.0",
                    stats_null_count=0,
                    stats_distinct_count=None,
                ),
            ),
        )
    ]
