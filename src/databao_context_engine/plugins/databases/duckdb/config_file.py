from __future__ import annotations

from pydantic import BaseModel, Field

from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabaseConfigFile


class DuckDBConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="duckdb")
    connection: DuckDBConnectionConfig


class DuckDBConnectionConfig(BaseModel):
    database_path: str = Field(description="Path to the DuckDB database file")
