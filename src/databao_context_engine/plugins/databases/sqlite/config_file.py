from pydantic import BaseModel, Field

from databao_context_engine.plugins.databases.base_db_plugin import BaseDatabaseConfigFile


class SQLiteConnectionConfig(BaseModel):
    database_path: str = Field(description="Path to the SQLite database file")


class SQLiteConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="sqlite")
    connection: SQLiteConnectionConfig
