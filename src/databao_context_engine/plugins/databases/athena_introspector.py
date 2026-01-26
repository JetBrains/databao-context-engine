from __future__ import annotations

from typing import Any, Annotated

from pyathena import connect
from pyathena.cursor import DictCursor
from pydantic import BaseModel, Field

from databao_context_engine.pluginlib.config import ConfigPropertyAnnotation
from databao_context_engine.plugins.base_db_plugin import BaseDatabaseConfigFile
from databao_context_engine.plugins.databases.base_introspector import BaseIntrospector, SQLQuery
from databao_context_engine.plugins.databases.databases_types import DatabaseSchema
from databao_context_engine.plugins.databases.introspection_model_builder import IntrospectionModelBuilder


class AwsProfileAuth(BaseModel):
    profile_name: str


class AwsIamAuth(BaseModel):
    aws_access_key_id: Annotated[str, ConfigPropertyAnnotation(secret=True)]
    aws_secret_access_key: Annotated[str, ConfigPropertyAnnotation(secret=True)]
    session_token: str | None = None


class AwsAssumeRoleAuth(BaseModel):
    role_arn: str | None = None
    role_session_name: str | None = None
    source_profile: str | None = None


class AwsDefaultAuth(BaseModel):
    # Uses environment variables, instance profile, ECS task role
    pass


class AthenaConnectionProperties(BaseModel):
    region_name: str
    schema_name: str = "default"
    catalog: str | None = "awsdatacatalog"
    work_group: str | None = None
    s3_staging_dir: str | None = None
    auth: AwsIamAuth | AwsProfileAuth | AwsDefaultAuth | AwsAssumeRoleAuth
    additional_properties: dict[str, Any] = {}

    def to_athena_kwargs(self) -> dict[str, Any]:
        kwargs = self.model_dump(
            exclude={
                "additional_properties": True,
            },
            exclude_none=True,
        )
        auth_fields = kwargs.pop("auth", {})
        kwargs.update(auth_fields)
        kwargs.update(self.additional_properties)
        return kwargs


class AthenaConfigFile(BaseDatabaseConfigFile):
    type: str = Field(default="athena")
    connection: AthenaConnectionProperties


class AthenaIntrospector(BaseIntrospector[AthenaConfigFile]):
    _IGNORED_SCHEMAS = {
        "information_schema",
    }
    supports_catalogs = True

    def _connect(self, file_config: AthenaConfigFile):
        return connect(**file_config.connection.to_athena_kwargs(), cursor_class=DictCursor)

    def _fetchall_dicts(self, connection, sql: str, params) -> list[dict]:
        with connection.cursor() as cur:
            cur.execute(sql, params or {})
            return cur.fetchall()

    def _get_catalogs(self, connection, file_config: AthenaConfigFile) -> list[str]:
        catalog = file_config.connection.catalog or self._resolve_pseudo_catalog_name(file_config)
        return [catalog]

    def _connect_to_catalog(self, file_config: AthenaConfigFile, catalog: str):
        return self._connect(file_config)

    def _sql_list_schemas(self, catalogs: list[str] | None) -> SQLQuery:
        if not catalogs:
            return SQLQuery("SELECT schema_name, catalog_name FROM information_schema.schemata", None)
        catalog = catalogs[0]
        sql = f"SELECT schema_name, catalog_name FROM {catalog}.information_schema.schemata"
        return SQLQuery(sql, None)

    def collect_catalog_model(self, connection, catalog: str, schemas: list[str]) -> list[DatabaseSchema] | None:
        if not schemas:
            return []

        comps = self._component_queries(catalog, schemas)
        results: dict[str, list[dict]] = {}

        for name, q in comps.items():
            results[name] = self._fetchall_dicts(connection, q, None)

        return IntrospectionModelBuilder.build_schemas_from_components(
            schemas=schemas,
            rels=results.get("relations", []),
            cols=results.get("columns", []),
            pk_cols=[],
            uq_cols=[],
            checks=[],
            fk_cols=[],
            idx_cols=[],
        )

    def _component_queries(self, catalog: str, schemas: list[str]) -> dict[str, str]:
        schemas_in = ", ".join(self._quote_literal(s) for s in schemas)
        return {
            "relations": self._sql_relations(catalog, schemas_in),
            "columns": self._sql_columns(catalog, schemas_in),
        }

    def _sql_relations(self, catalog: str, schemas_in: str) -> str:
        return f"""
            SELECT
                table_schema AS schema_name,
                table_name,
                CASE table_type
                    WHEN 'BASE TABLE' THEN 'table'
                    WHEN 'VIEW' THEN 'view'
                    ELSE LOWER(table_type)
                END AS kind,
                NULL AS description
            FROM 
                {catalog}.information_schema.tables
            WHERE 
                table_schema IN ({schemas_in})
        """

    def _sql_columns(self, catalog: str, schemas_in: str) -> str:
        return f"""
        SELECT 
            table_schema AS schema_name,
            table_name, 
            column_name, 
            ordinal_position, 
            data_type,
            is_nullable
        FROM 
            {catalog}.information_schema.columns
        WHERE 
            table_schema IN ({schemas_in})
        ORDER BY
            table_schema,
            table_name,
            ordinal_position
        """

    def _resolve_pseudo_catalog_name(self, file_config: AthenaConfigFile) -> str:
        return "awsdatacatalog"

    def _sql_sample_rows(self, catalog: str, schema: str, table: str, limit: int) -> SQLQuery:
        sql = f'SELECT * FROM "{schema}"."{table}" LIMIT %(limit)s'
        return SQLQuery(sql, {"limit": limit})

    def _quote_literal(self, value: str) -> str:
        return "'" + str(value).replace("'", "''") + "'"
