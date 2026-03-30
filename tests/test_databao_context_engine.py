from datetime import datetime

import pytest
import time_machine

from databao_context_engine import (
    DatabaoContextEngine,
    DatabaseSchemaLite,
    DatabaseTableDetails,
    DatabaseTableLite,
    Datasource,
    DatasourceContext,
    DatasourceId,
    DatasourceType,
)
from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext
from databao_context_engine.datasources.datasource_context import DatasourceContextHash
from databao_context_engine.plugins.databases.databases_types import (
    DatabaseCatalog,
    DatabaseColumn,
    DatabaseIntrospectionResult,
    DatabaseSchema,
    DatabaseTable,
)
from databao_context_engine.plugins.dbt.types import DbtContext, DbtSemanticLayer
from databao_context_engine.project.layout import DEPRECATED_ALL_RESULTS_FILE_NAME
from databao_context_engine.serialization.yaml import to_yaml_string
from tests.utils.project_creation import (
    given_datasource_config_file,
    given_output_dir_with_built_contexts,
)


def _database_built_context_yaml(*, datasource_id: str, datasource_type: str = "postgres") -> str:
    return to_yaml_string(
        BuiltDatasourceContext(
            datasource_id=datasource_id,
            datasource_type=datasource_type,
            context=DatabaseIntrospectionResult(
                catalogs=[
                    DatabaseCatalog(
                        name="analytics",
                        schemas=[
                            DatabaseSchema(
                                name="public",
                                tables=[
                                    DatabaseTable(
                                        name="orders",
                                        description="Customer orders",
                                        columns=[
                                            DatabaseColumn(name="id", type="INTEGER", nullable=False),
                                            DatabaseColumn(name="customer_id", type="INTEGER", nullable=False),
                                        ],
                                        samples=[],
                                    )
                                ],
                            )
                        ],
                    )
                ]
            ),
        )
    )


def test_databao_engine__can_not_be_created_on_non_existing_project(tmp_path):
    non_existing_project_dir = tmp_path / "non-existing-project"

    with pytest.raises(ValueError):
        DatabaoContextEngine(domain_dir=non_existing_project_dir)


def test_databao_engine__get_datasource_list_with_no_datasources(project_path):
    databao_context_engine = DatabaoContextEngine(domain_dir=project_path)

    given_output_dir_with_built_contexts(
        databao_context_engine._project_layout,
        [],
    )

    datasource_list = databao_context_engine.get_introspected_datasource_list()

    assert datasource_list == []


def test_databao_engine__get_datasource_list_with_multiple_datasources(project_path):
    databao_context_engine = DatabaoContextEngine(domain_dir=project_path)
    given_datasource_config_file(
        databao_context_engine._project_layout,
        datasource_name="full/a",
        config_content={"type": "any", "name": "a"},
    )
    given_datasource_config_file(
        databao_context_engine._project_layout,
        datasource_name="other/b",
        config_content={"type": "type", "name": "b"},
    )
    given_datasource_config_file(
        databao_context_engine._project_layout,
        datasource_name="full/c",
        config_content={"type": "type2", "name": "c"},
    )

    given_output_dir_with_built_contexts(
        databao_context_engine._project_layout,
        [
            (
                DatasourceId.from_string_repr("full/a.yaml"),
                to_yaml_string({"datasource_id": "full/a.yaml", "datasource_type": "any", "context": "Context for a"}),
            ),
            (
                DatasourceId.from_string_repr("other/b.yaml"),
                to_yaml_string(
                    {"datasource_id": "other/b.yaml", "datasource_type": "type", "context": "Context for b"}
                ),
            ),
        ],
    )

    datasource_list = databao_context_engine.get_introspected_datasource_list()

    assert datasource_list == [
        Datasource(id=DatasourceId.from_string_repr("full/a.yaml"), type=DatasourceType(full_type="any")),
        Datasource(id=DatasourceId.from_string_repr("other/b.yaml"), type=DatasourceType(full_type="type")),
    ]


def test_databao_engine__get_datasource_context(project_path):
    databao_context_engine = DatabaoContextEngine(domain_dir=project_path)

    given_output_dir_with_built_contexts(
        databao_context_engine._project_layout,
        [
            (DatasourceId.from_string_repr("full/a.yaml"), "Context for a"),
            (DatasourceId.from_string_repr("other/c.yaml"), "Context for c"),
            (DatasourceId.from_string_repr("full/b.yaml"), "Context for b"),
        ],
    )

    with time_machine.travel("2025-03-11 10:30:00", tick=False):
        result = databao_context_engine.get_datasource_context(DatasourceId.from_string_repr("full/b.yaml"))

        assert result == DatasourceContext(
            datasource_id=DatasourceId.from_string_repr("full/b.yaml"),
            context="Context for b",
            context_hash=DatasourceContextHash(
                datasource_id=DatasourceId.from_string_repr("full/b.yaml"),
                hash="d0558e050f773aafd7fb03d7b1480240",
                hash_algorithm="XXH3_128",
                hashed_at=datetime.now(),
            ),
        )


def test_databao_engine__get_datasource_contexts(project_path):
    databao_context_engine = DatabaoContextEngine(domain_dir=project_path)

    given_output_dir_with_built_contexts(
        databao_context_engine._project_layout,
        [
            (DatasourceId.from_string_repr("full/a.yaml"), "Context for a"),
            (DatasourceId.from_string_repr("other/c.yaml"), "Context for c"),
            (DatasourceId.from_string_repr("full/b.yaml"), "Context for b"),
        ],
    )

    with time_machine.travel("2025-03-11 10:30:00", tick=False):
        result = databao_context_engine.get_datasource_contexts(
            [DatasourceId.from_string_repr("full/b.yaml"), DatasourceId.from_string_repr("full/a.yaml")]
        )

        assert result == [
            DatasourceContext(
                datasource_id=DatasourceId.from_string_repr("full/b.yaml"),
                context="Context for b",
                context_hash=DatasourceContextHash(
                    datasource_id=DatasourceId.from_string_repr("full/b.yaml"),
                    hash="d0558e050f773aafd7fb03d7b1480240",
                    hash_algorithm="XXH3_128",
                    hashed_at=datetime.now(),
                ),
            ),
            DatasourceContext(
                datasource_id=DatasourceId.from_string_repr("full/a.yaml"),
                context="Context for a",
                context_hash=DatasourceContextHash(
                    datasource_id=DatasourceId.from_string_repr("full/a.yaml"),
                    hash="20bac267fb5190fd67d64f55aa04bb88",
                    hash_algorithm="XXH3_128",
                    hashed_at=datetime.now(),
                ),
            ),
        ]


def test_databao_engine__get_datasource_context_for_unbuilt_datasource(project_path):
    databao_context_engine = DatabaoContextEngine(domain_dir=project_path)

    given_output_dir_with_built_contexts(
        databao_context_engine._project_layout,
        [
            (DatasourceId.from_string_repr("full/a.yaml"), "Context for a"),
            (DatasourceId.from_string_repr("other/c.yaml"), "Context for c"),
            (DatasourceId.from_string_repr("full/b.yaml"), "Context for b"),
        ],
    )

    with pytest.raises(ValueError):
        databao_context_engine.get_datasource_context(DatasourceId.from_string_repr("full/d.yaml"))


def test_databao_engine__get_all_contexts(project_path):
    databao_context_engine = DatabaoContextEngine(domain_dir=project_path)

    given_output_dir_with_built_contexts(
        databao_context_engine._project_layout,
        [
            (DatasourceId.from_string_repr("full/a.yaml"), "Context for a"),
            (DatasourceId.from_string_repr("other/c.yaml"), "Context for c"),
            (DatasourceId.from_string_repr("full/b.yaml"), "Context for b"),
            (DatasourceId.from_string_repr("files/d.txt.yaml"), "Context for d"),
        ],
    )
    # Make sure backup files, duckdb and all_results.yaml files are ignored
    (databao_context_engine._project_layout.output_dir / DEPRECATED_ALL_RESULTS_FILE_NAME).touch()
    databao_context_engine._project_layout.db_path.touch()
    DatasourceId.from_string_repr("full/a.yaml").absolute_path_to_context_file(
        databao_context_engine._project_layout
    ).with_suffix(".yaml~").touch()

    with time_machine.travel("2025-03-11 10:30:00", tick=False):
        result = databao_context_engine.get_all_contexts()

        assert result == [
            DatasourceContext(
                datasource_id=DatasourceId.from_string_repr("files/d.txt"),
                context="Context for d",
                context_hash=DatasourceContextHash(
                    datasource_id=DatasourceId.from_string_repr("files/d.txt"),
                    hash="fdf1498af4c87c0476ad53e7f35ac13b",
                    hash_algorithm="XXH3_128",
                    hashed_at=datetime.now(),
                ),
            ),
            DatasourceContext(
                datasource_id=DatasourceId.from_string_repr("full/a.yaml"),
                context="Context for a",
                context_hash=DatasourceContextHash(
                    datasource_id=DatasourceId.from_string_repr("full/a.yaml"),
                    hash="20bac267fb5190fd67d64f55aa04bb88",
                    hash_algorithm="XXH3_128",
                    hashed_at=datetime.now(),
                ),
            ),
            DatasourceContext(
                datasource_id=DatasourceId.from_string_repr("full/b.yaml"),
                context="Context for b",
                context_hash=DatasourceContextHash(
                    datasource_id=DatasourceId.from_string_repr("full/b.yaml"),
                    hash="d0558e050f773aafd7fb03d7b1480240",
                    hash_algorithm="XXH3_128",
                    hashed_at=datetime.now(),
                ),
            ),
            DatasourceContext(
                datasource_id=DatasourceId.from_string_repr("other/c.yaml"),
                context="Context for c",
                context_hash=DatasourceContextHash(
                    datasource_id=DatasourceId.from_string_repr("other/c.yaml"),
                    hash="8625bd07459e6cf9543fda1ba57e60e8",
                    hash_algorithm="XXH3_128",
                    hashed_at=datetime.now(),
                ),
            ),
        ]


def test_databao_engine__get_all_contexts_formatted(project_path):
    databao_context_engine = DatabaoContextEngine(domain_dir=project_path)

    given_output_dir_with_built_contexts(
        databao_context_engine._project_layout,
        [
            (DatasourceId.from_string_repr("full/a.yaml"), "Context for a"),
            (DatasourceId.from_string_repr("other/c.yaml"), "Context for c"),
            (DatasourceId.from_string_repr("full/b.yaml"), "Context for b"),
        ],
    )

    result = databao_context_engine.get_all_contexts_formatted()

    assert (
        result.strip()
        == """
# ===== full/a.yaml =====
Context for a
# ===== full/b.yaml =====
Context for b
# ===== other/c.yaml =====
Context for c
    """.strip()
    )


def test_databao_engine__list_database_datasources(project_path):
    databao_context_engine = DatabaoContextEngine(domain_dir=project_path)
    given_output_dir_with_built_contexts(
        databao_context_engine._project_layout,
        [
            (
                DatasourceId.from_string_repr("databases/warehouse.yaml"),
                _database_built_context_yaml(datasource_id="databases/warehouse.yaml"),
            ),
            (
                DatasourceId.from_string_repr("analytics/dbt_project.yaml"),
                to_yaml_string(
                    BuiltDatasourceContext(
                        datasource_id="analytics/dbt_project.yaml", datasource_type="dbt", context={"result": "ok"}
                    )
                ),
            ),
        ],
    )

    assert databao_context_engine.list_database_datasources() == [
        Datasource(
            id=DatasourceId.from_string_repr("databases/warehouse.yaml"), type=DatasourceType(full_type="postgres")
        ),
    ]


def test_databao_engine__list_database_schema_tree_and_table_details(project_path):
    databao_context_engine = DatabaoContextEngine(domain_dir=project_path)
    given_output_dir_with_built_contexts(
        databao_context_engine._project_layout,
        [
            (
                DatasourceId.from_string_repr("databases/warehouse.yaml"),
                _database_built_context_yaml(datasource_id="databases/warehouse.yaml"),
            )
        ],
    )
    datasource_id = DatasourceId.from_string_repr("databases/warehouse.yaml")

    assert databao_context_engine.list_database_schemas_and_tables(datasource_id) == [
        DatabaseSchemaLite(
            datasource_id=str(datasource_id),
            catalog_name="analytics",
            schema_name="public",
            description=None,
            tables=[DatabaseTableLite(table_name="orders", description="Customer orders")],
        )
    ]

    details = databao_context_engine.get_database_table_details(datasource_id, "analytics", "public", "orders")

    assert details == DatabaseTableDetails(
        datasource_id="databases/warehouse.yaml",
        catalog_name="analytics",
        schema_name="public",
        table=DatabaseTable(
            name="orders",
            description="Customer orders",
            columns=[
                DatabaseColumn(name="id", type="INTEGER", nullable=False),
                DatabaseColumn(name="customer_id", type="INTEGER", nullable=False),
            ],
            samples=[],
        ),
    )


def test_databao_engine__database_metadata_errors(project_path):
    databao_context_engine = DatabaoContextEngine(domain_dir=project_path)

    dbt_datasource_id = DatasourceId.from_string_repr("analytics/dbt_project.yaml")
    warehouse_datasource_id = DatasourceId.from_string_repr("databases/warehouse.yaml")
    given_output_dir_with_built_contexts(
        project_layout=databao_context_engine._project_layout,
        contexts=[
            (
                dbt_datasource_id,
                to_yaml_string(
                    {
                        "datasource_id": str(dbt_datasource_id),
                        "datasource_type": "dbt",
                        "context": DbtContext(
                            models=[], semantic_layer=DbtSemanticLayer(semantic_models=[], metrics=[])
                        ),
                    }
                ),
            ),
            (
                warehouse_datasource_id,
                to_yaml_string(
                    {
                        "datasource_id": str(warehouse_datasource_id),
                        "datasource_type": "postgres",
                        "context": DatabaseIntrospectionResult(catalogs=[]),
                    }
                ),
            ),
        ],
    )

    with pytest.raises(ValueError, match="Context file was not built"):
        databao_context_engine.list_database_schemas_and_tables(DatasourceId.from_string_repr("missing/source.yaml"))

    with pytest.raises(ValueError, match="not database-capable"):
        databao_context_engine.list_database_schemas_and_tables(dbt_datasource_id)
