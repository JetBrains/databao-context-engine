from databao_context_engine.plugins.databases.context_enricher import enrich_database_context
from databao_context_engine.plugins.databases.databases_types import (
    DatabaseCatalog,
    DatabaseColumn,
    DatabaseIntrospectionResult,
    DatabaseSchema,
    DatabaseTable,
)
from tests.utils.fakes import FakeDescriptionProvider


def _build_context(*, with_existing_descriptions: bool) -> DatabaseIntrospectionResult:
    return DatabaseIntrospectionResult(
        catalogs=[
            DatabaseCatalog(
                name="default",
                description="catalog-existing" if with_existing_descriptions else None,
                schemas=[
                    DatabaseSchema(
                        name="main",
                        description="schema-existing" if with_existing_descriptions else None,
                        tables=[
                            DatabaseTable(
                                name="users",
                                description="table-existing" if with_existing_descriptions else None,
                                columns=[
                                    DatabaseColumn(
                                        name="id",
                                        type="INTEGER",
                                        nullable=False,
                                        description="column-existing" if with_existing_descriptions else None,
                                    )
                                ],
                                samples=[],
                            )
                        ],
                    )
                ],
            )
        ]
    )


def test_enrich_database_context_adds_descriptions_when_missing():
    context = _build_context(with_existing_descriptions=False)
    provider = FakeDescriptionProvider()

    enriched = enrich_database_context(context, provider)

    catalog = enriched.catalogs[0]
    schema = catalog.schemas[0]
    table = schema.tables[0]
    column = table.columns[0]

    assert catalog.description == "fake-desc::default"
    assert schema.description == "fake-desc::main"
    assert table.description == "fake-desc::users"
    assert column.description is not None
    assert column.description.startswith("fake-desc::")
    assert len(provider.calls) == 4


def test_enrich_database_context_keeps_existing_descriptions():
    context = _build_context(with_existing_descriptions=True)
    provider = FakeDescriptionProvider()

    enriched = enrich_database_context(context, provider)

    catalog = enriched.catalogs[0]
    schema = catalog.schemas[0]
    table = schema.tables[0]
    column = table.columns[0]

    assert catalog.description == "catalog-existing"
    assert schema.description == "schema-existing"
    assert table.description == "table-existing"
    assert column.description == "column-existing"
    assert provider.calls == []


def test_enrich_database_context_keeps_original_values_on_description_failure():
    context = _build_context(with_existing_descriptions=False)
    provider = FakeDescriptionProvider(fail_at={0})

    enriched = enrich_database_context(context, provider)

    catalog = enriched.catalogs[0]
    schema = catalog.schemas[0]
    table = schema.tables[0]
    column = table.columns[0]

    assert column.description is None
    assert table.description == "fake-desc::users"
    assert schema.description == "fake-desc::main"
    assert catalog.description == "fake-desc::default"
    assert len(provider.calls) == 4
