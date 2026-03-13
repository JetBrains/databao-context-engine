from datetime import datetime
from typing import Any

import pytest
import yaml
from pydantic import BaseModel

from databao_context_engine import DatasourceContext, DatasourceId
from databao_context_engine.build_sources.build_service import BuildService
from databao_context_engine.build_sources.plugin_execution import BuiltDatasourceContext
from databao_context_engine.datasources.datasource_context import DatasourceContextHash
from databao_context_engine.datasources.types import PreparedConfig, PreparedDatasource
from databao_context_engine.pluginlib.build_plugin import DatasourceType, DefaultBuildDatasourcePlugin
from databao_context_engine.plugins.plugin_loader import DatabaoContextPluginLoader, NoPluginFoundForDatasource
from tests.utils.dummy_build_plugin import DummyDefaultDatasourcePlugin, DummyEnrichableDatasourcePlugin


def mk_result(*, name: str = "files/foo.md", typ: str = "files/md", result=None) -> BuiltDatasourceContext:
    return BuiltDatasourceContext(
        datasource_id=name,
        datasource_type=typ,
        context=result if result is not None else {"ok": True},
    )


def mk_prepared_config(
    *,
    path: str,
    full_type: str,
    config: dict[str, Any] | None = None,
    datasource_name: str = "demo",
) -> PreparedDatasource:
    return PreparedConfig(
        datasource_id=DatasourceId.from_string_repr(path),
        datasource_type=DatasourceType(full_type=full_type),
        config=config or {"type": full_type, "name": datasource_name},
        datasource_name=datasource_name,
    )


def mk_context(*, path: str, payload: dict, context_hash: DatasourceContextHash | None = None) -> DatasourceContext:
    datasource_id = DatasourceId.from_string_repr(path)
    return DatasourceContext(
        datasource_id=datasource_id,
        context=yaml.safe_dump(payload),
        context_hash=context_hash
        or DatasourceContextHash(
            datasource_id=datasource_id,
            hash="irrelevant for this test",
            hash_algorithm="irrelevant for this test",
            hashed_at=datetime.now(),
        ),
    )


class DemoContext(BaseModel):
    title: str


class TypedDummyPlugin(DefaultBuildDatasourcePlugin):
    id = "tests/typed_dummy"
    name = "Typed Dummy Plugin"
    context_type = DemoContext

    def supported_types(self) -> set[str]:
        return {"typed_dummy"}

    def build_context(self, full_type: str, datasource_name: str, file_config: dict[str, Any]) -> Any:
        return {"title": datasource_name}

    def enrich_context(self, context: Any, description_provider: Any) -> Any:
        return {"summary": f"done:{context.title}"}

    def divide_context_into_chunks(self, context: Any) -> list:
        return []


@pytest.fixture
def chunk_embed_svc(mocker):
    return mocker.Mock(name="ChunkEmbeddingService")


@pytest.fixture
def plugin_loader():
    return DatabaoContextPluginLoader(
        plugins_by_type={
            DatasourceType(full_type="dummy_default"): DummyDefaultDatasourcePlugin(),
            DatasourceType(full_type="dummy_enrichable"): DummyEnrichableDatasourcePlugin(),
        }
    )


@pytest.fixture
def svc(chunk_embed_svc, project_layout, plugin_loader):
    return BuildService(
        project_layout=project_layout,
        chunk_embedding_service=chunk_embed_svc,
        plugin_loader=plugin_loader,
    )


@pytest.fixture
def description_provider(mocker):
    return mocker.Mock(name="DescriptionProvider")


def test_build_context_returns_dummy_plugin_result(svc):
    prepared = mk_prepared_config(path="dummy/source.yaml", full_type="dummy_default")

    out = svc.build_context(prepared_source=prepared)

    assert out == BuiltDatasourceContext(
        datasource_id="dummy/source.yaml",
        datasource_type="dummy_default",
        context={"ok": True},
    )


def test_build_context_bubbles_execute_plugin_error(svc, mocker):
    prepared = mk_prepared_config(path="dummy/source.yaml", full_type="dummy_default")
    mocker.patch(
        "databao_context_engine.build_sources.build_service.execute_plugin", side_effect=RuntimeError("exec-fail")
    )

    with pytest.raises(RuntimeError, match="exec-fail"):
        svc.build_context(prepared_source=prepared)


def test_build_context_raises_when_plugin_is_missing(svc):
    prepared = mk_prepared_config(path="dummy/source.yaml", full_type="missing_plugin")

    with pytest.raises(NoPluginFoundForDatasource):
        svc.build_context(prepared_source=prepared)


def test_index_datasource_context_embeds_deserialized_chunks(svc, chunk_embed_svc):
    ctx = mk_context(
        path="dummy/source.yaml",
        payload={
            "datasource_id": "dummy/source.yaml",
            "datasource_type": "dummy_default",
            "context": {"ok": True},
        },
    )

    chunk_embed_svc.is_index_up_to_date.return_value = False

    svc.index_datasource_context(context=ctx)

    chunk_embed_svc.embed_chunks.assert_called_once_with(
        chunks=[DummyDefaultDatasourcePlugin().divide_context_into_chunks({"ok": True})[0]],
        context_hash=ctx.context_hash,
        full_type="dummy_default",
        datasource_id="dummy/source.yaml",
        override=False,
    )


def test_index_datasource_context_skips_when_index_is_up_to_date(svc, chunk_embed_svc):
    ctx = mk_context(
        path="dummy/source.yaml",
        payload={
            "datasource_id": "dummy/source.yaml",
            "datasource_type": "dummy_default",
            "context": {"ok": True},
        },
    )
    chunk_embed_svc.is_index_up_to_date.return_value = True

    svc.index_datasource_context(context=ctx)

    chunk_embed_svc.embed_chunks.assert_not_called()


def test_index_datasource_context_force_index_bypasses_up_to_date_check(svc, chunk_embed_svc):
    ctx = mk_context(
        path="dummy/source.yaml",
        payload={
            "datasource_id": "dummy/source.yaml",
            "datasource_type": "dummy_default",
            "context": {"ok": True},
        },
    )

    svc.index_datasource_context(context=ctx, force_index=True)

    chunk_embed_svc.is_index_up_to_date.assert_not_called()
    chunk_embed_svc.embed_chunks.assert_called_once_with(
        chunks=[DummyDefaultDatasourcePlugin().divide_context_into_chunks({"ok": True})[0]],
        context_hash=ctx.context_hash,
        full_type="dummy_default",
        datasource_id="dummy/source.yaml",
        override=True,
    )


def test_index_built_context_embeds_using_datasource_type_lookup(svc, chunk_embed_svc):
    built_context = mk_result(name="dummy/source.yaml", typ="dummy_default", result={"ok": True})
    context_hash = DatasourceContextHash(
        datasource_id=DatasourceId.from_string_repr("dummy/source.yaml"),
        hash="hash-123",
        hash_algorithm="XXH3_128",
        hashed_at=datetime.now(),
    )
    chunk_embed_svc.is_index_up_to_date.return_value = False

    svc.index_built_context(built_context=built_context, context_hash=context_hash)

    chunk_embed_svc.embed_chunks.assert_called_once_with(
        chunks=[DummyDefaultDatasourcePlugin().divide_context_into_chunks({"ok": True})[0]],
        context_hash=context_hash,
        full_type="dummy_default",
        datasource_id="dummy/source.yaml",
        override=False,
    )


def test_index_datasource_context_no_chunks_skips_embed(svc, chunk_embed_svc):
    ctx = mk_context(
        path="dummy/enrichable.yaml",
        payload={
            "datasource_id": "dummy/enrichable.yaml",
            "datasource_type": "dummy_enrichable",
            "context": {"value": "demo", "description": None},
        },
    )
    chunk_embed_svc.is_index_up_to_date.return_value = False

    svc.index_datasource_context(context=ctx)

    chunk_embed_svc.embed_chunks.assert_not_called()


def test_enrich_built_context_returns_replaced_context(
    chunk_embed_svc, description_provider, project_layout, plugin_loader
):
    svc = BuildService(
        project_layout=project_layout,
        chunk_embedding_service=chunk_embed_svc,
        plugin_loader=plugin_loader,
        description_provider=description_provider,
    )
    built_context = mk_result(
        name="dummy/enrichable.yaml",
        typ="dummy_enrichable",
        result={"value": "hello", "description": None},
    )
    description_provider.describe.return_value = "generated-description"

    result = svc.enrich_built_context(built_context)

    assert result == BuiltDatasourceContext(
        datasource_id="dummy/enrichable.yaml",
        datasource_type="dummy_enrichable",
        context={"value": "hello", "description": "ENRICHED::generated-description"},
    )
    description_provider.describe.assert_called_once_with(text="hello", context="dummy_enrichable")


def test_enrich_built_context_raises_without_description_provider(svc):
    built_context = mk_result(
        name="dummy/enrichable.yaml",
        typ="dummy_enrichable",
        result={"value": "hello", "description": None},
    )

    with pytest.raises(ValueError, match="Prompt provider should never be None"):
        svc.enrich_built_context(built_context)


def test_enrich_datasource_context_deserializes_to_typed_context(chunk_embed_svc, description_provider, project_layout):
    plugin_loader = DatabaoContextPluginLoader(
        plugins_by_type={DatasourceType(full_type="typed_dummy"): TypedDummyPlugin()}
    )
    svc = BuildService(
        project_layout=project_layout,
        chunk_embedding_service=chunk_embed_svc,
        plugin_loader=plugin_loader,
        description_provider=description_provider,
    )
    context = mk_context(
        path="dummy/typed.yaml",
        payload={
            "datasource_id": "dummy/typed.yaml",
            "datasource_type": "typed_dummy",
            "context": {"title": "hello"},
        },
    )

    result = svc.enrich_datasource_context(context)

    assert result == BuiltDatasourceContext(
        datasource_id="dummy/typed.yaml",
        datasource_type="typed_dummy",
        context={"summary": "done:hello"},
    )


def test_index_context_if_necessary_reindexes_only_stale_contexts(svc, chunk_embed_svc, mocker):
    stale_hash = DatasourceContextHash(
        datasource_id=DatasourceId.from_string_repr("dummy/stale.yaml"),
        hash="stale",
        hash_algorithm="XXH3_128",
        hashed_at=datetime.now(),
    )
    fresh_hash = DatasourceContextHash(
        datasource_id=DatasourceId.from_string_repr("dummy/fresh.yaml"),
        hash="fresh",
        hash_algorithm="XXH3_128",
        hashed_at=datetime.now(),
    )
    context = mk_context(
        path="dummy/stale.yaml",
        payload={
            "datasource_id": "dummy/stale.yaml",
            "datasource_type": "dummy_default",
            "context": {"ok": True},
        },
        context_hash=stale_hash,
    )

    chunk_embed_svc.is_index_up_to_date.side_effect = [False, True]
    get_datasource_context = mocker.patch(
        "databao_context_engine.build_sources.build_service.get_datasource_context",
        return_value=context,
    )
    index_datasource_context = mocker.patch.object(svc, "index_datasource_context")

    svc.index_context_if_necessary([stale_hash, fresh_hash])

    get_datasource_context.assert_called_once_with(svc._project_layout, stale_hash.datasource_id)
    index_datasource_context.assert_called_once_with(context=context, force_index=True)
