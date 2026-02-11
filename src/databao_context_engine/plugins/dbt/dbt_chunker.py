from dataclasses import dataclass

from databao_context_engine.pluginlib.build_plugin import EmbeddableChunk
from databao_context_engine.plugins.dbt.types import DbtColumn, DbtContext, DbtMetric, DbtModel, DbtSemanticModel


@dataclass
class DbtColumnChunkContent:
    database_name: str
    schema_name: str
    model_name: str
    column: DbtColumn


def build_dbt_chunks(context: DbtContext) -> list[EmbeddableChunk]:
    chunks = []

    for model in context.models:
        chunks.append(_create_model_chunk(model))

        for column in model.columns:
            chunks.append(_create_column_chunk(model, column))

    for semantic_model in context.semantic_layer.semantic_models:
        chunks.append(_create_semantic_model_chunk(semantic_model))

    for metric in context.semantic_layer.metrics:
        chunks.append(_create_metric_chunk(metric))

    return chunks


def _create_model_chunk(model: DbtModel) -> EmbeddableChunk:
    return EmbeddableChunk(embeddable_text=_build_model_chunk_text(model), content=model)


def _build_model_chunk_text(model: DbtModel) -> str:
    # TODO: Use description and potentially other infos?
    return f"Model {model.name} in database {model.database} and schema {model.schema}, with unique id {model.id}"


def _create_column_chunk(model: DbtModel, column: DbtColumn) -> EmbeddableChunk:
    return EmbeddableChunk(
        embeddable_text=_build_column_chunk_text(model, column),
        content=DbtColumnChunkContent(
            database_name=model.database, schema_name=model.schema, model_name=model.name, column=column
        ),
    )


def _build_column_chunk_text(model: DbtModel, column: DbtColumn) -> str:
    # TODO: Use description and potentially other infos?
    return f"Column {column.name} in model {model.id}"


def _create_semantic_model_chunk(semantic_model: DbtSemanticModel) -> EmbeddableChunk:
    return EmbeddableChunk(
        embeddable_text=_build_semantic_model_chunk_text(semantic_model),
        content=semantic_model,
    )


def _build_semantic_model_chunk_text(semantic_model: DbtSemanticModel) -> str:
    return f"Semantic model {semantic_model.name} with id {semantic_model.id}, referencing model {semantic_model.model}"


def _create_metric_chunk(metric: DbtMetric) -> EmbeddableChunk:
    return EmbeddableChunk(
        embeddable_text=_build_metric_chunk_text(metric),
        content=metric,
    )


def _build_metric_chunk_text(metric: DbtMetric) -> str:
    suffix = (
        f", depending on semantic model {metric.depends_on_semantic_model}" if metric.depends_on_semantic_model else ""
    )
    return f"Metric {metric.name} with id {metric.id}{suffix}"
