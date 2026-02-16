import re
from collections import defaultdict
from pathlib import Path
from typing import Any

from databao_context_engine.plugins.dbt.types import (
    DbtAcceptedValuesConstraint,
    DbtColumn,
    DbtConfigFile,
    DbtConstraint,
    DbtContext,
    DbtMaterialization,
    DbtMetric,
    DbtModel,
    DbtRelationshipConstraint,
    DbtSemanticDimension,
    DbtSemanticEntity,
    DbtSemanticLayer,
    DbtSemanticMeasure,
    DbtSemanticModel,
    DbtSimpleConstraint,
)
from databao_context_engine.plugins.dbt.types_artifacts import (
    DbtArtifacts,
    DbtCatalog,
    DbtCatalogColumn,
    DbtCatalogNode,
    DbtManifest,
    DbtManifestColumn,
    DbtManifestMetric,
    DbtManifestModel,
    DbtManifestSemanticModel,
    DbtManifestTest,
)


def check_connection(config_file: DbtConfigFile) -> None:
    _read_dbt_artifacts(config_file.dbt_target_folder_path.expanduser())


def extract_context(config_file: DbtConfigFile) -> DbtContext:
    artifacts = _read_dbt_artifacts(config_file.dbt_target_folder_path.expanduser())

    return _extract_context_from_artifacts(artifacts)


def _read_dbt_artifacts(dbt_target_folder_path: Path) -> DbtArtifacts:
    if not dbt_target_folder_path.is_dir():
        raise ValueError(f'Invalid "dbt_target_folder_path": not a directory ({dbt_target_folder_path})')

    # TODO: Check the manifest schema version?
    manifest_file = dbt_target_folder_path.joinpath("manifest.json")
    if not manifest_file.is_file():
        raise ValueError(f'Invalid "dbt_target_folder_path": missing manifest.json file ({manifest_file})')

    manifest = DbtManifest.model_validate_json(manifest_file.read_text())

    catalog_file = dbt_target_folder_path.joinpath("catalog.json")
    catalog = DbtCatalog.model_validate_json(catalog_file.read_text()) if catalog_file.is_file() else None

    return DbtArtifacts(manifest=manifest, catalog=catalog)


def _extract_context_from_artifacts(artifacts: DbtArtifacts) -> DbtContext:
    manifest_models = [
        manifest_node
        for manifest_node in artifacts.manifest.nodes.values()
        if isinstance(manifest_node, DbtManifestModel)
    ]

    manifest_tests_by_model_and_column = _get_manifest_tests(artifacts.manifest)

    catalog_nodes = artifacts.catalog.nodes if artifacts.catalog else {}

    # TODO: Extract the stages? Or at least the "highest-level" models (= marts?)
    # TODO: Organize the models by schemas? Or by stages?
    return DbtContext(
        models=[
            _manifest_model_to_dbt_model(
                manifest_model,
                catalog_nodes.get(manifest_model.unique_id, None),
                manifest_tests_by_model_and_column.get(manifest_model.unique_id, {}),
            )
            for manifest_model in manifest_models
        ],
        semantic_layer=DbtSemanticLayer(
            semantic_models=[
                _manifest_semantic_model_to_dbt_semantic_model(manifest_semantic_model)
                for manifest_semantic_model in artifacts.manifest.semantic_models.values()
            ],
            metrics=[
                _manifest_metric_to_dbt_metric(manifest_metric)
                for manifest_metric in artifacts.manifest.metrics.values()
            ],
        ),
    )


def _get_manifest_tests(manifest: DbtManifest) -> dict[Any, dict[Any, list]]:
    """Extract all tests nodes in the manifest and groups them by model and column."""
    manifest_tests_by_model_and_column: dict[str, dict[str, list[DbtManifestTest]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for manifest_node in manifest.nodes.values():
        if isinstance(manifest_node, DbtManifestTest) and manifest_node.attached_node and manifest_node.column_name:
            manifest_tests_by_model_and_column[manifest_node.attached_node][manifest_node.column_name].append(
                manifest_node
            )
    return manifest_tests_by_model_and_column


def _manifest_model_to_dbt_model(
    manifest_model: DbtManifestModel,
    catalog_node: DbtCatalogNode | None,
    test_nodes_by_column_name: dict[str, list[DbtManifestTest]],
) -> DbtModel:
    catalog_columns = catalog_node.columns if catalog_node else {}

    return DbtModel(
        id=manifest_model.unique_id,
        name=manifest_model.name,
        database=manifest_model.database,
        schema=manifest_model.schema_,
        description=manifest_model.description,
        columns=[
            _manifest_column_to_dbt_column(
                manifest_column,
                catalog_columns.get(manifest_column.name),
                test_nodes_by_column_name.get(manifest_column.name, []),
            )
            for manifest_column in manifest_model.columns.values()
        ],
        materialization=_manifest_materialization_to_dbt_materializaton(
            manifest_model.config.materialized if manifest_model.config else None
        ),
        primary_key=manifest_model.primary_key,
        depends_on_nodes=manifest_model.depends_on.get("nodes", []) if manifest_model.depends_on else [],
    )


def _manifest_column_to_dbt_column(
    manifest_column: DbtManifestColumn, catalog_column: DbtCatalogColumn | None, test_nodes: list[DbtManifestTest]
) -> DbtColumn:
    constraints = _manifest_test_to_dbt_constraint(test_nodes)

    return DbtColumn(
        name=manifest_column.name,
        description=manifest_column.description,
        type=catalog_column.type if catalog_column else manifest_column.data_type,
        constraints=constraints,
    )


def _manifest_test_to_dbt_constraint(test_nodes: list[DbtManifestTest]) -> list[DbtConstraint]:
    result: list[DbtConstraint] = []

    for test_node in test_nodes:
        is_enforced = test_node.config.severity == "ERROR" if test_node.config else False

        if test_node.test_metadata is None:
            continue

        match test_node.test_metadata.name:
            case "not_null":
                result.append(
                    DbtSimpleConstraint(type="not_null", is_enforced=is_enforced, description=test_node.description)
                )
            case "unique":
                result.append(
                    DbtSimpleConstraint(type="unique", is_enforced=is_enforced, description=test_node.description)
                )
            case "accepted_values":
                if test_node.test_metadata.kwargs is None:
                    continue

                accepted_values = test_node.test_metadata.kwargs.get("values", None)
                if accepted_values is None:
                    continue
                result.append(
                    DbtAcceptedValuesConstraint(
                        type="accepted_values",
                        is_enforced=is_enforced,
                        description=test_node.description,
                        accepted_values=accepted_values,
                    )
                )
            case "relationships":
                if test_node.test_metadata.kwargs is None:
                    continue

                target_model = _extract_ref_model(test_node.test_metadata.kwargs.get("to", None))
                if target_model is None:
                    continue
                target_column = test_node.test_metadata.kwargs.get("field", None)
                if target_column is None:
                    continue
                result.append(
                    DbtRelationshipConstraint(
                        type="relationships",
                        is_enforced=is_enforced,
                        description=test_node.description,
                        target_model=target_model,
                        target_column=target_column,
                    )
                )
            case _:
                continue

    return result


def _manifest_semantic_model_to_dbt_semantic_model(
    manifest_semantic_model: DbtManifestSemanticModel,
) -> DbtSemanticModel:
    return DbtSemanticModel(
        id=manifest_semantic_model.unique_id,
        name=manifest_semantic_model.name,
        model=_extract_ref_model(manifest_semantic_model.model),
        description=manifest_semantic_model.description,
        entities=[
            DbtSemanticEntity(
                name=manifest_entity.name, type=manifest_entity.type, description=manifest_entity.description
            )
            for manifest_entity in manifest_semantic_model.entities
        ]
        if manifest_semantic_model.entities
        else [],
        measures=[
            DbtSemanticMeasure(
                name=manifest_measure.name, agg=manifest_measure.agg, description=manifest_measure.description
            )
            for manifest_measure in manifest_semantic_model.measures
        ]
        if manifest_semantic_model.measures
        else [],
        dimensions=[
            DbtSemanticDimension(
                name=manifest_dimension.name, type=manifest_dimension.type, description=manifest_dimension.description
            )
            for manifest_dimension in manifest_semantic_model.dimensions
        ]
        if manifest_semantic_model.dimensions
        else [],
    )


def _manifest_metric_to_dbt_metric(manifest_metric: DbtManifestMetric) -> DbtMetric:
    return DbtMetric(
        id=manifest_metric.unique_id,
        name=manifest_metric.name,
        description=manifest_metric.description,
        type=manifest_metric.type,
        label=manifest_metric.label,
        depends_on_nodes=manifest_metric.depends_on.get("nodes", []) if manifest_metric.depends_on else [],
    )


def _extract_ref_model(target_model_with_ref: str | None) -> str | None:
    if target_model_with_ref is None:
        return None

    match = re.fullmatch(r"ref\(['\"]([\w.]+)['\"]\)", target_model_with_ref)
    if match:
        return match.group(1)

    return None


def _manifest_materialization_to_dbt_materializaton(materialized: str | None) -> DbtMaterialization | None:
    if materialized is None:
        return None

    try:
        return DbtMaterialization(materialized)
    except ValueError:
        return None
