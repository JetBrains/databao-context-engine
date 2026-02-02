from pathlib import Path

from databao_context_engine.plugins.dbt.types import (
    DbtColumn,
    DbtConfigFile,
    DbtContext,
    DbtMaterialization,
    DbtModel,
)
from databao_context_engine.plugins.dbt.types_artifacts import (
    DbtArtifacts,
    DbtCatalog,
    DbtManifest,
    DbtManifestColumn,
    DbtManifestModel,
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
        manifest_model
        for manifest_model in artifacts.manifest.nodes.values()
        if isinstance(manifest_model, DbtManifestModel)
    ]

    # TODO: Add the catalog if available to read the actual column type
    # TODO: Extract the stages? Or at least the "highest-level" models (= marts?)
    # TODO: Extract the constraints
    # TODO: Organize the models by schemas? Or by stages?
    return DbtContext(
        models=[_manifest_model_to_dbt_model(manifest_model) for manifest_model in manifest_models],
    )


def _manifest_model_to_dbt_model(manifest_model: DbtManifestModel) -> DbtModel:
    return DbtModel(
        id=manifest_model.unique_id,
        name=manifest_model.name,
        database=manifest_model.database,
        schema=manifest_model.schema_,
        description=manifest_model.description,
        columns=[
            _manifest_column_to_dbt_column(manifest_column) for manifest_column in manifest_model.columns.values()
        ],
        materialization=_manifest_materialization_to_dbt_materializaton(
            manifest_model.config.materialized if manifest_model.config else None
        ),
        primary_key=manifest_model.primary_key,
        depends_on_nodes=manifest_model.depends_on.get("nodes", []) if manifest_model.depends_on else [],
    )


def _manifest_column_to_dbt_column(manifest_column: DbtManifestColumn) -> DbtColumn:
    return DbtColumn(
        name=manifest_column.name,
        description=manifest_column.description,
        type=manifest_column.data_type,
    )


def _manifest_materialization_to_dbt_materializaton(materialized: str | None) -> DbtMaterialization | None:
    if materialized is None:
        return None

    try:
        return DbtMaterialization(materialized)
    except ValueError:
        return None
