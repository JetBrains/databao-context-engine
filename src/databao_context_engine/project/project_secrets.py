from __future__ import annotations

import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from databao_context_engine.project.layout import (
    SECRETS_FILE_NAME,
    ProjectLayout,
)
from databao_context_engine.serialization.yaml import to_yaml_string

_SECRET_REF_PATTERN = re.compile(r"^\$\{secret:([A-Za-z0-9._-]+)\}$")


def make_secret_ref(secret_key: str) -> str:
    return f"${{secret:{secret_key}}}"


def parse_secret_ref(value: str) -> str | None:
    match = _SECRET_REF_PATTERN.fullmatch(value.strip())
    return match.group(1) if match is not None else None


def load_project_secrets(project_layout: ProjectLayout) -> dict[str, Any]:
    return load_secrets_file(project_layout.secrets_file)


def load_secrets_file(file_path: Path) -> dict[str, Any]:
    """Load secrets from a project secrets file.

    The file must contain a top level yaml mapping of secret_key -> secret_value.

    Returns:
         An empty dict if the file does not exist.

    Raises:
         ValueError: if the file content is not a mapping.
    """
    if not file_path.is_file():
        return {}

    raw_content = yaml.safe_load(file_path.read_text())
    if raw_content is None:
        return {}

    if not isinstance(raw_content, Mapping):
        raise ValueError(f"Secrets file must contain a mapping: {file_path}")

    return dict(raw_content)


def merge_and_store_project_secrets(project_layout: ProjectLayout, secrets: Mapping[str, Any]) -> None:
    """Merge the given secrets into the project's secrets file and write the result."""
    if len(secrets) == 0:
        return

    merged_secrets = load_project_secrets(project_layout)
    merged_secrets.update(secrets)

    project_layout.secrets_file.write_text(to_yaml_string(merged_secrets))


def resolve_project_secret_references(project_layout: ProjectLayout, value: Any) -> Any:
    return resolve_secret_references(value=value, secrets=load_project_secrets(project_layout))


def resolve_secret_references(value: Any, secrets: Mapping[str, Any]) -> Any:
    """Recursively replace `${secret:...}` references with values from the secrets mapping."""
    if isinstance(value, Mapping):
        return {k: resolve_secret_references(v, secrets) for k, v in value.items()}

    if isinstance(value, list):
        return [resolve_secret_references(v, secrets) for v in value]

    if isinstance(value, str):
        secret_key = parse_secret_ref(value)
        if secret_key is None:
            return value

        if secret_key not in secrets:
            raise ValueError(f"Error in config. The secret '{secret_key}' is missing from {SECRETS_FILE_NAME}")

        return secrets[secret_key]

    return value
