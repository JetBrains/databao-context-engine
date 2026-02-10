import re

_TABLE_NAME_RE = re.compile(r"^embedding_[a-z0-9_]+$")


class TableNamePolicy:
    @staticmethod
    def build(*, embedder: str, model_id: str, dim: int) -> str:
        safe_model = model_id.replace(":", "_").replace("-", "_").replace(" ", "_").replace(".", "_").lower()
        return f"embedding_{embedder}__{safe_model}__{dim}"

    @staticmethod
    def validate_table_name(*, table_name: str):
        if not _TABLE_NAME_RE.fullmatch(table_name):
            raise ValueError(f"invalid table_name {table_name!r}; expected pattern {_TABLE_NAME_RE.pattern}")
