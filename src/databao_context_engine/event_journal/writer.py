import datetime
import json
from pathlib import Path
from uuid import UUID, uuid1

from databao_context_engine.system.properties import get_dce_path


def get_journal_file(dce_path: Path) -> Path:
    return dce_path / "event-journal" / "journal.txt"


def log_event(*, project_id: UUID, dce_version: str, event_type: str, event_id: UUID = uuid1(), **kwargs):
    current_timestamp = datetime.datetime.now().isoformat()
    journal_file = get_journal_file(get_dce_path())
    journal_file.parent.mkdir(parents=True, exist_ok=True)
    with journal_file.open("a+", encoding="utf-8") as journal_handle:
        event_json = json.dumps(
            {
                "id": str(event_id),
                "project_id": str(project_id),
                "dce_version": dce_version,
                "timestamp": current_timestamp,
                "type": event_type,
                **kwargs,
            }
        )
        journal_handle.write(event_json)
        journal_handle.write("\n")
