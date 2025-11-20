import datetime
import json
import os
from pathlib import Path
from uuid import UUID, uuid1


def get_nemory_path() -> Path:
    nemory_path = os.getenv("NEMORY_PATH") or "~/.nemory"
    return Path(nemory_path).expanduser().resolve()


def get_journal_file(nemory_path: Path) -> Path:
    return nemory_path / "event-journal" / "journal.txt"


def log_event(*, project_id: UUID, nemory_version: str, event_type: str, event_id: UUID = uuid1(), **kwargs):
    current_timestamp = datetime.datetime.now().isoformat()
    journal_file = get_journal_file(get_nemory_path())
    journal_file.parent.mkdir(parents=True, exist_ok=True)
    with journal_file.open("a+", encoding="utf-8") as journal_handle:
        event_json = json.dumps(
            {
                "id": str(event_id),
                "project_id": str(project_id),
                "nemory_version": nemory_version,
                "timestamp": current_timestamp,
                "type": event_type,
                **kwargs,
            }
        )
        journal_handle.write(event_json)
        journal_handle.write("\n")
