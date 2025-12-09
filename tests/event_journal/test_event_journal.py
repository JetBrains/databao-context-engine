import json
import uuid
from datetime import datetime
from importlib.metadata import version

import duckdb

from nemory.event_journal.writer import get_journal_file, log_event


def write_test_events():
    for i in range(10):
        log_event(
            project_id=uuid.uuid1(),
            nemory_version=version("nemory"),
            event_type="test_event",
            сustom_field=f"event-{i}",
        )


def test_reading_journal_with_duckdb(nemory_path):
    write_test_events()
    with duckdb.connect() as conn:
        json_events: list = [
            json.loads(e)
            for (e,) in conn.execute(
                f"SELECT json FROM read_json_objects('{get_journal_file(nemory_path)}')"
            ).fetchall()
        ]
        json_events.sort(key=lambda e: datetime.fromisoformat(e["timestamp"]))
        assert len(json_events) == 10
        assert json_events[0]["type"] == "test_event"
        assert json_events[0]["сustom_field"] == "event-0"
        assert json_events[9]["сustom_field"] == "event-9"
