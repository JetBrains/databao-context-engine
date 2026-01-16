from datetime import datetime


class RunNamePolicy:
    _RUN_DIR_PREFIX = "run-"

    def build(self, *, run_started_at: datetime):
        return f"{RunNamePolicy._RUN_DIR_PREFIX}{run_started_at.isoformat(timespec='seconds')}"
