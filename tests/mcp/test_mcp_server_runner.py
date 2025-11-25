from datetime import datetime, timedelta
from pathlib import Path

from nemory.mcp.mcp_runner import _get_latest_run_name
from nemory.project.layout import get_output_dir, read_config_file
from nemory.storage.repositories.run_repository import RunRepository


def test_get_latest_run_dir__with_multiple_run_dirs(db_path, project_path: Path, run_repo: RunRepository):
    output_path = get_output_dir(project_path)
    output_path.mkdir()

    project_id = read_config_file(project_path).project_id

    most_recent_date = datetime.now()
    most_recent_run = run_repo.create(project_id=str(project_id), nemory_version="1.0", started_at=most_recent_date)
    for i in reversed(range(1, 5)):
        date_for_run_folder = most_recent_date - timedelta(days=i)
        run_repo.create(project_id=str(project_id), nemory_version="1.0", started_at=date_for_run_folder)

    result = _get_latest_run_name(project_path, db_path)

    assert result == most_recent_run.run_name
