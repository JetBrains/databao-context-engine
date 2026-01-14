from datetime import datetime, timedelta
from pathlib import Path

from nemory.project.layout import get_output_dir, read_config_file
from nemory.project.runs import resolve_run_name_from_repo
from nemory.storage.repositories.run_repository import RunRepository


def test_get_latest_run_dir__with_multiple_run_dirs(project_path: Path, run_repo: RunRepository):
    output_path = get_output_dir(project_path)
    output_path.mkdir()

    project_id = str(read_config_file(project_path).project_id)

    most_recent_date = datetime.now()
    most_recent_run = run_repo.create(project_id=project_id, nemory_version="1.0", started_at=most_recent_date)
    for i in reversed(range(1, 5)):
        date_for_run_folder = most_recent_date - timedelta(days=i)
        run_repo.create(project_id=project_id, nemory_version="1.0", started_at=date_for_run_folder)

    result = resolve_run_name_from_repo(run_repository=run_repo, project_id=project_id, run_name=None)

    assert result == most_recent_run.run_name
