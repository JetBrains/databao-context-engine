from datetime import datetime, timedelta

from databao_context_engine.project.layout import ProjectLayout, get_output_dir
from databao_context_engine.project.runs import resolve_run_name_from_repo
from databao_context_engine.storage.repositories.run_repository import RunRepository


def test_get_latest_run_dir__with_multiple_run_dirs(project_layout: ProjectLayout, run_repo: RunRepository):
    output_path = get_output_dir(project_layout.project_dir)
    output_path.mkdir()

    project_id = str(project_layout.read_config_file().project_id)

    most_recent_date = datetime.now()
    most_recent_run = run_repo.create(project_id=project_id, dce_version="1.0", started_at=most_recent_date)
    for i in reversed(range(1, 5)):
        date_for_run_folder = most_recent_date - timedelta(days=i)
        run_repo.create(project_id=project_id, dce_version="1.0", started_at=date_for_run_folder)

    result = resolve_run_name_from_repo(run_repository=run_repo, project_id=project_id, run_name=None)

    assert result == most_recent_run.run_name
