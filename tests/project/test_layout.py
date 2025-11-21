from datetime import datetime, timedelta
from pathlib import Path

import pytest

from nemory.project.layout import get_latest_run_name, get_output_dir
from nemory.storage.repositories.run_repository import RunRepository


def test_get_latest_run_dir__with_no_output_dir(project_path: Path):
    with pytest.raises(ValueError):
        get_latest_run_name(project_path)


def test_get_latest_run_dir__with_no_run_dirs(project_path: Path):
    get_output_dir(project_path).mkdir()

    with pytest.raises(ValueError):
        get_latest_run_name(project_path)


def test_get_latest_run_dir__with_no_run_dirs_but_other_dirs(project_path: Path):
    output_path = get_output_dir(project_path)
    output_path.mkdir()
    output_path.joinpath("other_dir").mkdir()

    with pytest.raises(ValueError):
        get_latest_run_name(project_path)


def test_get_latest_run_dir__with_multiple_run_dirs(project_path: Path):
    output_path = get_output_dir(project_path)
    output_path.mkdir()

    most_recent_date = datetime.now()
    for i in reversed(range(5)):
        date_for_run_folder = most_recent_date - timedelta(days=i)
        output_path.joinpath(RunRepository.generate_run_dir_name(date_for_run_folder)).mkdir()

    result = get_latest_run_name(project_path)

    assert result == RunRepository.generate_run_dir_name(most_recent_date)
