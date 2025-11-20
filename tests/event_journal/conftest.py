from pathlib import Path

import pytest

from nemory.event_journal import writer


@pytest.fixture()
def nemory_path(mocker, tmp_path: Path):
    mocker.patch.object(writer, "get_nemory_path", return_value=tmp_path)
    yield tmp_path
