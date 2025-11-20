from pathlib import Path

import pytest


@pytest.fixture()
def nemory_path(mocker, tmp_path: Path):
    mocker.patch("nemory.system.properties._nemory_path", new=tmp_path)
    yield tmp_path
