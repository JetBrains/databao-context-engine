from datetime import datetime, timedelta

from nemory.services.run_name_policy import RunNamePolicy
from nemory.storage.models import RunDTO


def test_create_and_get(run_repo):
    created = run_repo.create(project_id="project-id", nemory_version="0.1.0")

    assert isinstance(created, RunDTO)
    assert created.run_id > 0
    assert created.project_id == "project-id"
    assert created.started_at is not None
    assert created.run_name.startswith("run-")
    assert created.ended_at is None
    assert created.nemory_version == "0.1.0"

    fetched = run_repo.get(run_id=created.run_id)
    assert created == fetched


def test_create__with_started_at(run_repo):
    started_at = datetime.now() - timedelta(days=1)
    created = run_repo.create(project_id="project-id", nemory_version="0.1.0", started_at=started_at)

    assert isinstance(created, RunDTO)
    assert created.run_id > 0
    assert created.project_id == "project-id"
    assert created.started_at == started_at
    assert created.run_name == RunNamePolicy().build(run_started_at=started_at)
    assert created.ended_at is None
    assert created.nemory_version == "0.1.0"

    fetched = run_repo.get(run_id=created.run_id)
    assert created == fetched


def test_get_by_run_name(run_repo):
    nemory_version = "0.1.0"

    project_id_1 = "project-id-1"
    project_id_2 = "project-id-2"
    started_1 = datetime.now() - timedelta(days=1)
    started_2 = datetime.now() - timedelta(days=3)

    run_repo.create(project_id=project_id_1, nemory_version=nemory_version, started_at=started_1)
    run_project_1_started_2 = run_repo.create(
        project_id=project_id_1, nemory_version=nemory_version, started_at=started_2
    )
    run_repo.create(project_id=project_id_2, nemory_version=nemory_version, started_at=started_2)

    run_name_policy = RunNamePolicy()
    assert (
        run_repo.get_by_run_name(project_id=project_id_1, run_name=run_name_policy.build(run_started_at=started_2))
        == run_project_1_started_2
    )
    assert (
        run_repo.get_by_run_name(project_id=project_id_2, run_name=run_name_policy.build(run_started_at=started_1))
        is None
    )


def test_get_latest_run_for_project(run_repo):
    nemory_version = "0.1.0"

    project_id_1 = "project-id-1"
    project_id_2 = "project-id-2"
    project_id_3 = "project-id-3"

    most_recent_started_at = datetime.now()
    run_repo.create(project_id=project_id_2, nemory_version=nemory_version, started_at=most_recent_started_at)
    run_repo.create(
        project_id=project_id_1, nemory_version=nemory_version, started_at=most_recent_started_at - timedelta(days=1)
    )
    expected_latest_run = run_repo.create(
        project_id=project_id_1, nemory_version=nemory_version, started_at=most_recent_started_at
    )
    run_repo.create(
        project_id=project_id_1, nemory_version=nemory_version, started_at=most_recent_started_at - timedelta(days=5)
    )

    assert run_repo.get_latest_run_for_project(project_id_1) == expected_latest_run
    assert run_repo.get_latest_run_for_project(project_id_3) is None


def test_get_missing_returns_none(run_repo):
    assert run_repo.get(999999) is None


def test_update_multiple_fields(run_repo):
    run = run_repo.create(project_id="project-id", nemory_version="0.1.0")
    end = run.started_at + timedelta(minutes=5)

    updated = run_repo.update(
        run.run_id,
        project_id="project-id2",
        ended_at=end,
        nemory_version="0.2.0",
    )
    assert updated is not None
    assert updated.project_id == "project-id2"
    assert updated.ended_at == end
    assert updated.nemory_version == "0.2.0"
    assert updated.started_at == run.started_at


def test_update_project_id_only(run_repo):
    run = run_repo.create(project_id="project-id")

    updated = run_repo.update(
        run.run_id,
        project_id="project-id2",
    )

    assert updated.project_id == "project-id2"


def test_update_missing_returns_none(run_repo):
    assert run_repo.update(999999) is None


def test_delete(run_repo):
    run = run_repo.create(project_id="project-id")

    deleted = run_repo.delete(run.run_id)

    assert deleted == 1
    assert run_repo.get(run.run_id) is None


def test_list(run_repo):
    r1 = run_repo.create(project_id="project-id1")
    r2 = run_repo.create(project_id="project-id2")
    r3 = run_repo.create(project_id="project-id3")

    rows = run_repo.list()
    assert [r.run_id for r in rows] == [r3.run_id, r2.run_id, r1.run_id]


def test_get_latest_for_project_returns_none_when_no_runs(run_repo):
    latest = run_repo.get_latest_run_for_project(project_id="project-id")
    assert latest is None
