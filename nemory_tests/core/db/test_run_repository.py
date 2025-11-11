from datetime import timedelta

from nemory.core.db.dtos import RunStatus, RunDTO


def test_create_and_get(run_repo):
    created = run_repo.create(status=RunStatus.RUNNING, nemory_version="0.1.0")

    assert isinstance(created, RunDTO)
    assert created.run_id > 0
    assert created.status == RunStatus.RUNNING
    assert created.ended_at is None
    assert created.nemory_version == "0.1.0"

    fetched = run_repo.get(run_id=created.run_id)
    assert created == fetched


def test_get_missing_returns_none(run_repo):
    assert run_repo.get(999999) is None


def test_update_multiple_fields(run_repo):
    run = run_repo.create(status=RunStatus.RUNNING, nemory_version="0.1.0")
    end = run.started_at + timedelta(minutes=5)

    updated = run_repo.update(
        run.run_id,
        status=RunStatus.FAILED,
        ended_at=end,
        nemory_version="0.2.0",
    )
    assert updated is not None
    assert updated.status is RunStatus.FAILED
    assert updated.ended_at == end
    assert updated.nemory_version == "0.2.0"
    assert updated.started_at == run.started_at


def test_update_status_only(run_repo):
    run = run_repo.create(status=RunStatus.RUNNING)

    updated = run_repo.update(
        run.run_id,
        status=RunStatus.SUCCEEDED,
    )

    assert updated.status == RunStatus.SUCCEEDED


def test_update_missing_returns_none(run_repo):
    assert run_repo.update(999999, status=RunStatus.FAILED) is None


def test_delete(run_repo):
    run = run_repo.create(status=RunStatus.RUNNING)

    deleted = run_repo.delete(run.run_id)

    assert deleted == 1
    assert run_repo.get(run.run_id) is None


def test_list(run_repo):
    r1 = run_repo.create(status=RunStatus.RUNNING)
    r2 = run_repo.create(status=RunStatus.SUCCEEDED)
    r3 = run_repo.create(status=RunStatus.FAILED)

    rows = run_repo.list()
    assert [r.run_id for r in rows] == [r3.run_id, r2.run_id, r1.run_id]
