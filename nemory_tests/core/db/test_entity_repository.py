import pytest

from nemory.core.db.dtos import RunStatus, EntityDTO
from nemory.core.db.exceptions.exceptions import IntegrityError


def _make_run(run_repo, version: str | None = None):
    return run_repo.create(status=RunStatus.RUNNING, nemory_version=version)


def test_create_and_get(entity_repo, run_repo):
    run = _make_run(run_repo)
    created = entity_repo.create(
        run_id=run.run_id,
        plugin="dbt",
        source_id="models/orders.sql",
        document="{}",
    )

    assert isinstance(created, EntityDTO)
    assert created.entity_id > 0
    assert created.run_id == run.run_id
    assert created.plugin == "dbt"
    assert created.source_id == "models/orders.sql"
    assert created.document == "{}"

    fetched = entity_repo.get(created.entity_id)
    assert fetched == created


def test_get_missing_returns_none(entity_repo):
    assert entity_repo.get(999_999) is None


def test_create_with_missing_fk_raises(entity_repo):
    with pytest.raises(IntegrityError):
        entity_repo.create(
            run_id=9999,
            plugin="dbt",
            source_id="models/orders.sql",
            document="{}",
        )


def test_update_fields(entity_repo, run_repo):
    run = _make_run(run_repo)
    ent = entity_repo.create(run_id=run.run_id, plugin="dbt", source_id="path/a", document="{}")

    updated = entity_repo.update(
        ent.entity_id,
        plugin="snowflake",
        source_id="db.schema.table",
        document='{"k": "v"}',
    )
    assert updated is not None
    assert updated.entity_id == ent.entity_id
    assert updated.run_id == run.run_id
    assert updated.plugin == "snowflake"
    assert updated.source_id == "db.schema.table"
    assert updated.document == '{"k": "v"}'

    assert updated.created_at == ent.created_at


def test_update_missing_returns_none(entity_repo):
    assert entity_repo.update(424242, plugin="x") is None


def test_delete(entity_repo, run_repo):
    run = _make_run(run_repo)
    ent = entity_repo.create(run_id=run.run_id, plugin="p", source_id="s", document="d")

    deleted = entity_repo.delete(ent.entity_id)
    assert deleted == 1
    assert entity_repo.get(ent.entity_id) is None


def test_delete_missing_returns_zero(entity_repo):
    assert entity_repo.delete(424242) == 0


def test_list(entity_repo, run_repo):
    run = _make_run(run_repo)

    e1 = entity_repo.create(run_id=run.run_id, plugin="p1", source_id="s1", document="d1")
    e2 = entity_repo.create(run_id=run.run_id, plugin="p2", source_id="s2", document="d2")
    e3 = entity_repo.create(run_id=run.run_id, plugin="p3", source_id="s3", document="d3")

    rows = entity_repo.list()
    assert [e.entity_id for e in rows] == [e3.entity_id, e2.entity_id, e1.entity_id]
