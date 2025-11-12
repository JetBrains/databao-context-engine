import pytest

from nemory.core.db.dtos import EntityDTO
from nemory.core.db.exceptions.exceptions import IntegrityError
from nemory_tests._utils.factories import make_run


def test_create_and_get(entity_repo, run_repo):
    run = make_run(run_repo)
    created = entity_repo.create(
        run_id=run.run_id,
        plugin="dbt",
        source_id="models/orders.sql",
        storage_directory="/some/path",
    )

    assert isinstance(created, EntityDTO)
    assert created.entity_id > 0
    assert created.run_id == run.run_id
    assert created.plugin == "dbt"
    assert created.source_id == "models/orders.sql"
    assert created.storage_directory == "/some/path"

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
            storage_directory="/path",
        )


def test_update_fields(entity_repo, run_repo):
    run = make_run(run_repo)
    ent = entity_repo.create(run_id=run.run_id, plugin="dbt", source_id="path/a", storage_directory="/path")

    updated = entity_repo.update(
        ent.entity_id,
        plugin="snowflake",
        source_id="db.schema.table",
        storage_directory="/another/path",
    )
    assert updated is not None
    assert updated.entity_id == ent.entity_id
    assert updated.run_id == run.run_id
    assert updated.plugin == "snowflake"
    assert updated.source_id == "db.schema.table"
    assert updated.storage_directory == "/another/path"

    assert updated.created_at == ent.created_at


def test_update_missing_returns_none(entity_repo):
    assert entity_repo.update(424242, plugin="x") is None


def test_delete(entity_repo, run_repo):
    run = make_run(run_repo)
    ent = entity_repo.create(run_id=run.run_id, plugin="p", source_id="s", storage_directory="s")

    deleted = entity_repo.delete(ent.entity_id)
    assert deleted == 1
    assert entity_repo.get(ent.entity_id) is None


def test_delete_missing_returns_zero(entity_repo):
    assert entity_repo.delete(424242) == 0


def test_list(entity_repo, run_repo):
    run = make_run(run_repo)

    e1 = entity_repo.create(run_id=run.run_id, plugin="p1", source_id="s1", storage_directory="s1")
    e2 = entity_repo.create(run_id=run.run_id, plugin="p2", source_id="s2", storage_directory="s2")
    e3 = entity_repo.create(run_id=run.run_id, plugin="p3", source_id="s3", storage_directory="s3")

    rows = entity_repo.list()
    assert [e.entity_id for e in rows] == [e3.entity_id, e2.entity_id, e1.entity_id]
