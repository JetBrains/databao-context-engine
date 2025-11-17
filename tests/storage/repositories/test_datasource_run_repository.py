import pytest

from nemory.storage.models import DatasourceRunDTO
from nemory.storage.exceptions.exceptions import IntegrityError
from tests.utils.factories import make_run


def test_create_and_get(datasource_run_repo, run_repo):
    run = make_run(run_repo)
    created = datasource_run_repo.create(
        run_id=run.run_id,
        plugin="dbt",
        source_id="models/orders.sql",
        storage_directory="/some/path",
    )

    assert isinstance(created, DatasourceRunDTO)
    assert created.datasource_run_id > 0
    assert created.run_id == run.run_id
    assert created.plugin == "dbt"
    assert created.source_id == "models/orders.sql"
    assert created.storage_directory == "/some/path"

    fetched = datasource_run_repo.get(created.datasource_run_id)
    assert fetched == created


def test_get_missing_returns_none(datasource_run_repo):
    assert datasource_run_repo.get(999_999) is None


def test_create_with_missing_fk_raises(datasource_run_repo):
    with pytest.raises(IntegrityError):
        datasource_run_repo.create(
            run_id=9999,
            plugin="dbt",
            source_id="models/orders.sql",
            storage_directory="/path",
        )


def test_update_fields(datasource_run_repo, run_repo):
    run = make_run(run_repo)
    datasource_run = datasource_run_repo.create(
        run_id=run.run_id, plugin="dbt", source_id="path/a", storage_directory="/path"
    )

    updated = datasource_run_repo.update(
        datasource_run.datasource_run_id,
        plugin="snowflake",
        source_id="db.schema.table",
        storage_directory="/another/path",
    )
    assert updated is not None
    assert updated.datasource_run_id == datasource_run.datasource_run_id
    assert updated.run_id == run.run_id
    assert updated.plugin == "snowflake"
    assert updated.source_id == "db.schema.table"
    assert updated.storage_directory == "/another/path"

    assert updated.created_at == datasource_run.created_at


def test_update_missing_returns_none(datasource_run_repo):
    assert datasource_run_repo.update(424242, plugin="x") is None


def test_delete(datasource_run_repo, run_repo):
    run = make_run(run_repo)
    datasource_run = datasource_run_repo.create(run_id=run.run_id, plugin="p", source_id="s", storage_directory="s")

    deleted = datasource_run_repo.delete(datasource_run.datasource_run_id)
    assert deleted == 1
    assert datasource_run_repo.get(datasource_run.datasource_run_id) is None


def test_delete_missing_returns_zero(datasource_run_repo):
    assert datasource_run_repo.delete(424242) == 0


def test_list(datasource_run_repo, run_repo):
    run = make_run(run_repo)

    e1 = datasource_run_repo.create(run_id=run.run_id, plugin="p1", source_id="s1", storage_directory="s1")
    e2 = datasource_run_repo.create(run_id=run.run_id, plugin="p2", source_id="s2", storage_directory="s2")
    e3 = datasource_run_repo.create(run_id=run.run_id, plugin="p3", source_id="s3", storage_directory="s3")

    rows = datasource_run_repo.list()
    assert [e.datasource_run_id for e in rows] == [e3.datasource_run_id, e2.datasource_run_id, e1.datasource_run_id]
