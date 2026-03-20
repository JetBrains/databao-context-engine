from datetime import datetime

import pytest

from databao_context_engine.storage.exceptions.exceptions import IntegrityError
from databao_context_engine.storage.models import DatasourceContextHashDTO


def test_insert_and_get_by_datasource_id_and_hash(datasource_context_hash_repo):
    hashed_at = datetime(2025, 1, 2, 3, 4, 5)

    created = datasource_context_hash_repo.insert(
        datasource_id="orders.yaml",
        hash_algorithm="sha256",
        hash_="abc123",
        hashed_at=hashed_at,
    )

    assert isinstance(created, DatasourceContextHashDTO)
    assert created == DatasourceContextHashDTO(
        datasource_context_hash_id=created.datasource_context_hash_id,
        datasource_id="orders.yaml",
        hash_algorithm="sha256",
        hash="abc123",
        hashed_at=created.hashed_at,
    )

    fetched = datasource_context_hash_repo.get_by_datasource_id_and_hash(
        datasource_id="orders.yaml",
        hash_algorithm="sha256",
        hash_="abc123",
    )

    assert fetched == created


def test_get_by_datasource_id_and_hash_missing_returns_none(datasource_context_hash_repo):
    assert (
        datasource_context_hash_repo.get_by_datasource_id_and_hash(
            datasource_id="missing.yaml",
            hash_algorithm="sha256",
            hash_="does-not-exist",
        )
        is None
    )


def test_list_returns_rows_in_descending_id_order(datasource_context_hash_repo):
    first = datasource_context_hash_repo.insert(
        datasource_id="first.yaml",
        hash_algorithm="sha256",
        hash_="hash-1",
        hashed_at=datetime(2025, 1, 1, 0, 0, 0),
    )
    second = datasource_context_hash_repo.insert(
        datasource_id="second.yaml",
        hash_algorithm="sha256",
        hash_="hash-2",
        hashed_at=datetime(2025, 1, 1, 0, 0, 1),
    )
    third = datasource_context_hash_repo.insert(
        datasource_id="third.yaml",
        hash_algorithm="sha256",
        hash_="hash-3",
        hashed_at=datetime(2025, 1, 1, 0, 0, 2),
    )

    assert datasource_context_hash_repo.list() == [third, second, first]


def test_delete_by_datasource_id_and_hash_removes_matching_row(datasource_context_hash_repo):
    kept = datasource_context_hash_repo.insert(
        datasource_id="customers.yaml",
        hash_algorithm="sha256",
        hash_="keep-me",
        hashed_at=datetime(2025, 1, 1, 0, 0, 0),
    )
    deleted = datasource_context_hash_repo.insert(
        datasource_id="customers.yaml",
        hash_algorithm="sha256",
        hash_="delete-me",
        hashed_at=datetime(2025, 1, 1, 0, 0, 1),
    )

    count = datasource_context_hash_repo.delete_by_datasource_id_and_hash(
        datasource_id="customers.yaml",
        hash_algorithm="sha256",
        hash_="delete-me",
    )

    assert count == 1
    assert (
        datasource_context_hash_repo.get_by_datasource_id_and_hash(
            datasource_id="customers.yaml",
            hash_algorithm="sha256",
            hash_="delete-me",
        )
        is None
    )
    assert (
        datasource_context_hash_repo.get_by_datasource_id_and_hash(
            datasource_id=kept.datasource_id,
            hash_algorithm=kept.hash_algorithm,
            hash_=kept.hash,
        )
        == kept
    )
    assert deleted not in datasource_context_hash_repo.list()


def test_delete_by_datasource_id_and_hash_missing_returns_zero(datasource_context_hash_repo):
    count = datasource_context_hash_repo.delete_by_datasource_id_and_hash(
        datasource_id="missing.yaml",
        hash_algorithm="sha256",
        hash_="missing-hash",
    )

    assert count == 0


def test_insert_duplicate_hash_raises_integrity_error(datasource_context_hash_repo):
    hashed_at = datetime(2025, 1, 2, 3, 4, 5)
    datasource_context_hash_repo.insert(
        datasource_id="orders.yaml",
        hash_algorithm="sha256",
        hash_="abc123",
        hashed_at=hashed_at,
    )

    with pytest.raises(IntegrityError):
        datasource_context_hash_repo.insert(
            datasource_id="orders.yaml",
            hash_algorithm="sha256",
            hash_="abc123",
            hashed_at=hashed_at,
        )
