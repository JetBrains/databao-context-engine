from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import AbstractContextManager
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class BaseConnector(Generic[T], ABC):
    @abstractmethod
    def connect(self, file_config: T, *, catalog: str | None = None) -> AbstractContextManager[Any]:
        """Connect to the database.

        If the `catalog` argument is provided, the connection is "scoped" to that catalog.
        """
        raise NotImplementedError

    @abstractmethod
    def execute(self, connection, sql: str, params) -> list[dict]:
        raise NotImplementedError

    def check_connection(self, file_config: T) -> None:
        with self.connect(file_config) as connection:
            self.execute(connection, self._connection_check_sql_query(), None)

    def _connection_check_sql_query(self) -> str:
        return "SELECT 1 as test"
