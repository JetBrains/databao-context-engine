import sqlite3
from pathlib import Path


def execute_sqlite_queries(db_file: Path, *queries: str) -> None:
    conn = sqlite3.connect(database=str(db_file))
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        with conn:
            for query in queries:
                conn.execute(query)
    finally:
        conn.close()


def create_sqlite_with_base_schema(sqlite_path: Path) -> None:
    execute_sqlite_queries(
        sqlite_path,
        """
        CREATE TABLE users (
            user_id   INTEGER NOT NULL,
            name      VARCHAR NOT NULL,
            email     VARCHAR NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,

            CONSTRAINT pk_users PRIMARY KEY (user_id),
            CONSTRAINT uq_users_email UNIQUE (email),
            CONSTRAINT chk_users_email CHECK (email LIKE '%@%')
        );
        """.strip(),
    )
