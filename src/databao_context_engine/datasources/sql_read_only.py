from dataclasses import dataclass
from enum import Enum

import sqlparse
from sqlparse import tokens as T
from sqlparse.sql import Statement

from databao_context_engine.datasources.datasource_discovery import logger


class SqlQueryAccessType(Enum):
    READ_ONLY = "read_only"
    WRITE = "write"
    UNKNOWN = "unknown"


@dataclass
class SqlReadOnlyDecision:
    classification: SqlQueryAccessType
    reason: str | None = None


_ALLOWED_STARTERS = {"SELECT", "WITH", "EXPLAIN", "SHOW", "DESCRIBE", "VALUES"}

_FORBIDDEN_KEYWORDS = {
    "INSERT",
    "UPDATE",
    "DELETE",
    "MERGE",
    "REPLACE",
    "UPSERT",
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "RENAME",
    "GRANT",
    "REVOKE",
    "BEGIN",
    "COMMIT",
    "ROLLBACK",
    "SAVEPOINT",
    "RELEASE",
    "SET",
    "USE",
    "COPY",
    "LOAD",
    "UNLOAD",
    "VACUUM",
    "ANALYZE",
    "OPTIMIZE",
    "REFRESH",
    "CALL",
    "EXEC",
    "EXECUTE",
    "INDEX",
    "SEQUENCE",
    "CONSTRAINT",
    "LOCK",
}


def classify_sql(sql: str) -> SqlReadOnlyDecision:
    if not sql or not sql.strip():
        return SqlReadOnlyDecision(SqlQueryAccessType.UNKNOWN, "Empty SQL")

    statements = [
        s
        for s in sqlparse.parse(sql)
        if any(tok.ttype not in (T.Whitespace, T.Newline, T.Comment) for tok in s.flatten())
    ]

    if len(statements) != 1:
        return SqlReadOnlyDecision(SqlQueryAccessType.WRITE, "Multiple SQL statements are not allowed")

    stmt: Statement = statements[0]
    first_token = stmt.token_first(skip_ws=True, skip_cm=True)
    if not first_token:
        return SqlReadOnlyDecision(SqlQueryAccessType.UNKNOWN, "No SQL keywords found")

    if first_token.ttype in (T.Punctuation,):
        return SqlReadOnlyDecision(SqlQueryAccessType.UNKNOWN, "Only punctuation, no SQL statement")

    if first_token.value not in _ALLOWED_STARTERS:
        return SqlReadOnlyDecision(SqlQueryAccessType.WRITE, f"Statement starts with disallowed keyword: {first_token}")

    for tok in stmt.flatten():
        if tok.ttype in (T.Whitespace, T.Newline, T.Comment, T.Punctuation):
            continue

        value = tok.value.upper()
        if tok.ttype == T.Keyword.DDL:
            return SqlReadOnlyDecision(SqlQueryAccessType.WRITE, f"DDL detected: {value}")

        if tok.ttype == T.Keyword.DML and value != "SELECT":
            return SqlReadOnlyDecision(SqlQueryAccessType.WRITE, f"DML detected: {value}")

        # Block SELECT ... INTO
        if value == "INTO":
            return SqlReadOnlyDecision(SqlQueryAccessType.WRITE, "SELECT ... INTO is not allowed")

        if tok.ttype in (T.Keyword, T.Keyword.Reserved) and value in _FORBIDDEN_KEYWORDS:
            return SqlReadOnlyDecision(SqlQueryAccessType.WRITE, f"Forbidden keyword: {value}")

    return SqlReadOnlyDecision(SqlQueryAccessType.READ_ONLY)


def is_read_only_sql(sql: str) -> bool:
    decision = classify_sql(sql)
    if decision.classification != SqlQueryAccessType.READ_ONLY:
        logger.warning("SQL is not read-only: %s", decision.reason)
    return decision.classification == SqlQueryAccessType.READ_ONLY
