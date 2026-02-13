import pytest

from databao_context_engine.datasources.sql_read_only import SqlQueryAccessType, classify_sql, is_read_only_sql


class SqlTestCase:
    def __init__(self, sql: str, expected_class: SqlQueryAccessType, description: str):
        self.sql = sql
        self.expected_class = expected_class
        self.description = description


read_only_tests = [
    SqlTestCase("SELECT * FROM users", SqlQueryAccessType.READ_ONLY, "Simple SELECT"),
    SqlTestCase("  \nSELECT id FROM accounts", SqlQueryAccessType.READ_ONLY, "SELECT with whitespace"),
    SqlTestCase("WITH cte AS (SELECT 1) SELECT * FROM cte", SqlQueryAccessType.READ_ONLY, "CTE with SELECT"),
    SqlTestCase("EXPLAIN SELECT * FROM users", SqlQueryAccessType.READ_ONLY, "EXPLAIN statement"),
    SqlTestCase("SHOW TABLES", SqlQueryAccessType.READ_ONLY, "SHOW statement"),
    SqlTestCase("DESCRIBE users", SqlQueryAccessType.READ_ONLY, "DESCRIBE statement"),
    SqlTestCase(
        "SELECT 'DROP TABLE users';", SqlQueryAccessType.READ_ONLY, "SELECT with forbidden word in string literal"
    ),
    SqlTestCase(
        "-- DELETE FROM users\nSELECT * FROM users",
        SqlQueryAccessType.READ_ONLY,
        "SELECT after comment with forbidden keyword",
    ),
    SqlTestCase("SELECT * * FROM a;", SqlQueryAccessType.READ_ONLY, "Invalid SQL syntax"),
]

write_tests = [
    SqlTestCase("INSERT INTO users (id) VALUES (1)", SqlQueryAccessType.WRITE, "INSERT DML"),
    SqlTestCase("UPDATE users SET name='x'", SqlQueryAccessType.WRITE, "UPDATE DML"),
    SqlTestCase("DELETE FROM users WHERE id=1", SqlQueryAccessType.WRITE, "DELETE DML"),
    SqlTestCase("CREATE TABLE test(id INT)", SqlQueryAccessType.WRITE, "CREATE DDL"),
    SqlTestCase("DROP TABLE test", SqlQueryAccessType.WRITE, "DROP DDL"),
    SqlTestCase("ALTER TABLE users ADD COLUMN age INT", SqlQueryAccessType.WRITE, "ALTER DDL"),
    SqlTestCase("SELECT * INTO new_table FROM users", SqlQueryAccessType.WRITE, "SELECT ... INTO forbidden write"),
    SqlTestCase("SELECT * FROM users; DELETE FROM users", SqlQueryAccessType.WRITE, "Multiple statements"),
    SqlTestCase(
        "WITH cte AS (INSERT INTO t VALUES(1)) SELECT * FROM cte", SqlQueryAccessType.WRITE, "CTE with write inside"
    ),
    SqlTestCase("CALL do_something()", SqlQueryAccessType.WRITE, "CALL procedure"),
    SqlTestCase("GRANT SELECT ON users TO role", SqlQueryAccessType.WRITE, "Privilege statement"),
    SqlTestCase("BEGIN; SELECT 1;", SqlQueryAccessType.WRITE, "Transaction BEGIN makes it write"),
    SqlTestCase(
        "PRAGMA table_info(users)", SqlQueryAccessType.WRITE, "PRAGMA is forbidden, though this one is readonly"
    ),
    SqlTestCase("SELECT * FROM users FOR UPDATE", SqlQueryAccessType.WRITE, "SELECT FOR UPDATE"),
    SqlTestCase("SELECT * FROM users LOCK IN SHARE MODE", SqlQueryAccessType.WRITE, "SELECT LOCK IN SHARE MODE"),
    SqlTestCase("EXPLAIN ANALYZE DELETE FROM users;", SqlQueryAccessType.WRITE, "EXPLAIN ANALYZE with forbidden query"),
]

unknown_tests = [
    SqlTestCase("", SqlQueryAccessType.UNKNOWN, "Empty string"),
    SqlTestCase("   ", SqlQueryAccessType.UNKNOWN, "Whitespace only"),
    SqlTestCase(";", SqlQueryAccessType.UNKNOWN, "Semicolon only"),
    SqlTestCase("-- comment only", SqlQueryAccessType.UNKNOWN, "Comment only"),
]


@pytest.mark.parametrize("case", read_only_tests)
def test_read_only_classification(case: SqlTestCase):
    decision = classify_sql(case.sql)
    assert decision.classification == case.expected_class, f"Failed: {case.description} sql:{case.sql}"


@pytest.mark.parametrize("case", write_tests)
def test_write_classification(case: SqlTestCase):
    decision = classify_sql(case.sql)
    assert decision.classification == case.expected_class, f"Failed: {case.description} sql:{case.sql}"


@pytest.mark.parametrize("case", unknown_tests)
def test_unknown_classification(case: SqlTestCase):
    decision = classify_sql(case.sql)
    assert decision.classification == case.expected_class, f"Failed: {case.description} sql:{case.sql}"


@pytest.mark.parametrize("case", read_only_tests + write_tests + unknown_tests)
def test_is_read_only_sql(case: SqlTestCase):
    result = is_read_only_sql(case.sql)
    expected = case.expected_class == SqlQueryAccessType.READ_ONLY
    assert result == expected, f"Boolean check failed: {case.description}"
