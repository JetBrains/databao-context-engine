import duckdb

from databao_context_engine.pluginlib.config import DuckDBSecret
from databao_context_engine.plugins.duckdb_tools import fetchall_dicts, generate_create_secret_sql


def test_duckdb_secret_sql():
    create_secret_sql = generate_create_secret_sql(
        "test_secret",
        DuckDBSecret(
            name="test_secret",
            type="s3",
            properties={
                "provider": "credential_chain",
                "profile": "MySandox-123",
                "chain": "sso",
                "validation": "none",
            },
        ),
    )
    with duckdb.connect() as conn:
        conn.sql(create_secret_sql)
        res = fetchall_dicts(conn.cursor(), "FROM duckdb_secrets();")
        assert len(res) == 1
        assert res[0]["name"] == "test_secret"
        assert res[0]["type"] == "s3"
        assert res[0]["provider"] == "credential_chain"
        assert not res[0]["persistent"]
