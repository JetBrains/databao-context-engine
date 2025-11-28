import pytest

from nemory.plugins.databases.mssql_introspector import MSSQLIntrospector


@pytest.mark.parametrize(
    "connection_config, expected_str",
    [
        pytest.param(
            {
                "host": "localhost",
                "port": 1433,
                "database": "test_db",
                "user": "user123",
                "password": "S3cur3;Pwd",
            },
            "server={localhost,1433};database={test_db};uid={user123};pwd={S3cur3;Pwd}",
            id="complete-config-with-port",
        ),
        pytest.param(
            {
                "host": "10.0.0.5",
                "database": "test_db",
                "user": "my_user",
                "password": "pass word",
            },
            "server={10.0.0.5,1433};database={test_db};uid={my_user};pwd={pass word}",
            id="default-port",
        ),
        pytest.param(
            {
                "host": "localhost",
                "instanceName": "SQL2019",
                "database": "prod db",
                "user": "domain\\user",
                "password": "password",
            },
            r"server={localhost\SQL2019};database={prod db};uid={domain\user};pwd={password}",
            id="instance-name",
        ),
        pytest.param(
            {
                "host": "}local;host{",
                "port": 1234,
                "database": "prod db",
                "user": "domain\\user",
                "password": "p@ss{word}",
            },
            r"server={}}local;host{{,1234};database={prod db};uid={domain\user};pwd={p@ss{{word}}}",
            id="escape-special-characters",
        ),
        pytest.param(
            {
                "host": "db.example.com",
                "port": 1401,
                "database": "sales",
                "user": "report",
                "password": "123",
                "trust_server_certificate": True,
            },
            "server={db.example.com,1401};database={sales};uid={report};pwd={123};trust_server_certificate=yes",
            id="trust-server-certificate",
        ),
        pytest.param(
            {
                "host": "localhost",
                "port": 1401,
                "database": "sales",
                "user": "report",
            },
            "server={localhost,1401};database={sales};uid={report}",
            id="empty-password",
        ),
        pytest.param(
            {
                "host": "10.0.0.5",
                "database": "analytics",
                "user": "user_1",
                "password": "pw",
                "encrypt": "no",
            },
            "server={10.0.0.5,1433};database={analytics};uid={user_1};pwd={pw};encrypt=no",
            id="encrypt-parameter",
        ),
    ],
)
def test_create_mssql_connection_string_exact(connection_config, expected_str):
    inspector = MSSQLIntrospector()
    conn_str = inspector._create_connection_string_for_config(connection_config)
    assert conn_str == expected_str
