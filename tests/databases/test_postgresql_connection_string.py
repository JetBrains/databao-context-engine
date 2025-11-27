import pytest
from nemory.plugins.databases.postgresql_introspector import PostgresqlIntrospector
from psycopg import conninfo


@pytest.mark.parametrize(
    "connection_config, expected_params",
    [
        pytest.param(
            {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "user": "test_user",
                "password": "secure_password",
            },
            {
                "host": "localhost",
                "port": "5432",
                "dbname": "test_db",
                "user": "test_user",
                "password": "secure_password",
            },
            id="complete-config",
        ),
        pytest.param(
            {
                "host": "localhost",
                "database": "test_db",
                "user": "test_user",
                "password": "secure_password",
            },
            {
                "host": "localhost",
                "port": "5432",
                "dbname": "test_db",
                "user": "test_user",
                "password": "secure_password",
            },
            id="missing-port",
        ),
        pytest.param(
            {
                "host": "localhost",
                "password": "secure_password",
            },
            {
                "host": "localhost",
                "port": "5432",
                "password": "secure_password",
            },
            id="missing-database-and-user",
        ),
        pytest.param(
            {
                "host": "localhost",
                "database": "test db",
                "user": "user  name",
                "password": "p@ss;word",
            },
            {
                "host": "localhost",
                "port": "5432",
                "dbname": "test db",
                "user": "user  name",
                "password": "p@ss;word",
            },
            id="parameters-with-spaces",
        ),
        pytest.param(
            {
                "host": "localhost",
                "database": "test'db",
                "user": "user''name",
                "password": "p@ss;word",
            },
            {
                "host": "localhost",
                "port": "5432",
                "dbname": "test'db",
                "user": "user''name",
                "password": "p@ss;word",
            },
            id="parameters-with-quotes",
        ),
        pytest.param(
            {
                "host": "localhost",
                "port": "1234",
                "database": r"test\db",
                "user": r"user\\name",
                "password": "p@ss;word",
            },
            {
                "host": "localhost",
                "port": "1234",
                "dbname": r"test\db",
                "user": r"user\\name",
                "password": "p@ss;word",
            },
            id="parameters-with-backslashes",
        ),
        pytest.param(
            {
                "host": "localhost",
                "database": "te  st'db",
                "user": "us''er'na:me",
                "password": r"p@ss;wo\rd",
            },
            {
                "host": "localhost",
                "port": "5432",
                "dbname": "te  st'db",
                "user": "us''er'na:me",
                "password": r"p@ss;wo\rd",
            },
            id="parameters-with-mixed-escaping",
        ),
    ],
)
def test_create_connection_string(connection_config, expected_params):
    inspector = PostgresqlIntrospector()
    connection_string = inspector._create_connection_string_for_config(connection_config)
    parsed = conninfo.conninfo_to_dict(connection_string)
    assert parsed == expected_params
