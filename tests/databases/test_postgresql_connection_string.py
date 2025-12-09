import pytest
from nemory.plugins.databases.postgresql_introspector import PostgresqlIntrospector, PostgresConnectionProperties
from psycopg import conninfo


@pytest.mark.parametrize(
    "connection_config, expected_params",
    [
        pytest.param(
            PostgresConnectionProperties(
                host="localhost",
                port=5432,
                database="test_db",
                user="test_user",
                password="secure_password",
            ),
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
            PostgresConnectionProperties(
                host="localhost",
                database="test_db",
                user="test_user",
                password="secure_password",
            ),
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
            PostgresConnectionProperties(
                host="localhost",
                password="secure_password",
            ),
            {
                "host": "localhost",
                "port": "5432",
                "password": "secure_password",
            },
            id="missing-database-and-user",
        ),
        pytest.param(
            PostgresConnectionProperties(
                host="localhost",
                database="test db",
                user="user  name",
                password="p@ss;word",
            ),
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
            PostgresConnectionProperties(
                host="localhost",
                database="test'db",
                user="user''name",
                password="p@ss;word",
            ),
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
            PostgresConnectionProperties(
                host="localhost",
                port=1234,
                database=r"test\db",
                user=r"user\\name",
                password="p@ss;word",
            ),
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
            PostgresConnectionProperties(
                host="localhost",
                database="te  st'db",
                user="us''er'na:me",
                password=r"p@ss;wo\rd",
            ),
            {
                "host": "localhost",
                "port": "5432",
                "dbname": "te  st'db",
                "user": "us''er'na:me",
                "password": r"p@ss;wo\rd",
            },
            id="parameters-with-mixed-escaping",
        ),
        pytest.param(
            PostgresConnectionProperties(
                host="localhost",
                database="te  st'db",
                user="us''er'na:me",
                password=r"p@ss;wo\rd",
                additional_properties={
                    "connect_timeout": 10,
                    "application_name": "test",
                },
            ),
            {
                "host": "localhost",
                "port": "5432",
                "dbname": "te  st'db",
                "user": "us''er'na:me",
                "password": r"p@ss;wo\rd",
                "connect_timeout": "10",
                "application_name": "test",
            },
            id="parameters-with-additional-properties",
        ),
    ],
)
def test_create_connection_string(connection_config, expected_params):
    inspector = PostgresqlIntrospector()
    connection_string = inspector._create_connection_string_for_config(connection_config)
    parsed = conninfo.conninfo_to_dict(connection_string)
    assert parsed == expected_params
