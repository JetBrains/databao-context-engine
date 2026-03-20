def before_migration(conn):
    conn.execute(
        """
        CREATE TABLE hook_source (
            id BIGINT PRIMARY KEY
        )
        """
    )
    conn.execute("INSERT INTO hook_source VALUES (4)")


def after_migration(conn):
    conn.execute("DROP TABLE hook_source")
    conn.execute(
        """
        CREATE TABLE hook_marker (
            id BIGINT PRIMARY KEY
        )
        """
    )
