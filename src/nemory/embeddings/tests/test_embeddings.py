from nemory.embeddings.impl.storage import load_embeddings_schema_sql, load_vss_extension_sql


def test_embeddings_schema_loaded():
    assert load_embeddings_schema_sql()


def test_vss_extension_loaded():
    assert load_vss_extension_sql()
