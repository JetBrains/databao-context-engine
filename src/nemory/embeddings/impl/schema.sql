CREATE TABLE IF NOT EXISTS embeddings (
    chunk_id    TEXT PRIMARY KEY,
    object_type TEXT NOT NULL,
    text        TEXT,
    vec         FLOAT[768]
);
CREATE INDEX IF NOT EXISTS emb_hnsw ON embeddings USING HNSW(vec) WITH (metric = 'cosine');

CREATE TABLE IF NOT EXISTS db_tables_chunks (
    chunk_id    TEXT REFERENCES embeddings(chunk_id),
    database_id TEXT,
    catalog     TEXT,
    schema      TEXT,
    table_name  TEXT,
    text        TEXT
);

CREATE TABLE IF NOT EXISTS db_columns_chunks (
    chunk_id    TEXT REFERENCES embeddings(chunk_id),
    database_id TEXT,
    catalog     TEXT,
    schema      TEXT,
    table_name  TEXT,
    column_name TEXT,
    text        TEXT
);

CREATE TABLE IF NOT EXISTS dbt_models_chunks (
    chunk_id    TEXT REFERENCES embeddings(chunk_id),
    dbt_id      TEXT,
    project_id  TEXT,
    model_name  TEXT,
    description TEXT,
    text        TEXT
);

CREATE TABLE IF NOT EXISTS dbt_columns_chunks (
    chunk_id        TEXT REFERENCES embeddings(chunk_id),
    dbt_id          TEXT,
    project_id      TEXT,
    model_name      TEXT,
    column_name     TEXT,
    type            TEXT,
    description     TEXT,
    constraints     TEXT,
    text            TEXT
);

CREATE TABLE IF NOT EXISTS dbt_sources_chunks (
    chunk_id     TEXT REFERENCES embeddings(chunk_id),
    dbt_id       TEXT,
    project_id   TEXT,
    source_group TEXT,
    schema       TEXT,
    table_name   TEXT,
    description  TEXT,
    text         TEXT
);

CREATE TABLE IF NOT EXISTS dbt_semantic_chunks (
    chunk_id       TEXT REFERENCES embeddings(chunk_id),
    dbt_id         TEXT,
    project_id     TEXT,
    semantic_model TEXT,
    kind           TEXT,
    name           TEXT,
    type_or_agg    TEXT,
    expr           TEXT,
    model          TEXT,
    description    TEXT,
    text           TEXT
);

CREATE TABLE IF NOT EXISTS files_chunks (
    chunk_id     TEXT REFERENCES embeddings(chunk_id),
    path         TEXT,
    file_name    TEXT,
    chunk_index  INTEGER,
    text         TEXT
);