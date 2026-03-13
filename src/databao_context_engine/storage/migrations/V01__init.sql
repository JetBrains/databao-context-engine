INSTALL vss;
LOAD vss;
SET hnsw_enable_experimental_persistence = true;

CREATE SEQUENCE IF NOT EXISTS datasource_context_hash_id_seq START 1;

CREATE TABLE IF NOT EXISTS datasource_context_hash (
    datasource_context_hash_id  BIGINT PRIMARY KEY DEFAULT nextval('datasource_context_hash_id_seq'),
    datasource_id               TEXT NOT NULL,
    hash_algorithm              TEXT NOT NULL,
    hash                        TEXT NOT NULL,
    hashed_at                   TIMESTAMP NOT NULL,
    UNIQUE (datasource_id, hash_algorithm, hash)
);

CREATE SEQUENCE IF NOT EXISTS chunk_id_seq START 1;

CREATE TABLE IF NOT EXISTS chunk (
    chunk_id                    BIGINT PRIMARY KEY DEFAULT nextval('chunk_id_seq'),
    full_type                   TEXT NOT NULL,
    datasource_id               TEXT NOT NULL,
    embeddable_text             TEXT NOT NULL,
    display_text                TEXT,
    created_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    keyword_index_text          TEXT,
    datasource_context_hash_id  BIGINT NOT NULL REFERENCES datasource_context_hash(datasource_context_hash_id)
);

CREATE TABLE IF NOT EXISTS embedding_model_registry (
    embedder    TEXT NOT NULL,
    model_id    TEXT NOT NULL,
    dim         INTEGER NOT NULL,
    table_name  TEXT NOT NULL,
    created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (embedder, model_id),
    UNIQUE (table_name)
);

CREATE TABLE IF NOT EXISTS embedding_ollama__nomic_embed_text_v1_5__768 (
    chunk_id   BIGINT NOT NULL REFERENCES chunk(chunk_id),
    vec          FLOAT[768] NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (chunk_id)
);
CREATE INDEX IF NOT EXISTS emb_hnsw_embedding_ollama__nomic_embed_text_v1_5__768 ON embedding_ollama__nomic_embed_text_v1_5__768 USING HNSW (vec) WITH (metric = 'cosine');

INSERT
OR IGNORE INTO
    embedding_model_registry(embedder, model_id, dim, table_name)
VALUES
    ('ollama', 'nomic-embed-text:v1.5', 768, 'embedding_ollama__nomic_embed_text_v1_5__768');
