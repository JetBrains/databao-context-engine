INSTALL vss;
LOAD vss;
SET hnsw_enable_experimental_persistence = true;

CREATE SEQUENCE IF NOT EXISTS chunk_id_seq START 1;

CREATE TABLE IF NOT EXISTS chunk (
    chunk_id        BIGINT PRIMARY KEY DEFAULT nextval('chunk_id_seq'),
    full_type       TEXT NOT NULL,
    datasource_id   TEXT NOT NULL,
    display_text    TEXT,
    created_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
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
