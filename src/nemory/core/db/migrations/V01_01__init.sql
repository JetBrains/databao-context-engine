INSTALL vss;
LOAD vss;
SET hnsw_enable_experimental_persistence = true;

CREATE SEQUENCE IF NOT EXISTS run_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS entity_id_seq START 1;
CREATE SEQUENCE IF NOT EXISTS segment_id_seq START 1;

CREATE TABLE IF NOT EXISTS run (
    run_id          BIGINT PRIMARY KEY DEFAULT nextval('run_id_seq'),
    status          TEXT NOT NULL,
    project_id      TEXT NOT NULL,
    started_at      TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at        TIMESTAMP,
    nemory_version  TEXT
);

CREATE TABLE IF NOT EXISTS entity (
    entity_id           BIGINT PRIMARY KEY DEFAULT nextval('entity_id_seq'),
    run_id              BIGINT NOT NULL REFERENCES run(run_id),
    plugin              TEXT NOT NULL,
    source_id           TEXT NOT NULL,
    storage_directory   TEXT NOT NULL,
    created_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS segment (
    segment_id       BIGINT PRIMARY KEY DEFAULT nextval('segment_id_seq'),
    entity_id        BIGINT NOT NULL REFERENCES entity(entity_id),
    embeddable_text  TEXT NOT NULL,
    display_text     TEXT,
    created_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS embedding (
    segment_id   BIGINT NOT NULL REFERENCES segment(segment_id),
    embedder     TEXT   NOT NULL,
    model_id     TEXT   NOT NULL,
    vec          FLOAT[768] NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (segment_id, embedder, model_id)
);
CREATE INDEX IF NOT EXISTS emb_hnsw ON embedding USING HNSW (vec) WITH (metric = 'cosine');

CREATE INDEX IF NOT EXISTS idx_entity_run ON entity(run_id);
CREATE INDEX IF NOT EXISTS idx_entity_plugin_run ON entity(plugin, run_id);
CREATE INDEX IF NOT EXISTS idx_segment_entity ON segment(entity_id);