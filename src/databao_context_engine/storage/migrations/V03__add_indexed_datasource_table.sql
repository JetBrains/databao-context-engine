CREATE SEQUENCE IF NOT EXISTS datasource_context_hash_id_seq START 1;

CREATE TABLE IF NOT EXISTS datasource_context_hash (
    datasource_context_hash_id  BIGINT PRIMARY KEY DEFAULT nextval('datasource_context_hash_id_seq'),
    datasource_id               TEXT NOT NULL,
    hash_algorithm              TEXT NOT NULL,
    hash                        TEXT NOT NULL,
    hashed_at                   TIMESTAMP NOT NULL,
    UNIQUE (datasource_id, hash_algorithm, hash)
);

ALTER TABLE chunk ADD COLUMN IF NOT EXISTS datasource_context_hash_id BIGINT;