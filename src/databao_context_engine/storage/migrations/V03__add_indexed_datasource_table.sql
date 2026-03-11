CREATE SEQUENCE IF NOT EXISTS datasource_context_hash_id_seq START 1;

CREATE TABLE IF NOT EXISTS datasource_context_hash (
    datasource_context_hash_id  BIGINT PRIMARY KEY DEFAULT nextval('datasource_context_hash_id_seq'),
    datasource_id               TEXT NOT NULL,
    hash_algorithm              TEXT NOT NULL,
    hash                        TEXT NOT NULL,
    hashed_at                   TIMESTAMP NOT NULL,
    UNIQUE (datasource_id, hash_algorithm, hash)
);

-- This migration loses all previously indexed chunks
-- We're dropping and recreating the chunk table to add the new foreign key
-- There is no need to keep the old data because it won't be used in the new code anyway
-- After this update, only chunks linked to a datasource_context_hash will be searched and current chunks don't have one

-- See V03__add_indexed_datasource_table.py for the pre-migration cleanup of
-- embedding shard tables referenced by embedding_model_registry.
-- We're not re-creating any embedding tables as the ones required by the embedding model will be created automatically
-- during the next index
DROP TABLE chunk;

CREATE TABLE chunk (
    chunk_id                    BIGINT PRIMARY KEY DEFAULT nextval('chunk_id_seq'),
    full_type                   TEXT NOT NULL,
    datasource_id               TEXT NOT NULL,
    embeddable_text             TEXT NOT NULL,
    display_text                TEXT,
    created_at                  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    keyword_index_text          TEXT,
    datasource_context_hash_id  BIGINT NOT NULL REFERENCES datasource_context_hash(datasource_context_hash_id)
);
