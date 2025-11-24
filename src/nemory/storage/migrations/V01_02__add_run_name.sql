-- Add the run_name to the run table. This is not using GENERATED because DuckDB only supports VIRTUAL GENERATED columns,
-- which defeats the purpose of storing the folder name we used when creating the run.
-- Ideally, this column would also have been set as NOT NULL and the couple (project_id, run_name) should be unique.
-- But DuckDB doesn't handle altering column constraints
ALTER TABLE run ADD COLUMN IF NOT EXISTS run_name TEXT;