-- Folders whose cleanup mark was dropped because a DB re-check found them still referenced.
-- The default backfills existing rows.
ALTER TABLE cleanup_log_t
    ADD COLUMN unmarked_folders JSONB NOT NULL DEFAULT '[]'::jsonb;
