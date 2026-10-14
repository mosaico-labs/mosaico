ALTER TABLE sequence_t
    ALTER COLUMN locator_name  TYPE TEXT COLLATE "C",
    ALTER COLUMN path_in_store TYPE TEXT COLLATE "C";

ALTER TABLE session_t
    ALTER COLUMN locator_name TYPE TEXT COLLATE "C";

ALTER TABLE topic_t
    ALTER COLUMN locator_name  TYPE TEXT COLLATE "C",
    ALTER COLUMN path_in_store TYPE TEXT COLLATE "C",
    ALTER COLUMN ontology_tag  TYPE TEXT COLLATE "C";

ALTER TABLE topic_optimization_t
    ALTER COLUMN opt_path_in_store TYPE TEXT COLLATE "C";

ALTER TABLE column_t
    ALTER COLUMN column_name  TYPE TEXT COLLATE "C",
    ALTER COLUMN ontology_tag TYPE TEXT COLLATE "C";

ALTER TABLE column_chunk_textual_t
    ALTER COLUMN min_value TYPE TEXT COLLATE "C",
    ALTER COLUMN max_value TYPE TEXT COLLATE "C";
