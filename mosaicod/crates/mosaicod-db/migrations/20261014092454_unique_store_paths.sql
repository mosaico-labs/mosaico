ALTER TABLE sequence_t
    ADD CONSTRAINT sequence_t_path_in_store_key UNIQUE (path_in_store);

ALTER TABLE topic_t
    ADD CONSTRAINT topic_t_path_in_store_key UNIQUE (path_in_store);

ALTER TABLE topic_optimization_t
    ADD CONSTRAINT topic_optimization_t_opt_path_in_store_key UNIQUE (opt_path_in_store);
