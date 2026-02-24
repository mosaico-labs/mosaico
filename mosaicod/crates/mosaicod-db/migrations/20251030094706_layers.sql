CREATE TABLE layer_t(
  layer_id          SERIAL PRIMARY KEY,
  layer_name        TEXT UNIQUE NOT NULL,
  layer_description TEXT NOT NULL DEFAULT ''
);