-- 010_raw_tables.sql
CREATE TABLE IF NOT EXISTS raw.parcel_raw (
  parcel_id TEXT PRIMARY KEY,
  category  TEXT,
  area_ha   DOUBLE PRECISION,
  geom      geometry(Polygon, 4326),
  load_ts   TIMESTAMP NOT NULL DEFAULT now(),
  record_source TEXT DEFAULT 'sample_data'
);

CREATE TABLE IF NOT EXISTS raw.settlement_raw (
  settlement_code TEXT PRIMARY KEY,
  name            TEXT,
  geom            geometry(Polygon, 4326),
  load_ts         TIMESTAMP NOT NULL DEFAULT now(),
  record_source   TEXT DEFAULT 'sample_data'
);
