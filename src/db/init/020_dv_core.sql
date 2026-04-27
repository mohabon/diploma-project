-- 020_dv_core.sql
-- Hubs
CREATE TABLE IF NOT EXISTS dv.hub_parcel (
  hk_parcel CHAR(32) PRIMARY KEY,
  bk_parcel TEXT UNIQUE,
  load_ts   TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.hub_settlement (
  hk_settlement CHAR(32) PRIMARY KEY,
  bk_settlement TEXT UNIQUE,
  load_ts   TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

-- Links
CREATE TABLE IF NOT EXISTS dv.link_parcel_settlement (
  hk_link_ps CHAR(32) PRIMARY KEY,
  hk_parcel  CHAR(32) NOT NULL REFERENCES dv.hub_parcel(hk_parcel),
  hk_settlement CHAR(32) NOT NULL REFERENCES dv.hub_settlement(hk_settlement),
  load_ts   TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

-- Satellites
CREATE TABLE IF NOT EXISTS dv.sat_parcel_thematic (
  hk_parcel  CHAR(32) NOT NULL REFERENCES dv.hub_parcel(hk_parcel),
  category   TEXT,
  area_ha    DOUBLE PRECISION,
  hashdiff   CHAR(32) NOT NULL,
  load_ts    TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dv.sat_parcel_geom (
  hk_parcel  CHAR(32) NOT NULL REFERENCES dv.hub_parcel(hk_parcel),
  geom       geometry(Polygon, 4326),
  hashdiff   CHAR(32) NOT NULL,
  load_ts    TIMESTAMP NOT NULL,
  record_source TEXT NOT NULL
);

-- DM: parcel usage summary
CREATE TABLE IF NOT EXISTS dm.f_parcel_usage (
  settlement_name TEXT,
  category TEXT,
  total_area_ha DOUBLE PRECISION,
  load_ts TIMESTAMP NOT NULL DEFAULT now()
);
