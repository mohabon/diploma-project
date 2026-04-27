import os, psycopg2, hashlib

DSN = os.getenv("DSN", "postgresql://dwh_admin:changeme@localhost:5432/dwh")

def md5_hex(s: str) -> str:
    return hashlib.md5(s.encode('utf-8')).hexdigest()

def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()

    # Upsert Hubs from RAW
    cur.execute("""
        INSERT INTO dv.hub_parcel (hk_parcel, bk_parcel, load_ts, record_source)
        SELECT md5(parcel_id), parcel_id, now(), 'raw.parcel_raw'
        FROM raw.parcel_raw
        ON CONFLICT (hk_parcel) DO NOTHING;
    """)
    cur.execute("""
        INSERT INTO dv.hub_settlement (hk_settlement, bk_settlement, load_ts, record_source)
        SELECT md5(settlement_code), settlement_code, now(), 'raw.settlement_raw'
        FROM raw.settlement_raw
        ON CONFLICT (hk_settlement) DO NOTHING;
    """)

    # Satellites (thematic & geom) for parcels
    cur.execute("""
        INSERT INTO dv.sat_parcel_thematic (hk_parcel, category, area_ha, hashdiff, load_ts, record_source)
        SELECT md5(p.parcel_id), p.category, p.area_ha,
               md5(coalesce(p.category,'') || '|' || coalesce(p.area_ha::text,'')),
               now(), 'raw.parcel_raw'
        FROM raw.parcel_raw p;
    """)
    cur.execute("""
        INSERT INTO dv.sat_parcel_geom (hk_parcel, geom, hashdiff, load_ts, record_source)
        SELECT md5(p.parcel_id), p.geom,
               md5(ST_AsText(p.geom)),
               now(), 'raw.parcel_raw'
        FROM raw.parcel_raw p;
    """)

    # Link parcels to settlements by spatial intersection
    cur.execute("""
        WITH j AS (
          SELECT md5(p.parcel_id) AS hk_parcel,
                 md5(s.settlement_code) AS hk_settlement
          FROM raw.parcel_raw p
          JOIN raw.settlement_raw s
            ON ST_Intersects(p.geom, s.geom)
        )
        INSERT INTO dv.link_parcel_settlement (hk_link_ps, hk_parcel, hk_settlement, load_ts, record_source)
        SELECT md5(j.hk_parcel || j.hk_settlement), j.hk_parcel, j.hk_settlement, now(), 'spatial_join'
        FROM j
        ON CONFLICT (hk_link_ps) DO NOTHING;
    """)

    # Build/refresh DM
    sql_path = "/app/sql/marts/create_or_refresh_dm_parcel_usage.sql"
    with open(sql_path, "r", encoding="utf-8") as f:
        cur.execute(f.read())

    conn.commit()
    cur.close()
    conn.close()
    print("Transformed to DV and refreshed DM.")

if __name__ == "__main__":
    main()
