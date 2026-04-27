import os, psycopg2

DSN = os.getenv("DSN", "postgresql://dwh_admin:changeme@localhost:5432/dwh")

def assert_zero(cur, sql, message):
    cur.execute(sql)
    n = cur.fetchone()[0]
    assert n == 0, f"{message}: {n}"
    print(f"OK: {message} = 0")

def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()

    # 1) No invalid parcel geometries
    assert_zero(cur, "SELECT COUNT(*) FROM raw.parcel_raw WHERE NOT ST_IsValid(geom);",
                "Invalid parcel geometries")

    # 2) No negative area
    assert_zero(cur, "SELECT COUNT(*) FROM raw.parcel_raw WHERE area_ha IS NULL OR area_ha <= 0;",
                "Non-positive area in parcels")

    # 3) DM not empty
    cur.execute("SELECT COUNT(*) FROM dm.f_parcel_usage;")
    dm_count = cur.fetchone()[0]
    assert dm_count > 0, "DM f_parcel_usage is empty"
    print(f"OK: dm.f_parcel_usage rows = {dm_count}")

    conn.close()

if __name__ == "__main__":
    main()
