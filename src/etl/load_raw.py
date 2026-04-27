import os, csv, psycopg2
from datetime import datetime

DSN = os.getenv("DSN", "postgresql://dwh_admin:changeme@localhost:5432/dwh")

def exec_sql(cur, sql, params=None):
    cur.execute(sql, params or ())

def load_csv_polygon(cur, table, csv_path, key_cols):
    with open(csv_path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            cols = list(row.keys())
            # Expect 'wkt' column for geometry
            geom_wkt = row.pop('wkt')
            placeholders = ','.join(['%s']*len(row))
            col_list = ','.join(row.keys())
            sql = f"""
            INSERT INTO {table} ({col_list}, geom, load_ts, record_source)
            VALUES ({placeholders}, ST_GeomFromText(%s, 4326), now(), 'sample_data')
            ON CONFLICT ({','.join(key_cols)}) DO UPDATE SET
              geom = EXCLUDED.geom,
              load_ts = now(),
              record_source = 'sample_data'
            """
            exec_sql(cur, sql, list(row.values()) + [geom_wkt])

def main():
    conn = psycopg2.connect(DSN)
    cur = conn.cursor()

    # Ensure tables exist (created by init scripts, but safe)
    cur.execute("SELECT 1")
    conn.commit()

    # Load sample data
    load_csv_polygon(cur, "raw.parcel_raw", "/app/sample_data/parcels.csv", ["parcel_id"])
    load_csv_polygon(cur, "raw.settlement_raw", "/app/sample_data/settlements.csv", ["settlement_code"])

    conn.commit()
    cur.close()
    conn.close()
    print("Loaded RAW sample data.")

if __name__ == "__main__":
    main()
