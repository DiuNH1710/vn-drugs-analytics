import psycopg2

def create_geocode_metadata(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS geocode_metadata (
                id SERIAL PRIMARY KEY,
                last_modification_time TIMESTAMP
            );
        """)
        conn.commit()

def get_last_geocoded_time(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(last_modification_time) FROM geocode_metadata;")
        result = cur.fetchone()
        return result[0]

def update_last_geocoded_time(conn, latest_ts ):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO geocode_metadata (last_modification_time)
            VALUES (%s)
        """, (latest_ts ,))
        conn.commit()
