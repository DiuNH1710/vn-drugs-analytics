import psycopg2

def create_geocode_metadata(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS geocode_metadata (
                key TEXT PRIMARY KEY,
                value TIMESTAMP
            );
        """)

def get_metadata(conn, key):
    create_geocode_metadata(conn)
    with conn.cursor() as cur:
        cur.execute("SELECT value FROM geocode_metadata WHERE key = %s", (key,))
        result = cur.fetchone()
        return result[0] if result else None

def update_metadata(conn, key, timestamp):
    create_geocode_metadata(conn)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO geocode_metadata (key, value)
            VALUES (%s, %s)
            ON CONFLICT (key) DO UPDATE
            SET value = EXCLUDED.value
        """, (key, timestamp))