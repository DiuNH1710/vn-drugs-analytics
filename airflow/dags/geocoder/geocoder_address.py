import pandas as pd
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from geocoder.geo_utils import get_lat_long
from geocoder.db_config import DB_CONFIG


BATCH_SIZE = 50
MAX_WORKERS = 2


def geocode_unique_addresses():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Tạo bảng kết quả nếu chưa tồn tại
    cur.execute("""
        CREATE TABLE IF NOT EXISTS geocoded_manufacturing_addresses (
            address_id INTEGER PRIMARY KEY,
            diachisanxuat TEXT,
            nuocsanxuat TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            location TEXT,
            geocoded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()

    print("⏳ Loading addresses to geocode...")

    while True:
        query = """
            SELECT ua.id, ua.diachisanxuat, ua.nuocsanxuat
            FROM unique_manufacturing_addresses ua
            LEFT JOIN geocoded_manufacturing_addresses ga ON ua.id = ga.address_id
            WHERE ga.address_id IS NULL
            ORDER BY ua.id
            LIMIT %s
        """
        df = pd.read_sql(query, conn, params=(BATCH_SIZE,))

        if df.empty:
            print("✅ No more addresses to geocode.")
            break

        print(f"🌐 Geocoding batch of {len(df)} rows...")

        results = [None] * len(df)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(get_lat_long, row['diachisanxuat'], row['nuocsanxuat']): idx
                for idx, row in df.iterrows()
            }

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    result = future.result()
                    results[idx] = result
                    print(f"✅ Geocoded row {df.iloc[idx]['id']}")
                except Exception as e:
                    print(f"❌ Error processing row {df.iloc[idx]['id']}: {e}")

        df['latitude'] = [res[0] if res else None for res in results]
        df['longitude'] = [res[1] if res else None for res in results]
        df['location'] = df.apply(
            lambda row: f"{row['latitude']},{row['longitude']}" if row['latitude'] and row['longitude'] else None,
            axis=1
        )

        # Lưu từng dòng geocoded thành công vào DB
        for _, row in df.iterrows():
            if row['latitude'] and row['longitude']:
                cur.execute("""
                    INSERT INTO geocoded_manufacturing_addresses (
                        address_id, diachisanxuat, nuocsanxuat, latitude, longitude, location
                    )
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (address_id) DO UPDATE
                    SET diachisanxuat = EXCLUDED.diachisanxuat,
                        nuocsanxuat = EXCLUDED.nuocsanxuat,
                        latitude = EXCLUDED.latitude,
                        longitude = EXCLUDED.longitude,
                        location = EXCLUDED.location,
                        geocoded_at = CURRENT_TIMESTAMP
                """, (
                    row['id'],
                    row['diachisanxuat'],
                    row['nuocsanxuat'],
                    row['latitude'],
                    row['longitude'],
                    row['location']
                ))
        conn.commit()
        print("💾 Batch saved to database.\n")

    conn.close()
    print("🎉 All addresses geocoded.")
