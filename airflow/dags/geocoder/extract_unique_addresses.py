import psycopg2
import pandas as pd
from geocoder.db_config import DB_CONFIG

def extract_unique_addresses():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Step 1: Truy vấn toàn bộ địa chỉ duy nhất từ bảng gốc
    print("⏳ Loading unique addresses from pharmaceutical_data...")
    query = """
        SELECT DISTINCT diachisanxuat, nuocsanxuat
        FROM pharmaceutical_data
        WHERE diachisanxuat IS NOT NULL AND nuocsanxuat IS NOT NULL
    """
    df = pd.read_sql(query, conn)

    print(f"✅ Found {len(df)} unique addresses.")

    # Step 2: Tạo bảng nếu chưa có
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS unique_manufacturing_addresses (
            id SERIAL PRIMARY KEY,
            diachisanxuat TEXT NOT NULL,
            nuocsanxuat TEXT NOT NULL,
            UNIQUE(diachisanxuat, nuocsanxuat)
        )
    """)

    # Step 3: Chỉ insert nếu địa chỉ chưa tồn tại
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO unique_manufacturing_addresses (diachisanxuat, nuocsanxuat)
            VALUES (%s, %s)
            ON CONFLICT (diachisanxuat, nuocsanxuat) DO NOTHING
        """, (row['diachisanxuat'], row['nuocsanxuat']))

    conn.commit()
    conn.close()
    print("🎉 Done inserting unique addresses.")
