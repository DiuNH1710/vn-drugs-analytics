import psycopg2
import pandas as pd
from datetime import datetime
from geocoder.db_config import DB_CONFIG
from geocoder.db_utils import get_metadata, update_metadata

def extract_unique_addresses():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    last_filtered_time = get_metadata(conn, 'last_filter_time')
    
    # Step 1: Truy vấn địa chỉ mới
    print("⏳ Loading addresses...")
    query = f"""
        SELECT DISTINCT diachisanxuat, nuocsanxuat, MAX(lastmodificationtime) as latest_time
        FROM pharmaceutical_data
        WHERE diachisanxuat IS NOT NULL AND nuocsanxuat IS NOT NULL
        {"AND lastmodificationtime > %s" if last_filtered_time else ""}
        GROUP BY diachisanxuat, nuocsanxuat
    """
    
    df = pd.read_sql(query, conn, params=[last_filtered_time] if last_filtered_time else [])

    print(f"✅ Found {len(df)} new unique addresses")
    
        # Step 2: Tạo bảng lưu kết quả nếu chưa có
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS unique_manufacturing_addresses (
            id SERIAL PRIMARY KEY,
            diachisanxuat TEXT,
            nuocsanxuat TEXT,
            latest_time TIMESTAMP
        )
    """)
    
     # Step 3: Insert vào bảng (nếu cần update thì dùng ON CONFLICT theo diachisanxuat + nuocsanxuat)
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO unique_manufacturing_addresses (diachisanxuat, nuocsanxuat, latest_time)
            VALUES (%s, %s, %s)
            ON CONFLICT (diachisanxuat, nuocsanxuat) DO UPDATE
            SET latest_time = EXCLUDED.latest_time
        """, (row['diachisanxuat'], row['nuocsanxuat'], row['latest_time']))

    # Step 4: Update thời gian lọc gần nhất
    if not df.empty:
        latest_ts = df['latest_time'].max()
        update_metadata(conn,'last_filter_time', latest_ts)

    conn.commit()
    conn.close()
    print("🎉 Done saving unique addresses.")

