import pandas as pd 
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from geocoder.geo_utils import get_lat_long
from geocoder.db_utils import (
    create_geocode_metadata,
    get_last_geocoded_time,
    update_last_geocoded_time
)

DB_CONFIG = {
      'host': 'host.docker.internal',
      'port': 5432,
      'database': 'postgres',
      'user': 'postgres',
      'password': '123456'
}



def geocode_data(): 
      conn = psycopg2.connect(**DB_CONFIG)
      create_geocode_metadata(conn)
      last_time = get_last_geocoded_time(conn)
      
      # Step 1: Read from PostgreSQL
      print("⏳ Loading data from PostgreSQL...")

      query = """
            SELECT sodangky, tencongtysanxuat, diachisanxuat, nuocsanxuat, fulladdress,lastmodificationtime
            FROM pharmaceutical_data
            WHERE fulladdress IS NOT NULL 
            {filter_clause}
            """.format(
                  filter_clause=f"AND lastmodificationtime > '{last_time}'" if last_time else ""
            )
      
      df = pd.read_sql(query, conn)
      
       # Step 2: Geocoding with thread pool
      print(f"🌐 Geocoding {len(df)} rows...")
      print("COLUMNS:", repr(df.columns.tolist()))
      
      locations = []
      
      with ThreadPoolExecutor(max_workers=10) as executor:
            # futures = { <Future A>: 0, <Future B>: 1, <Future C>: 2}; với 0.1.2 là vị trí của row trong df
            futures = {executor.submit(get_lat_long, row['fulladdress']): index for index, row in df.iterrows()} 
            # Duyệt qua các future đã submit
            for future in as_completed(futures):
                  index = futures[future]
                  print(f"✅ Geocoded row {index + 1}/{len(df)}")
                  try:
                        result = future.result() # trả về longitude, latitude của hàm get_lat_long
                        locations.append(result)
                  except Exception as e:
                        print(f"❌ Error processing row {index}: {e}")
                        locations.append(None)
      
      df['latitude'] = [loc[0] if loc else None for loc in locations]
      df['longitude'] = [loc[1] if loc else None for loc in locations]
      df['location'] = df.apply(
            lambda row: f"{row['latitude']},{row['longitude']}" if row['latitude'] and row['longitude'] else None,
            axis=1
      )
      
      # Step 3: Save back to PostgreSQL
      
      print("✅ Saving geocoded data to PostgreSQL...")
      with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS pharmaceutical_geocoded (
                  sodangky TEXT PRIMARY KEY,
                  tencongtysanxuat, TEXT,
                  diachisanxuat, TEXT,
                  nuocsanxuat, TEXT,
                  fulladdress TEXT,
                  latitude DOUBLE PRECISION,
                  longitude DOUBLE PRECISION,
                  location TEXT,
                  lastmodificationtime TIMESTAMP
                  );
            """)
      for _, row in df.iterrows():
            cur.execute("""
                  INSERT INTO pharmaceutical_geocoded (
                  sodangky, tencongtysanxuat,, diachisanxuat,, nuocsanxuat,,
                   fulladdress, latitude, longitude, location, lastmodificationtime
                  )
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                  ON CONFLICT (sodangky) DO UPDATE SET
                  tencongtysanxuat, = EXCLUDED.tencongtysanxuat,,
                  diachisanxuat, = EXCLUDED.diachisanxuat,,
                  nuocsanxuat, = EXCLUDED.nuocsanxuat,,
                  fulladdress = EXCLUDED. fulladdress,
                  latitude = EXCLUDED.latitude,
                  longitude = EXCLUDED.longitude,
                  location = EXCLUDED.location,
                  lastmodificationtime = EXCLUDED.lastmodificationtime
                  WHERE pharmaceutical_geocoded.lastmodificationtime < EXCLUDED.lastmodificationtime;
            """, (
                row['soDangKy'],
                row['tencongtysanxuat,'],
                row['diachisanxuat,'],
                row['nuocsanxuat,'],
                row['fulladdress'],
                row['latitude'],
                row['longitude'],
                row['location'],
                  row['lastmodificationtime']
            ))
            
      latest_ts = df['lastmodificationtime'].max()
      update_last_geocoded_time(conn, latest_ts)
      conn.commit()
      conn.close()
      print("🎉 Done! Data written to table 'pharmaceutical_geocoded'.")

