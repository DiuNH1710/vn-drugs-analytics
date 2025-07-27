import pandas as pd 
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from geocoder.geo_utils import get_lat_long
from geocoder.db_utils import get_metadata, update_metadata
from geocoder.db_config import DB_CONFIG



def geocode_unique_addresses(): 
      conn = psycopg2.connect(**DB_CONFIG)
      cur = conn.cursor()
      last_geocoded_time = get_metadata(conn, 'last_geocoded_time')
      
      # Step 1: Read from PostgreSQL
      print("⏳ Loading data from PostgreSQL...")

      query = """
        SELECT id, diachisanxuat, nuocsanxuat, latest_time
        FROM unique_manufacturing_addresses
        {filter_clause}
    """.format(
        filter_clause=f"WHERE latest_time > '{last_geocoded_time}'" if last_geocoded_time else ""
    )
      
      df = pd.read_sql(query, conn)
      
      if df.empty:
        print("✅ No new addresses to geocode.")
        return
      
       # Step 2: Geocoding with thread pool
      print(f"🌐 Geocoding {len(df)} rows...")
      print("COLUMNS:", repr(df.columns.tolist()))
      
      results = [None] * len(df)
      
      with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(get_lat_long, row['diachisanxuat'], row['nuocsanxuat']): idx
            for idx, row in df.iterrows()
        }

        for future in as_completed(futures):
            idx = futures[future]
            try:
                result = future.result()
                results[idx] = result
                print(f"✅ Geocoded row {idx+1}/{len(df)}")
            except Exception as e:
                print(f"❌ Error processing row {idx}: {e}")
      
      df['latitude'] = [res[0] if res else None for res in results]
      df['longitude'] = [res[1] if res else None for res in results]
      df['location'] = df.apply(
            lambda row: f"{row['latitude']},{row['longitude']}" if row['latitude'] and row['longitude'] else None,
            axis=1
      )

      
      # Step 3: Save back to PostgreSQL
      #create dâtabase table if not exists
      
     # Save to geocoded_manufacturing_addresses
     
      cur.execute("""
            CREATE TABLE IF NOT EXISTS geocoded_manufacturing_addresses (
                address_id INTEGER PRIMARY KEY,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                location TEXT,
                geocoded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
      
      for _, row in df.iterrows():
            if row['latitude'] and row['longitude']:
                  cur.execute("""
                        INSERT INTO geocoded_manufacturing_addresses (address_id, latitude, longitude, location)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (address_id) DO UPDATE
                        SET latitude = EXCLUDED.latitude,
                                    longitude = EXCLUDED.longitude,
                                    location = EXCLUDED.location,
                                    geocoded_at = CURRENT_TIMESTAMP
                  """, (
                              row['id'], row['latitude'], row['longitude'], row['location']
                  ))

      latest_ts = df['latest_time'].max()
      update_metadata(conn, 'last_geocoded_time', latest_ts)
      conn.commit()
      conn.close()
      print("🎉 Done geocoding and saving results.")
