from geopy import Nominatim
from geopy.exc import GeocoderTimedOut

def get_lat_long(address, fallback_country=None):
    geolocator = Nominatim(user_agent="vn_drug_geocoder", timeout=10)
    parts = [p.strip() for p in address.split(',') if p.strip()]

    for i in range(len(parts)):
        current_address = ', '.join(parts[i:])
        for attempt in range(2):  # 1 lần chính + 1 lần retry
            try:
                location = geolocator.geocode(current_address, timeout=10)
                if location:
                    return location.latitude, location.longitude
            except GeocoderTimedOut:
                print(f"⚠️ Timeout on attempt {attempt+1} for: {current_address}")
            except Exception as e:
                print(f"❌ Error geocoding address '{current_address}': {e}")
                break 


    # Nếu tất cả phần địa chỉ đều fail → fallback sang quốc gia
    if fallback_country:
        country = fallback_country.split('/')[0].strip()  # Lấy phần đầu nếu có nhiều nước
        try:
            print(f"🔁 Fallback to country: {country}")
            location = geolocator.geocode(country, timeout=10)
            if location:
                return location.latitude, location.longitude
        except Exception as e:
            print(f"❌ Fallback failed for country '{country}': {e}")

    return None