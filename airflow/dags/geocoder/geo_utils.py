from geopy import Nominatim
from geopy.exc import GeocoderTimedOut

def get_lat_long(address):
    geolocator = Nominatim(user_agent="vn_drug_geocoder", timeout=10)
    parts = address.split(',')
    while parts:
            try:
                current_address = ','.join(parts)
                location = geolocator.geocode(current_address)
                if location:
                  return location.latitude, location.longitude
            except GeocoderTimedOut:
                  continue
            except Exception as e:
                print(f"Error geocoding address '{current_address}': {e}")
                return None
            parts.pop(0)