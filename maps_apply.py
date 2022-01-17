import sys
import re 
from geopy import GoogleV3
from geopy.exc import GeocoderTimedOut, GeocoderAuthenticationFailure

AUTH_KEY = 'YOUR API KEY'
googleGeo = GoogleV3(api_key=AUTH_KEY)


class geocode:
    def __init__(self, address):
        self.address = address
    
    def get_geocode(self):
        location = None 
        attempt = 0 
        while (location is None) and (attempt <=3):
            try:
                attempt += 1
                location = googleGeo.geocode(self.address, timeout=10)
            except GeocoderAuthenticationFailure:
                print('Error: GeocoderAuthenticationFailure while geocoding {} during attempt #{}').format(self.address, attempt)
            except GeocoderTimedOut:
                print('Error: GeocoderTimedOut while geocoding {} during attempt #{}').format(self.address, attempt)
        return location


class Country:
    def __init__(self, location):
        self.location = location
    
    def get_country(self):
        while self.location is not None:
            result = self.location[0]
            country = [x.strip() for x in result.split(',')].pop(-1)
            pattern = re.compile('([A-Z])')
            if pattern.match(country):
                return country
            else:
                country = [x.strip() for x in result.split(',')].pop(-2)
                return country
        else: 
            return None