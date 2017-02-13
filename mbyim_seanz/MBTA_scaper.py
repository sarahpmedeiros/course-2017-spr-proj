#Separate python file used for testing purposes to figure out how to scrape the MBTA API sites
#this code is used in data_pull.py and this python file does not need to be run

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
from requests.auth import HTTPBasicAuth
import json


with open('../auth.json') as auth_file:
	auth_key = json.load(auth_file)

api_key = auth_key['mbtadeveloperportal']['key']

#MBTA Info
url = 'http://realtime.mbta.com/developer/api/v2/routes?api_key=' + api_key + '&format=json'
response = urllib.request.urlopen(url).read().decode("utf-8")
mbta_route_data = json.loads(response)

subway_zero_route_id = [element['route_id'] for element in mbta_route_data['mode'][0]['route']] #e.g. ['Green-B', 'Green-C', 'Green-D', 'Green-E', 'Mattapan']
subway_one_route_id = [element['route_id'] for element in mbta_route_data['mode'][1]['route']]
bus_route_id = [element['route_id'] for element in mbta_route_data['mode'][3]['route']] 

mode_ids = [subway_zero_route_id, subway_one_route_id, bus_route_id]

#Stop Location Data
stop_locations_unique = []
for mode_id in mode_ids: #iterate through all the list of list of modes
    for route_id in mode_id: #e.g. Green-B
        url = 'http://realtime.mbta.com/developer/api/v2/stopsbyroute?api_key=' + api_key + '&format=json&route=' + route_id
        response = urllib.request.urlopen(url).read().decode("utf-8")
        route_info = json.loads(response)

        #How many directions - are they equivalent for our purposes, or unique?
        directions = route_info['direction']

        for i in range(len(directions)):
            routes_stops_locations = [{"stop_id": stop['stop_id'], "stop_lon": stop['stop_lon'], "stop_lat": stop['stop_lat'], "route_id":route_id} for stop in route_info['direction'][i]['stop']]

            for stop in routes_stops_locations:
                if stop['stop_id'] not in stop_locations_unique:
                    stop_locations_unique.append(stop)

string_quote = json.dumps(stop_locations_unique)
string_quote.replace("'", '"')

print(string_quote)
print(type(string_quote))

