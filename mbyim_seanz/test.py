import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
from requests.auth import HTTPBasicAuth
import json
from pprint import pprint

with open('../auth.json') as auth_file:
	auth_key = json.load(auth_file)

api_key = auth_key['mbtadeveloperportal']['key']
print(api_key)

#MBTA Info
url = 'http://realtime.mbta.com/developer/api/v2/routes?api_key=' + api_key + '&format=json'
response = urllib.request.urlopen(url).read().decode("utf-8")
mbta_route_data = json.loads(response)
#mbta_route_data = json.dumps(r, sort_keys=True, indent=2)
subway_zero_routes = [element['route_id'] for element in mbta_route_data['mode'][0]['route']]
subway_one_routes = [element['route_id'] for element in mbta_route_data['mode'][1]['route']]#mbta_route_data['mode'][1]['route']
bus_routes = [element['route_id'] for element in mbta_route_data['mode'][3]['route']]#mbta_route_data['mode'][3]['route']
modes = [subway_zero_routes, subway_one_routes, bus_routes]
#print(modes)

#Stop Location Data
#for mode in modes:
	#for i in mode:
url = 'http://realtime.mbta.com/developer/api/v2/stopsbyroute?api_key=' + api_key + '&format=json&route=' + '106'
response = urllib.request.urlopen(url).read().decode("utf-8")
route_info = json.loads(response)
#print(route_info)
#How many directions - are they equivalent for our purposes, or unique?
routes_stops_locations_inbound = [[x['stop_id'], x['stop_lon'], x['stop_lat']] for x in route_info['direction'][0]['stop']]
routes_stops_locations_outbound = [[x['stop_id'], x['stop_lon'], x['stop_lat']] for x in route_info['direction'][1]['stop']]
stop_locations_unique = []
for x in routes_stops_locations_inbound:
		if x not in stop_locations_unique:
			stop_locations_unique.append(x)
for y in routes_stops_locations_outbound:
		if y not in stop_locations_unique: #int(y[0])
			stop_locations_unique.append(y)

print(stop_locations_unique)
print(len(stop_locations_unique))



'''

#print(routes_stops_locations)



