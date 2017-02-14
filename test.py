import urllib.request
import json

url = "https://data.cityofboston.gov/resource/awu8-dc52.json"
response = urllib.request.urlopen(url).read().decode("utf-8")
print(json.dumps(json.loads(response), sort_keys=True, indent=2))