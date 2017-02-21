'''
    Pauline Ramirez and Carlos Syquia
    transformation2.py
    obesity and healthy corner stores and correlation to hospital locations
'''

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
from geopy.distance import vincenty
from geopy.geocoders import Nominatim


class transformation2(dml.Algorithm):
    contributor = 'pgr_syquiac'
    reads = ['pgr_syquiac.cdc', 'pgr_syquiac.pools', 'pgr_syquiac.stores']
    writes = ['pgr_syquiac.obesity_pools_stores']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pgr_syquiac', 'pgr_syquiac')

        print("Starting.....")

        cdcRepo = repo.pgr_syquiac.cdc
        poolsRepo = repo.pgr_syquiac.pools
        storesRepo = repo.pgr_syquiac.stores

        # Gather obesity statistics
        cdc = cdcRepo.find()
        obesity = []
        print("Collecting appropriate data from CDC data set...")
        for i in cdc:
            # Look to make sure its a census tract, for health insurance rates
            if (i['measure'] == 'Obesity among adults aged >=18 Years' or
                i['measureid'] == 'OBESITY'):
                obesity.append(i)

        print(len(obesity))

        pools_ = poolsRepo.find()
        pools = []

        for i in pools_:
            i['coordinates'] = [] # Need to add a field for the coordinates
            pools.append(i)

        print(len(pools))
        pools = pools[:len(pools)-1] # Take out the last row because it's just empty
        # print(len(pools))


        # Create Nominatim
        geolocator = Nominatim()
        count = 0
        # Fill out the coordinates field for each pool from the dataset
        # for i in pools:
        #     # print(i)
        #     addr = i["st_no"] + " "
        #     count += 1
        #     # Need this if statement because geopy doesn't recognize "Bl" as a street suffix
        #     if i["suffix"] == "BL":
        #         addr += i["st_name"] + " Blvd " + i["location_1_city"] + " " + i["location_1_zip"]
        #     elif i["st_name"] == "VFW ":
        #         addr += "Veterans of Foreign Wars Pkwy " + i['location_1_zip']
        #     elif i["st_name"] == "THIRD AVE BLDG #150": # Typo in this address so we need a case for that
        #         addr += "THIRD AVE " + i["location_1_zip"]
        #     elif i["business_name"] == "WELLBRIDGE" or i["business_name"] == 'Commonwealth Sports Club (Special Purpose Pool)':
        #         addr += i["st_name"] + " " + i["suffix"] + " 02215"
        #     elif i["business_name"] == 'RESIDENCE INN MARRIOTT (SY)' or i['business_name'] == 'RESIDENCE INN MARRIOTT (SPA)':
        #         addr += i["st_name"] + " " + i["suffix"] + " 02129"
        #     elif i["business_name"] == "Langham Hotel" or i["business_name"] == 'Langham Hotel (Sp. Purpose Pool)':
        #         addr += i["st_name"] + " " + i["suffix"] + " 02110"
        #     elif (i['business_name'] == 'Courtyard By Marriott (Swim Pool)') or (i['business_name'] == 'Courtyard by Marriott (Swimming Pool)') or (i['business_name'] == 'Courtyard By Marriott (Sp. Purpose Pool)') or (i['business_name'] == 'Courtyard by Marriott (Special Purpose Pool)'):
        #         addr += "Mcclellan Hwy " + i["location_1_zip"]
        #     else:
        #         if "location_1_zip" in i:
        #             addr += i["st_name"] + " " + i["suffix"] + " " + i["location_1_zip"]
        #         else:
        #             # print(i)
        #             addr += i["st_name"] + " " + i["suffix"] + " Boston MA "
        #
        #     # print(addr)
        #     # print("Count : " + str(count))
        #     location = geolocator.geocode(addr)
        #     # print("Address: " + location.address)
        #     # print("Coordinates: " + str((location.longitude, location.latitude)))
        #     # print("--**--")
        #
        #     i["coordinates"].append((location.longitude, location.latitude))

        #print(pools[0])
        # Collect the closest pools and add that to the stores, and then collect the closest obesity data points

        stores_ = storesRepo.find()
        stores = []
        for i in stores_:
            i['closest_pools'] = []
            i['obesity_rates'] = []
            stores.append(i)


        # print("Mapping swimming pools to closest corner stores...")
        # for i in pools:
        #     # We want to iterate through all the visits rates and map them to the
        #     # closest hospital
        #     distance = vincenty(i['coordinates'][0], stores[0]['location']['coordinates']).miles
        #     idx = 0
        #
        #     for j in range(len(stores)):
        #         rate_distance = vincenty(i['coordinates'][0], stores[j]['location']['coordinates']).miles
        #         if rate_distance < distance:
        #             distance = rate_distance
        #             idx = j
        #
        #     stores[idx]['closest_pools'].append(i)

        print("Now mapping obesity data points to the closest corner stores...")
        for i in obesity:
            # We want to iterate through all the visits rates and map them to the
            # closest hospital
            distance = vincenty(i['geolocation']['coordinates'], stores[0]['location']['coordinates']).miles
            idx = 0

            for j in range(len(stores)):
                rate_distance = vincenty(i['geolocation']['coordinates'], stores[j]['location']['coordinates']).miles
                if rate_distance < distance:
                    distance = rate_distance
                    idx = j

            stores[idx]['obesity_rates'].append(i)

        # print(stores[0])

        repo.dropPermanent("obesity_pools_stores")
        repo.createPermanent("obesity_pools_stores")
        repo['pgr_syquiac.obesity_pools_stores'].insert_many(stores)
        print("Inserted new collection!")

        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}

        # print(len(stores))
        # print(stores[0]['location'])
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pgr_syquiac', 'pgr_syquiac')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:pgr_syquiac#transformation2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        obesity_pools_stores = doc.entity('dat:pgr_syquiac#obesity_pools_stores', {prov.model.PROV_LABEL: 'Obesity rates, public pools and healthy corner stores', prov.model.PROV_TYPE:'ont:Dataset'})
        get_obesity_pools_stores = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Obesity rates and their distance from public pools and healthy corner stores'})
        doc.wasAssociatedWith(get_obesity_pools_stores, this_script)
        doc.used(obesity_pools_stores, get_obesity_pools_stores, startTime)
        doc.wasAttributedTo(obesity_pools_stores, this_script)
        doc.wasGeneratedBy(obesity_pools_stores, get_obesity_pools_stores, endTime)

transformation2.execute()

doc = transformation1.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
