import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class datapull(dml.Algorithm):
    contributor = 'mbyim_seanz'
    reads = []
    writes = ['mbyim_seanz.vehicle_tax', 'mbyim_seanz.parking_tickets', 'mbyim_seanz.mbta_stops', 'mbyim_seanz.property_assessments', 'mbyim_seanz.snow_parking']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mbyim_seanz', 'mbyim_seanz')

        #Parking Tickets Info---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        # url = 'https://data.cityofboston.gov/resource/cpdb-ie6e.json?$select=ticket_loc,violation1'
        url = 'https://data.cityofboston.gov/resource/cpdb-ie6e.json?$limit=1000000'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("parking_tickets")
        repo.createCollection("parking_tickets")
        repo['mbyim_seanz.parking_tickets'].insert_many(r)
        repo['mbyim_seanz.parking_tickets'].metadata({'complete':True})
        print(repo['mbyim_seanz.parking_tickets'].metadata())

        #Vehicle Excise Tax Info------------------------------------------------------------------------------------------------------------------------------------------
        # url = 'https://data.cityofboston.gov/resource/ww9y-x77a.json?$select=zip'
        url = 'https://data.cityofboston.gov/resource/ww9y-x77a.json?$limit=1000000'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("vehicle_tax")
        repo.createCollection("vehicle_tax")
        repo['mbyim_seanz.vehicle_tax'].insert_many(r)
        repo['mbyim_seanz.vehicle_tax'].metadata({'complete':True})
        print(repo['mbyim_seanz.vehicle_tax'].metadata())

        #MBTA Info------------------------------------------------------------------------------------------------------------------------------------------
        #MBTA API key Info
        with open('../auth.json') as auth_file:
            auth_key = json.load(auth_file)

        api_key = auth_key['mbtadeveloperportal']['key']
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

        r = json.loads(string_quote)
        s = json.dumps(r, sort_keys=True, indent=2)

        repo.dropCollection("mbta_stops")
        repo.createCollection("mbta_stops")
        repo['mbyim_seanz.mbta_stops'].insert_many(r)
        repo['mbyim_seanz.mbta_stops'].metadata({'complete':True})
        print(repo['mbyim_seanz.mbta_stops'].metadata())

        #Property assessment data------------------------------------------------------------------------------------------------------------------------------------------
        # url = 'https://data.cityofboston.gov/resource/jsri-cpsq.json?%24select=full_address,ZIPCODE,AV_LAND,AV_BLDG,AV_TOTAL,Location'
        url = 'https://data.cityofboston.gov/resource/jsri-cpsq.json?$limit=1000000'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("property_assessments")
        repo.createCollection("property_assessments")
        repo['mbyim_seanz.property_assessments'].insert_many(r)
        repo['mbyim_seanz.property_assessments'].metadata({'complete':True})
        print(repo['mbyim_seanz.property_assessments'].metadata())

        #Snow parking data------------------------------------------------------------------------------------------------------------------------------------------
        url = 'http://datamechanics.io/data/mbyim_seanz/SnowParking.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("snow_parking")
        repo.createCollection("snow_parking")
        repo['mbyim_seanz.snow_parking'].insert_many(r)
        repo['mbyim_seanz.snow_parking'].metadata({'complete':True})
        print(repo['mbyim_seanz.snow_parking'].metadata())

        #end------------------------------------------------------------------------------------------------------------------------------------------
        repo.logout()
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
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
        repo.authenticate('mbyim_seanz', 'mbyim_seanz')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('mbta', 'http://realtime.mbta.com/developer/api/v2/stopsbyroute')

        this_script = doc.agent('alg:mbyim_seanz#data_pull', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_vehicle_tax = doc.entity('bdp:ww9y-x77a', {'prov:label':'Vehicle Tax', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_parking_tickets = doc.entity('bdp:cpdb-ie6e', {'prov:label':'Parking Tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_mbta_stops = doc.entity('mbta:filler', {'prov:label':'MBTA Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_property_assessments = doc.entity('bdp:jsri-cpsq', {'prov:label':'Property Assessments 2014', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_snow_parking = doc.entity('dat:mbyim_seanz/SnowParking.json', {'prov:label':'Snow Parking', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_vehicle_tax = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_parking_tickets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_mbta_stops = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_property_assessments = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_snow_parking = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_vehicle_tax, this_script)
        doc.wasAssociatedWith(get_parking_tickets, this_script)
        doc.wasAssociatedWith(get_mbta_stops, this_script)
        doc.wasAssociatedWith(get_property_assessments, this_script)
        doc.wasAssociatedWith(get_snow_parking, this_script)

        doc.usage(get_vehicle_tax, resource_vehicle_tax, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?$select=zip' #not sure what this does
                  }
        )
        doc.usage(get_parking_tickets, resource_parking_tickets, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?$select=ticket_loc,violation1'
                  }
        )
        doc.usage(get_mbta_stops, resource_mbta_stops, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?format=json'
                  }
        )
        doc.usage(get_property_assessments, resource_property_assessments, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?$select=full_address,ZIPCODE,AV_LAND,AV_BLDG,AV_TOTAL,Location'
                  }
        )

        doc.usage(get_snow_parking, resource_snow_parking, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?format=json'
                  }
        )

        parking_tickets = doc.entity('dat:mbyim_seanz#parking_tickets', {prov.model.PROV_LABEL:'Parking Tickets', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(parking_tickets, this_script)
        doc.wasGeneratedBy(parking_tickets, get_parking_tickets, endTime)
        #doc.wasDerivedFrom(parking_tickets, resource, get_parking_tickets, get_parking_tickets, get_parking_tickets)

        vehicle_tax = doc.entity('dat:mbyim_seanz#vehicle_tax', {prov.model.PROV_LABEL:'Vehicle Tax', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(vehicle_tax, this_script)
        doc.wasGeneratedBy(vehicle_tax, get_vehicle_tax, endTime)
        #doc.wasDerivedFrom(vehicle_tax, resource, get_vehicle_tax, get_vehicle_tax, get_vehicle_tax)

        mbta_stops = doc.entity('dat:mbyim_seanz#mbta_stops', {prov.model.PROV_LABEL:'MBTA Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(mbta_stops, this_script)
        doc.wasGeneratedBy(mbta_stops, get_mbta_stops, endTime)
        #doc.wasDerivedFrom(vehicle_tax, resource, get_vehicle_tax, get_vehicle_tax, get_vehicle_tax)

        property_assessments = doc.entity('dat:mbyim_seanz#property_assessments', {prov.model.PROV_LABEL:'Property Assessments 2014', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(property_assessments, this_script)
        doc.wasGeneratedBy(property_assessments, get_property_assessments, endTime)
        #doc.wasDerivedFrom(vehicle_tax, resource, get_vehicle_tax, get_vehicle_tax, get_vehicle_tax)

        snow_parking = doc.entity('dat:mbyim_seanz#snow_parking', {prov.model.PROV_LABEL:'Snow Parking', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(snow_parking, this_script)
        doc.wasGeneratedBy(snow_parking, get_snow_parking, endTime)

        #end---
        repo.logout()
        return doc

datapull.execute()
doc = datapull.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
