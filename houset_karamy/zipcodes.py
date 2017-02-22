
import urllib.request
import json
import sodapy
import dml
import prov.model
import datetime
import uuid
import math
from collections import defaultdict 

class zipcodes(dml.Algorithm):
    contributor = 'houset_karamy'
    reads = ['houset_karamy.streetsBoston', 'houset_karamy.policeStations']
    writes = ['houset_karamy.zipcodes']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('houset_karamy', 'houset_karamy')
        repo.dropCollection("zipcodes")
        repo.createCollection("zipcodes")

        #import data we're using
        streets = repo['houset_karamy.streetsBoston'].find()
        stations = repo['houset_karamy.policeStations'].find()
        

        #get the zipcodes of police stations
        zipsStations = []
        for station in stations:
            # print(station)
            zipsStations.append((station["name"],station["location_zip"]))

        # print(zipStations)
        #get the different street's zipcodes
        zipsBoston = []
        for st in streets:
            try:
                st['st_name_std']
            except KeyError:
                continue
            else:
                zipsBoston.append((st['st_name_std'], st['l_postcode']))
        
        together = {}
        for station in zipsStations:
            together.update({station[0]: list()})



        for station in zipsStations:
            for street in zipsBoston:
                if(station[1] == street[1]):
                    together[station[0]].append(street[0])

        #insert into new database        
        repo['houset_karamy.zipcodes'].insert_many(together)
        
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        """
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            """

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate("houset_karamy", "houset_karamy")
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('mag', 'https://data.mass.gov/resource/')
        
        this_script = doc.agent('alg:houset_karamy#zipcodes', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource= doc.entity('mag:ms23-5ubn', {'prov:label':'Zip Codes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
                        
        get_zipcodes = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_zipcodes, this_script)

        doc.usage(get_zipcodes, resource, startTime, None,
                  {prov.model.PROV_TYPE:"ont:Retrieval"#,
                  #"ont:Query":"?type=Animal+fld_crime&$select=type,latitude,longitude,OPEN_DT"
                  }
                  )

        zipcodes = doc.entity("dat:houset_karamy#zipcodes", {prov.model.PROV_LABEL:"zipcodes", prov.model.PROV_TYPE:"ont:DataSet"})
        doc.wasAttributedTo(zipcodes, this_script)
        doc.wasGeneratedBy(zipcodes, get_zipcodes, endTime)
        doc.wasDerivedFrom(zipcodes, resource, get_zipcodes, get_zipcodes, get_zipcodes)

        repo.logout()
                  
        return doc

#zipcodes.execute()
#doc = zipcodes.provenance()
