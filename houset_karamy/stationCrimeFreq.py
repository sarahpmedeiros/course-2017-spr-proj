import urllib.request
import json
import sodapy
import dml
import prov.model
import datetime
import uuid
import math

class stationCrimeFreq(dml.Algorithm):
    contributor = 'houset_karamy'
    reads = ['houset_karamy.crimeFreq', 'houset_karamy.policeStations']
    writes = ['houset_karamy.stationCrimeFreq']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('houset_karamy', 'houset_karamy')
        repo.dropCollection("stationCrimeFreq")
        repo.createCollection("stationCrimeFreq")

        #import data we're using
        crimeF = repo['houset_karamy.crimeFreq'].find()
        stations = repo['houset_karamy.policeStations'].find()

        #get the different districts' addresses
        addresses= []
        for district in stations:
            addresses.append((district["name"], district["location_location"]))

        #get all Boston districts and count
        bostonD =[]
        for dist in crimeF:
            if (dist["district/neighborhood"][0] == "Boston"):
                bostonD.append(dist)
                
            
        #count the number of crimesBoston in each district
        #putTogether = []
        for x in addresses:
            for y in bostonD:
                if (y["district/neighborhood"][1] in x):
                    addresses.append(y["count"])
                    

        #get rid of duplicates
#       finalCount = []
#        for d in totalCount:
#            if (d not in finalCount):
#                finalCount.append(d)
        
        #insert into new database        
        repo['houset_karamy.stationCrimeFreq'].insert_many(addresses)
        
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
        doc.add_namespace("alg", "http://datamechanics.io/algorithm/") # The scripts are in <folder>#<filename> format.
        doc.add_namespace("dat", "http://datamechanics.io/data/") # The data sets are in <user>#<collection> format.
        doc.add_namespace("ont", "http://datamechanics.io/ontology#") # "Extension", "DataResource", "DataSet", "Retrieval", "Query", or "Computation".
        doc.add_namespace("log", "http://datamechanics.io/log/") # The event log.
        doc.add_namespace("bdp", "https://data.cityofboston.gov/resource/")

        this_script = doc.agent("alg:houset_karamy#stationCrimeFreq", {prov.model.PROV_TYPE:prov.model.PROV["SoftwareAgent"], "ont:Extension":"py"})
        resource = doc.entity("bdp:pyxn-r3i2", {"prov:label":"stationCrimeFreq", prov.model.PROV_TYPE:"ont:DataResource", "ont:Extension":"json"})
        get_stationCrimeFreq = doc.activity("log:uuid"+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_stationCrimeFreq, this_script)

        doc.usage(get_stationCrimeFreq, resource, startTime, None,
                  {prov.model.PROV_TYPE:"ont:Retrieval"#,
                  #"ont:Query":"?type=Animal+fld_crime&$select=type,latitude,longitude,OPEN_DT"
                  }
                  )

        stationCrimeFreq = doc.entity("dat:houset_karamy#stationCrimeFreq", {prov.model.PROV_LABEL:"stationCrimeFreq", prov.model.PROV_TYPE:"ont:DataSet"})
        doc.wasAttributedTo(stationCrimeFreq, this_script)
        doc.wasGeneratedBy(stationCrimeFreq, get_stationCrimeFreq, endTime)
        doc.wasDerivedFrom(stationCrimeFreq, resource, get_stationCrimeFreq, get_cstationCrimeFreq, get_stationCrimeFreq)

        repo.logout()
                  
        return doc

stationCrimeFreq.execute()
doc = stationCrimeFreq.provenance()
