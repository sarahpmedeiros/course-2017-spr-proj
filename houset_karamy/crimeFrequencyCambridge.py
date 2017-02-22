import urllib.request
import json
import sodapy
import dml
import prov.model
import datetime
import uuid
import math

class crimeFrequencyCambridge(dml.Algorithm):
    contributor = 'houset_karamy'
    reads = ['houset_karamy.crimeReportsCambridge']
    writes = ['houset_karamy.crimeStreetsFrequency']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('houset_karamy', 'houset_karamy')

        crimes = repo['houset_karamy.crimeReportsCambridge'].find()
        
        crimeNeighborhood = []
        for neighborhood in crimes:
            crimeNeighborhood.append(neighborhood['neighborhood'])

        crimeCounts = []
        for crime in crimeNeighborhood:
            crimeCounts.append((crimeNeighborhood.count(crime), crime))

        finalCount = []
        for crime in crimeCounts:
            finalCount.append({'crime':crime[1], 'count': crime[0]})

        repo['houset_karamy.crimeFrequencyCambridge'].insert_many(finalCount)

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
        doc.add_namespace('cma', 'https://data.cambridgema.gov/resource/')   

        this_script = doc.agent("alg:houset_karamy#example", {prov.model.PROV_TYPE:prov.model.PROV["SoftwareAgent"], "ont:Extension":"py"})
        resource = doc.entity('cma:dypy-nwuz', {'prov:label':'Crime Frequency Cambridge', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_CrimeFrequencyCambridge = doc.activity("log:uuid"+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_CrimeFrequencyCambridge, this_script)

        doc.usage(get_CrimeFrequencyCambridge, resource, startTime, None,
                  {prov.model.PROV_TYPE:"ont:Retrieval"#,
                  #"ont:Query":"?type=Animal+fld_crime&$select=type,latitude,longitude,OPEN_DT"
                  }
                  )

        crimeFrequencyCambridge = doc.entity("dat:houset_karamy#crimeFrequencyCambridge", {prov.model.PROV_LABEL:"crimeFrequencyCambridge", prov.model.PROV_TYPE:"ont:DataSet"})
        doc.wasAttributedTo(crimeFrequencyCambridge, this_script)
        doc.wasGeneratedBy(crimeFrequencyCambridge, get_CrimeFrequencyCambridge, endTime)
        doc.wasDerivedFrom(crimeFrequencyCambridge, resource, get_CrimeFrequencyCambridge, get_CrimeFrequencyCambridge, get_CrimeFrequencyCambridge)

        repo.logout()
                  
        return doc

crimeFrequencyCambridge.execute()
doc = transformation1.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


## eof