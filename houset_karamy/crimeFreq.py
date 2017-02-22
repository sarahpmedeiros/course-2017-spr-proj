import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

class crimeFreq(dml.Algorithm):
    contributor = "houset_karamy"
    reads = ["houset_karamy.crimeReportsBoston", "houset_karamy.crimeReportsCambridge"]
    writes = ["houset_karamy.crimeFreq"]

    
    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate("houset_karamy", "houset_karamy")
        repo.dropCollection("crimeFreq")
        repo.createCollection("crimeFreq")


        #import data we're using
        crimesBoston = repo['houset_karamy.crimeReportsBoston'].find()
        crimesCambridge = repo['houset_karamy.crimeReportsCambridge'].find()
        
        #get the different districts
        crimeDist = []
        # crimedist2 = []
        for district in crimesBoston:
            crimeDist.append("Boston", district["reptdistrict"])

        for neighborhood in crimesCambridge:
            crimeDist.append("Cambridge", neighborhood["neighborhood"])
            
        #count the number of crimesBoston in each district
        crimeCounts = []
        for crime in crimeDist:
            crimeCounts.append((crimeDist.count(crime), crime))

        # for crime in crimeDist:
        #     crimeCounts.append((crimeDist.count(crime), crime))
        
        #get the final count for each in dictionary form
        totalCount = []
        for crime in crimeCounts:
            if (crime not in totalCount):
                totalCount.append({'district/neighborhood': crime[1], 'count': crime[0]})
        
        #get rid of duplicates
        finalCount = []
        for d in totalCount:
            if (d not in finalCount):
                finalCount.append(d)
        
        print(finalCount)
        #insert into new database        
        repo['houset_karamy.crimeFreq'].insert_many(totalCount)
        
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

        this_script = doc.agent("alg:houset_karamy#example", {prov.model.PROV_TYPE:prov.model.PROV["SoftwareAgent"], "ont:Extension":"py"})
        resource = doc.entity("bdp:crime", {"prov:label":"crimeFreq", prov.model.PROV_TYPE:"ont:DataResource", "ont:Extension":"json"})
        get_crimeFreq = doc.activity("log:uuid"+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_crimeFreq, this_script)

        doc.usage(get_crimeFreq, resource, startTime, None,
                  {prov.model.PROV_TYPE:"ont:Retrieval"#,
                  #"ont:Query":"?type=Animal+fld_crime&$select=type,latitude,longitude,OPEN_DT"
                  }
                  )

        crimeFreq = doc.entity("dat:houset_karamy#crimeFreq", {prov.model.PROV_LABEL:"crimeFreq", prov.model.PROV_TYPE:"ont:DataSet"})
        doc.wasAttributedTo(crimeFreq, this_script)
        doc.wasGeneratedBy(crimeFreq, get_crimeFreq, endTime)
        doc.wasDerivedFrom(crimeFreq, resource, get_crimeFreq, get_crimeFreq, get_crimeFreq)

        repo.logout()
                  
        return doc

crimeFreq.execute()
doc = crimeFreq.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
