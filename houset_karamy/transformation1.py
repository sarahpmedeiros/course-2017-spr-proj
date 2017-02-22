import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

class transformation1(dml.Algorithm):
    contributor = "houset_karamy"
    reads = ["houset_karamy.crimeReportsBoston"]
    writes = ["houset_karamy.transformation1"]

    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate("houset_karamy", "houset_karamy")
        repo.dropCollection("transformation1")
        repo.createCollection("transformation1")


        #import data we're using
        crimes = repo['houset_karamy.crimeReportsBoston'].find()
        
        #get the different districts
        crimeDist = []
        for district in crimes:
            crimeDist.append(district["reptdistrict"])
            
        #count the number of crimes in each district
        crimeCounts = []
        for crime in crimeDist:
            crimeCounts.append((crimeDist.count(crime), crime))
        
        #get the final count for each in dictionary form
        totalCount = []
        for crime in crimeCounts:
            totalCount.append({'district': crime[1], 'count': crime[0]})
        
        #get rid of duplicates
        #insert into new database
        finalCount = []
        finalCountS = set(totalCount)
        finalCount = dict(finalCountS)
        
        
                
        repo['houset_karamy.transformation1'].insert_many(finalCount)
        
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
        resource = doc.entity("bdp:crime", {"prov:label":"Transformation1", prov.model.PROV_TYPE:"ont:DataResource", "ont:Extension":"json"})
        get_transformation1 = doc.activity("log:uuid"+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_transformation1, this_script)

        doc.usage(get_transformation1, resource, startTime, None,
                  {prov.model.PROV_TYPE:"ont:Retrieval"#,
                  #"ont:Query":"?type=Animal+fld_crime&$select=type,latitude,longitude,OPEN_DT"
                  }
                  )

        transformation1 = doc.entity("dat:houset_karamy#transformation1", {prov.model.PROV_LABEL:"transformation1", prov.model.PROV_TYPE:"ont:DataSet"})
        doc.wasAttributedTo(transformation1, this_script)
        doc.wasGeneratedBy(transformation1, get_transformation1, endTime)
        doc.wasDerivedFrom(transformation1, resource, get_transformation1, get_transformation1, get_transformation1)

        repo.logout()
                  
        return doc

transformation1.execute()
doc = transformation1.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
