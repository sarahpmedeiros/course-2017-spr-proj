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


        # import crime data
        #client1 = sodapy.Socrata("data.cityofboston.gov", None)
        #response1 = client1.get("crime")
        #s = json.dumps(response1, sort_keys=True, indent=2)
        
            
        #crime_month = project(response1, lambda x: (x["year"], x["month"]))
        #crime_month2015 = [crime_2015month for crime_2015month in crime_month if crime_2015month[0] == "2015"]
        #crime12 = [crime12_2015 for crime12_2015 in crime_month2015 if crime12_2015[1] == '1' or crime12_2015[1] == '2']
        #print("The crime number happens in Jan and Feb in 2015 is: ", len(crime12))
        #crime34 = [crime34_2015 for crime34_2015 in crime_month2015 if crime34_2015[1] == '3' or crime34_2015[1] == '4']
        #print("The crime number happens in Mar and Apr in 2015 is: ", len(crime34))
        #crime56 = [crime56_2015 for crime56_2015 in crime_month2015 if crime56_2015[1] == '5' or crime56_2015[1] == '6']
        #print("The crime number happens in May and Jun in 2015 is: ", len(crime56))
        #crime78 = [crime78_2015 for crime78_2015 in crime_month2015 if crime78_2015[1] == '7' or crime78_2015[1] == '8']
        #print("The crime number happens in Jul and Aug in 2015 is: ", len(crime78))

        crimes = repo['houset_karamy.crimeReportsBoston'].find()
        
        crimeTypes = []
        for crime in crimes:
            crimeTypes.append(crime["reptdistrict"])
            
        crimeCounts = []
        for crime in crimeTypes:
            crimeCounts.append((crimeTypes.count(crime), crime))
        
        finalCount = []
        for crime in crimeCounts:
            finalCount.append({'crime': crime[1], 'count': crime[0]})
        
        repo['houset_karamy.transformation1'].insert_many(finalCount)
        
        #repo["houset_karamy.transformation1"].insert_many(response1)
        #repo["houset_karamy.transformation1"].metadata({"complete":True})
        #print(repo["houset_karamy.transformation1"].metadata())

        #print("The percentage of fld complaint / crime in Jan and Feb is: ", len(fld12) / len(crime12))
        #print("The percentage of fld complaint / crime in Mar and Apr is: ", len(fld34) / len(crime34))
        #print("The percentage of fld complaint / crime in May and Jun is: ", len(fld56) / len(crime56))
        #print("The percentage of fld complaint / crime in Jul and Aug is: ", len(fld78) / len(crime78))


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

        fld_crime = doc.entity("dat:houset_karamy#transformation1", {prov.model.PROV_LABEL:"transformation1", prov.model.PROV_TYPE:"ont:DataSet"})
        doc.wasAttributedTo(transformation1, this_script)
        doc.wasGeneratedBy(transformation1, get_transformation1, endTime)
        doc.wasDerivedFrom(transformation1, resource, get_transformation1, get_transformation1, get_transformation1)

        repo.logout()
                  
        return doc

transformation1.execute()
doc = transformation1.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
