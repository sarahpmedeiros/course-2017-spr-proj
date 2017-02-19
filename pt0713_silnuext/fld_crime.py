import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy


class fld_crime(dml.Algorithm):
    contributor = 'pt0713_silnuext'
    reads = []
    writes = ['pt0713_silnuext.fld_crime']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pt0713_silnuext', 'pt0713_silnuext')
        repo.dropCollection("fld_crime")
        repo.createCollection("fld_crime")





        client = sodapy.Socrata("data.mass.gov", None)
        response = client.get("x99p-b88k")
        #r = json.loads(response)
        s = json.dumps(response, sort_keys=True, indent=2)
        print(s)
        repo['pt0713_silnuext.fld_crime'].insert_many(response)
        repo['pt0713_silnuext.fld_crime'].metadata({'complete':True})
        print(repo['pt0713_silnuext.fld_crime'].metadata())




        client1 = sodapy.Socrata("data.cityofboston.gov", None)
        response1 = client1.get("crime")

        #r = json.loads(response)
        s = json.dumps(response1, sort_keys=True, indent=2)
        print(s)

        repo['pt0713_silnuext.fld_crime'].insert_many(response1)
        repo['pt0713_silnuext.fld_crime'].metadata({'complete':True})
        print(repo['pt0713_silnuext.fld_crime'].metadata())


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
        repo.authenticate('pt0713_silnuext', 'pt0713_silnuext')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:pt0713_silnuext#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_fld_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_fld_crime, this_script)

        doc.usage(get_fld_crime, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+fld_crime&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        fld_crime = doc.entity('dat:pt0713_silnuext#fld_crime', {prov.model.PROV_LABEL:'Animals fld_crime', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(fld_crime, this_script)
        doc.wasGeneratedBy(fld_crime, get_fld_crime, endTime)
        doc.wasDerivedFrom(fld_crime, resource, get_fld_crime, get_fld_crime, get_fld_crime)


        repo.logout()
                  
        return doc

fld_crime.execute()
doc = fld_crime.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
