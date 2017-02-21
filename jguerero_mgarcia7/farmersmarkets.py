# PROV DONE

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class farmersmarkets(dml.Algorithm):
    contributor = 'jguerero_mgarcia7'
    reads = []
    writes = ['jguerero_mgarcia7.farmersmarkets']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jguerero_mgarcia7', 'jguerero_mgarcia7')

        # Summer Farmers Markets
        url = 'https://data.cityofboston.gov/resource/ckir-e47p.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("farmersmarkets")
        repo.createCollection("farmersmarkets")
        repo['jguerero_mgarcia7.farmersmarkets'].insert_many(r)


        # Winter Farmers Markets
        url = 'https://data.cityofboston.gov/resource/txud-qumr.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        
        repo['jguerero_mgarcia7.farmersmarkets'].insert_many(r)

        repo['jguerero_mgarcia7.farmersmarkets'].metadata({'complete':True})
        print(repo['jguerero_mgarcia7.farmersmarkets'].metadata())

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
        repo.authenticate('jguerero_mgarcia7', 'jguerero_mgarcia7')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jguerero_mgarcia7#farmersmarkets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_wfarmers = doc.entity('bdp:txud-qumr', {'prov:label':'Winter Farmers Markets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_sfarmers = doc.entity('bdp:ckir-e47p', {'prov:label':'Summer Farmers Markets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_farmersmarkets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_farmersmarkets, this_script)
        doc.usage(get_farmersmarkets, resource_wfarmers, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )
        doc.usage(get_farmersmarkets, resource_sfarmers, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        farmersmarkets = doc.entity('dat:jguerero_mgarcia7#farmersmarkets', {prov.model.PROV_LABEL:'farmersmarkets', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(farmersmarkets, this_script)
        doc.wasGeneratedBy(farmersmarkets, get_farmersmarkets, endTime)
        doc.wasDerivedFrom(farmersmarkets, resource_wfarmers, get_farmersmarkets, get_farmersmarkets, get_farmersmarkets)
        doc.wasDerivedFrom(farmersmarkets, resource_sfarmers, get_farmersmarkets, get_farmersmarkets, get_farmersmarkets)



        repo.logout()
                  
        return doc

## eof
