import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class retrieve(dml.Algorithm):
    contributor = 'hschurma_rcalleja'
    reads = []
    writes = ['hschurma_rcalleja.funding', 'hschurma_rcalleja.location', 'hschurma_rcalleja.graduation', 'hschurma_rcalleja.SAT', 'hschurma_rcalleja.gradrates']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hschurma_rcalleja', 'hschurma_rcalleja')
        
        url = 'https://data.cityofboston.gov/api/views/e29s-ympv/rows.json?accessType=DOWNLOAD'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("location")
        repo.createCollection("location")
        repo['hschurma_rcalleja.location'].insert_one(r)


        url = 'http://datamechanics.io/data/hshurma_rcalleja/graddata.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("graduation")
        repo.createCollection("graduation")
        repo['hschurma_rcalleja.graduation'].insert_one(r)

        url = 'http://datamechanics.io/data/hshurma_rcalleja/funding.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("funding")
        repo.createCollection("funding")
        repo['hschurma_rcalleja.funding'].insert_many(r)

        url = 'http://datamechanics.io/data/hshurma_rcalleja/SAT2014.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("SAT")
        repo.createCollection("SAT")
        repo['hschurma_rcalleja.SAT'].insert_many(r)

        url = 'http://datamechanics.io/data/hshurma_rcalleja/gradrates06_16.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("gradrates")
        repo.createCollection("gradrates")
        repo['hschurma_rcalleja.gradrates'].insert_many(r)


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.'''
        

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hschurma_rcalleja', 'hschurma_rcalleja')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:hschurma_rcalleja#retrieve', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_loc = doc.entity('bdp:e29s-ympv', {'prov:label':'Boston Public Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_grad = doc.entity('bdp:wgrya-vhq5', {'prov:label':'Graduates Attending College', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_SAT = doc.entity('dat:hschurma_rcalleja/SAT2014.json', {'prov:label':'BPS SAT Scores', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_g_rates = doc.entity('dat:hschurma_rcalleja/gradrates06_16', {'prov:label':'BPS Graduation Rates', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_fund = doc.entity('dat:hschurma_rcalleja/funding.json', {'prov:label':'BPS Funding', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_loc = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_grad = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_SAT = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_g_rates = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_fund = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_loc, this_script)
        doc.wasAssociatedWith(get_grad, this_script)
        doc.wasAssociatedWith(get_SAT, this_script)
        doc.wasAssociatedWith(get_g_rates, this_script)
        doc.wasAssociatedWith(get_fund, this_script)

        doc.usage(get_loc, resource_loc, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        
        doc.usage(get_grad, resource_grad, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?graduating_year=2012&district_name=Boston'
                  }
                  )
        
        doc.usage(get_SAT, resource_SAT, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )
        
        doc.usage(get_g_rates, resource_g_rates, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        doc.usage(get_fund, resource_fund, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        loc = doc.entity('dat:hschurma_rcalleja#loc', {prov.model.PROV_LABEL:'Boston Public Schools', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(loc, this_script)
        doc.wasGeneratedBy(loc, get_loc, endTime)
        doc.wasDerivedFrom(loc, resource_loc, get_loc, get_loc, get_loc)

        grad = doc.entity('dat:hschurma_rcalleja#grad', {prov.model.PROV_LABEL:'Graduates Attending College', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(grad, this_script)
        doc.wasGeneratedBy(grad, get_grad, endTime)
        doc.wasDerivedFrom(grad, resource_grad, get_grad, get_grad, get_grad)

        SAT = doc.entity('dat:hschurma_rcalleja#SAT', {prov.model.PROV_LABEL:'BPS SAT Scores', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(SAT, this_script)
        doc.wasGeneratedBy(SAT, get_SAT, endTime)
        doc.wasDerivedFrom(SAT, resource_SAT, get_SAT, get_SAT, get_SAT)

        g_rates = doc.entity('dat:hschurma_rcalleja#g_rates', {prov.model.PROV_LABEL:'BPS Graduation Rates', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(g_rates, this_script)
        doc.wasGeneratedBy(g_rates, get_g_rates, endTime)
        doc.wasDerivedFrom(g_rates, resource_g_rates, get_g_rates, get_g_rates, get_g_rates)

        fund = doc.entity('dat:hschurma_rcalleja#fund', {prov.model.PROV_LABEL:'BPS Funding', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(fund, this_script)
        doc.wasGeneratedBy(fund, get_fund, endTime)
        doc.wasDerivedFrom(fund, resource_fund, get_fund, get_fund, get_fund)

        #repo.record(doc.serialize())
        repo.logout()
                  
        return doc

retrieve.execute()
doc = retrieve.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
