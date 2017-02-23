import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class project1(dml.Algorithm):
    contributor = 'jspinell_mpinheir'
    reads = []
    writes = ['jspinell_mpinheir.ageRanges',
              'jspinell_mpinheir.crimeRate',
              'jspinell_mpinheir.educationCosts', 
              'jspinell_mpinheir.housingRates',
              'jspinell_mpinheir.neighborhoods']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
    
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jspinell_mpinheir', 'jspinell_mpinheir')
    
        # Begin data collection
        url1 = 'http://datamechanics.io/data/jspinell_mpinheir/AgeRanges.json'
        url2 = 'http://datamechanics.io/data/jspinell_mpinheir/CrimeRate.json'
        url3 = 'http://datamechanics.io/data/jspinell_mpinheir/EducationCosts.json'
        url4 = 'http://datamechanics.io/data/jspinell_mpinheir/HousingRates.json'
        url5 = 'http://datamechanics.io/data/jspinell_mpinheir/neighborhoods.json'
        
        response1 = urllib.request.urlopen(url1).read().decode("utf-8")
        response2 = urllib.request.urlopen(url2).read().decode("utf-8")
        response3 = urllib.request.urlopen(url3).read().decode("utf-8")
        response4 = urllib.request.urlopen(url4).read().decode("utf-8")
        response5 = urllib.request.urlopen(url5).read().decode("utf-8")
        
        ageRanges = json.loads(response1)
        crimeRate = json.loads(response2)
        educationCosts = json.loads(response3)
        housingRates = json.loads(response4)
        neighborhoods = json.loads(response5)
            
        # Add to repo
        repo.dropCollection('ageRanges')
        repo.createCollection('ageRanges')
        repo['jspinell_mpinheir.ageRanges'].insert_many(ageRanges)
        
        repo.dropCollection('crimeRate')
        repo.createCollection('crimeRate')
        repo['jspinell_mpinheir.crimeRate'].insert_many(crimeRate)
        
        repo.dropCollection('educationCosts')
        repo.createCollection('educationCosts')
        repo['jspinell_mpinheir.educationCosts'].insert_many(educationCosts)
        
        repo.dropCollection('housingRates')
        repo.createCollection('housingRates')
        repo['jspinell_mpinheir.housingRates'].insert_many(housingRates)
        
        repo.dropCollection('neighborhoods')
        repo.createCollection('neighborhoods')
        repo['jspinell_mpinheir.neighborhoods'].insert_many(neighborhoods)
        
        # Finished
        repo.logout()
        endTime = datetime.datetime.now()
        return {'start':startTime, 'end':endTime}
        
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
        repo.authenticate('jspinell_mpinheir', 'jspinell_mpinheir')
            
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/') # boston data portal
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/resource/') #cambridge data portal

        this_script = doc.agent('alg:jspinell_mpinheir#project1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

"""
project1.execute()
doc = project1.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
"""