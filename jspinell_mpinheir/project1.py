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
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jspinell_mpinheir', 'jspinell_mpinheir')
            
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('ageRanges','http://datamechanics.io/data/jspinell_mpinheir/AgeRanges.json') 
        doc.add_namespace('crimeRate','http://datamechanics.io/data/jspinell_mpinheir/CrimeRate.json')
        doc.add_namespace('housingRates','http://datamechanics.io/data/jspinell_mpinheir/HousingRates.json')
        doc.add_namespace('educationCosts','http://datamechanics.io/data/jspinell_mpinheir/EducationCosts.json')
        doc.add_namespace('neighborhoods','http://datamechanics.io/data/jspinell_mpinheir/neighborhoods.json')
        
    
    
        this_script = doc.agent('alg:jspinell_mpinheir#project1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
    
           
        ageRanges_resource = doc.entity('http://datamechanics.io/data/jspinell_mpinheir/AgeRanges.json', {'prov:label':'Population by Age Ranges', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        crimeRates_resource = doc.entity('http://datamechanics.io/data/jspinell_mpinheir/CrimeRate.json', {'prov:label':'Crime Rates by Zip Code', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        housingRates_resource = doc.entity('http://datamechanics.io/data/jspinell_mpinheir/HousingRates.json', {'prov:label':'Housing Rates by Zip Code', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        educationCosts_resource = doc.entity('http://datamechanics.io/data/jspinell_mpinheir/EducationCosts.json', {'prov:label':'Education Data by Zip Code', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        neighborhoods_resource = doc.entity('http://datamechanics.io/data/jspinell_mpinheir/neighborhoods.json', {'prov:label':'Neighborhoods to Zip Codes', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
    
            
        this_ageRanges = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        this_crimeRates = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        this_housingRates = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        this_educationCosts = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        this_neighborhoods = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
    
           
        doc.wasAssociatedWith(this_ageRanges, this_script)
        doc.wasAssociatedWith(this_crimeRates, this_script)
        doc.wasAssociatedWith(this_housingRates, this_script)
        doc.wasAssociatedWith(this_educationCosts, this_script)
        doc.wasAssociatedWith(this_neighborhoods, this_script)
        
    
           
        doc.usage(this_ageRanges, ageRanges_resource,startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_crimeRates, crimeRates_resource,startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_housingRates, housingRates_resource,startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_educationCosts, educationCosts_resource,startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(this_neighborhoods, neighborhoods_resource,startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        
    
        ageRanges = doc.entity('dat:jspinell_mpinheir#ageRanges', {prov.model.PROV_LABEL:'Population by Age Ranges', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(ageRanges, this_script)
        doc.wasGeneratedBy(ageRanges, this_ageRanges, endTime)
        doc.wasDerivedFrom(ageRanges, ageRanges_resource, this_ageRanges,this_ageRanges, this_ageRanges)
    
        crimeRates = doc.entity('dat:jspinell_mpinheir#crimeRates', {prov.model.PROV_LABEL:'Crime Rates by Zip Code', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimeRates, this_script)
        doc.wasGeneratedBy(crimeRates, this_crimeRates, endTime)
        doc.wasDerivedFrom(crimeRates, crimeRates_resource, this_crimeRates,this_crimeRates, this_crimeRates)
    
        housingRates = doc.entity('dat:jspinell_mpinheir#housingRates', {prov.model.PROV_LABEL:'Housing Rates by Zip Code', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(housingRates, this_script)
        doc.wasGeneratedBy(housingRates, this_housingRates, endTime)
        doc.wasDerivedFrom(housingRates, housingRates_resource, this_housingRates,this_housingRates, this_housingRates)
    
        educationCosts = doc.entity('dat:jspinell_mpinheir#educationCosts', {prov.model.PROV_LABEL:'Education Data by Zip Code', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(educationCosts, this_script)
        doc.wasGeneratedBy(educationCosts, this_educationCosts, endTime)
        doc.wasDerivedFrom(educationCosts, educationCosts_resource, this_educationCosts,this_educationCosts, this_educationCosts)
    
        neighborhoods = doc.entity('dat:jspinell_mpinheir#neighborhoods', {prov.model.PROV_LABEL:'Neighborhoods to Zip Codes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhoods, this_script)
        doc.wasGeneratedBy(neighborhoods, this_neighborhoods, endTime)
        doc.wasDerivedFrom(neighborhoods, neighborhoods_resource, this_neighborhoods,this_neighborhoods, this_neighborhoods)
    
      
        #repo.record(doc.serialize())
        
        repo.logout()
    
        return doc

"""
project1.execute()
doc = project1.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
"""
