import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math
import re 

__author__ = 'Ann Ming Samborski'

class zipEnergyUse(dml.Algorithm):
    contributor = 'asambors_maxzm'
    reads = ['asambors_maxzm.zipcodetolatlong',
             'asambors_maxzm.energywater',
             'asambors_maxzm.hospitals']
    writes = ['asambors_maxzm.zipenergyuse']

    @staticmethod
    def provides_care(property_type):
            recognized = ['Urgent Care/Clinic/Other Outpatient',
                          'Hospital (General Medical & Surgical)',
                          'Other - Specialty Hospital']
            return property_type['property_type'] in recognized

    @staticmethod
    def select(R, s):
    	return [t for t in R if s(t)] 

    @staticmethod
    def aggregate(R, f):
    	# (key, everything else)
    	keys = {r[0] for r in R}
    	return [(key, f([v for (k,v) in R if k == key])) for key in keys]

    @staticmethod
    def project(R, p):
    	return [p(t) for t in R]

    @staticmethod
    def filter_hospital_attr(hospital):
    	return {'name': hospital['name'],
    			'local':hospital['local']}

    @staticmethod
    def has_metrics(hospital):
    	if hospital['site_energy_use'] != 'Not Available':
    		return True 

    @staticmethod
    def product(R, S):
    	return [(t,u) for t in R for u in S]

    @staticmethod
    def execute(): #trial = False):
    	startTime = datetime.datetime.now()

    	# Establish the date base connection
    	client = dml.pymongo.MongoClient()
    	repo = client.repo
    	repo.authenticate('asambors_maxzm','asambors_maxzm')

    	# Load data
    	zipcodetolatlong = repo['asambors_maxzm.zipcodetolatlong']
    	energywater = repo['asambors_maxzm.energywater']
    	hospitals = repo['asambors_maxzm.hospitals']

    	# Grab entries in energy data set
    	retrieveEnergyWater = energywater.find({},{'_id': False})

    	# Retrieve only properties that are care providers
    	properties = zipEnergyUse.select(retrieveEnergyWater, zipEnergyUse.provides_care)
    	with_metrics_properties = zipEnergyUse.select(properties, zipEnergyUse.has_metrics)

    	# Transform so key is zip code 
    	projected_properties = zipEnergyUse.project(with_metrics_properties, lambda h: (h['zip'], int(re.sub("[^\d\.]", "", h['site_energy_use']))))
    	# print(projected_properties)

    	# Aggregate by zipcode to receive sum of energy used
    	agg_properties = zipEnergyUse.aggregate(projected_properties, sum)

    	final_energy = zipEnergyUse.project(agg_properties, lambda h: {'zip_code': h[0], 'energy': h[1]})
    	# print(final_energy)

    	# Grab entries in hospitals data set
    	retrieveHospitals = hospitals.find({}, {'_id': False})

    	# Group by zip code
    	# Remove all entries except location_zip, neighborhood, name 
    	filtered_hospitals = zipEnergyUse.project(retrieveHospitals, lambda h: {'zip_code': h['zipcode'], 'neigh': h['neigh'], 'name': h['name']})
    	# print(filtered_hospitals)

    	# Then a product of the two
    	initial_combined = zipEnergyUse.product(final_energy, filtered_hospitals)

    	# Select if zipcodes match
    	aggregate_on_zip = zipEnergyUse.select(initial_combined, lambda t: int(t[0]['zip_code']) == int(t[1]['zip_code']))

    	# Combine into one dictionary
    	hospital_to_energy = zipEnergyUse.project(aggregate_on_zip, lambda x: {**x[0], **x[1]})
    	
    	for i in hospital_to_energy:
    		print(i)

    	repo.dropCollection("zipenergyuse")
    	repo.createCollection("zipenergyuse")
    	repo['asambors_maxzm.zipenergyuse'].insert_many(hospital_to_energy)
    	repo['asambors_maxzm.zipenergyuse'].metadata({'complete':True})

    	print("THE DATA IS UPLOADED.")

    	endTime = datetime.datetime.now
    	return {"start":startTime,"end":endTime}


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
        repo.authenticate('asambors_maxzm', 'asambors_maxzm')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        # ADD THREE DATA SOURCES
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/') # Boston Data Portal
        doc.add_namespace('datm', 'http://datamechanics.io/data/') # datamechanics.io
        doc.add_namespace('cdc', 'https://chronicdata.cdc.gov/resource/') # CDC Data Portal

        this_script = doc.agent('alg:asambors_maxzm#fetchData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # BOSTON DATA PORTAL
        hospitals_resource = doc.entity('bdp:u6fv-m8v4', {'prov:label':'Hospital Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        energy_resource = doc.entity('bdp:vxhe-ma3y', {'prov:label':'Building Energy and Water Use Metrics', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        # CDC DATA
        sleep_resource = doc.entity('cdc:eqbn-8mpz', {'prov:label':'Sleeping less than 7 hours among adults aged >=18 years', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        # DATAMECHANICS.IO DATA
        zip_to_income_resource = doc.entity('datm:asambors_maxzm', {'prov:label':'Zip code to estimated income', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        lat_to_zip_resource = doc.entity('datm:asambors_maxzm', {'prov:label':'Latitude, longitude to zip code', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_hospitals = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_energy = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)  
        get_sleep = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime) 
        get_zip_to_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)  
        get_lat_to_zip = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime) 

        doc.wasAssociatedWith(get_hospitals, this_script)
        doc.wasAssociatedWith(get_energy, this_script)
        doc.wasAssociatedWith(get_sleep, this_script)
        doc.wasAssociatedWith(get_zip_to_income, this_script)
        doc.wasAssociatedWith(get_lat_to_zip, this_script)

        doc.usage(get_hospitals, hospitals_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_energy, energy_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_sleep, sleep_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_zip_to_income, zip_to_income_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_lat_to_zip, lat_to_zip_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        Hospitals = doc.entity('dat:asambors_maxzm#hospitals', {prov.model.PROV_LABEL:'Hospital Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Hospitals, this_script)
        doc.wasGeneratedBy(Hospitals, get_hospitals, endTime)
        doc.wasDerivedFrom(Hospitals, hospitals_resource, get_hospitals, get_hospitals, get_hospitals)

        Energy = doc.entity('dat:asambors_maxzm#energywater', {prov.model.PROV_LABEL:'Building Energy and Water Use Metrics', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Energy, this_script)
        doc.wasGeneratedBy(Energy, get_energy, endTime)
        doc.wasDerivedFrom(Energy, energy_resource, get_energy, get_energy, get_energy)

        Sleep = doc.entity('dat:asambors_maxzm#nosleepma', {prov.model.PROV_LABEL:'Sleeping less than 7 hours among adults aged >=18 years', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Sleep, this_script)
        doc.wasGeneratedBy(Sleep, get_sleep, endTime)
        doc.wasDerivedFrom(Sleep, sleep_resource, get_sleep, get_sleep, get_sleep)

        ZipToIncome = doc.entity('dat:asambors_maxzm#ziptoincome', {prov.model.PROV_LABEL:'Zip code to estimated income', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(ZipToIncome, this_script)
        doc.wasGeneratedBy(ZipToIncome, get_zip_to_income, endTime)
        doc.wasDerivedFrom(ZipToIncome, zip_to_income_resource, get_zip_to_income, get_zip_to_income, get_zip_to_income)

        ZipcodeToLatLong = doc.entity('dat:asambors_maxzm#zipcodetolatlong', {prov.model.PROV_LABEL:'Latitude, longitude to zip code', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(ZipcodeToLatLong, this_script)
        doc.wasGeneratedBy(ZipcodeToLatLong, get_lat_to_zip, endTime)
        doc.wasDerivedFrom(ZipcodeToLatLong, lat_to_zip_resource, get_lat_to_zip, get_lat_to_zip, get_lat_to_zip)

        repo.logout()
                  
        return doc


zipEnergyUse.execute()

