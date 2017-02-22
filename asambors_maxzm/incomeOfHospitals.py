import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math

class incomeOfHospitals(dml.Algorithm):
        contributor = 'asambors_maxzm'
        reads = ['asambors_maxzm.hospitals','asambors_maxzm.ziptoincome']
        writes = ['asambors_maxzm.incomeofhospitals']

        def select(R, s):
                return [t for t in R if s(t)]

        def aggregate(R, f):
                keys = {r[0] for r in R}
                return [(key, f([v for (k,v) in R if k == key])) for key in keys]

        def project(R, p):
                return [p(t) for t in R]

        def product(R, S):
                return [(t,u) for t in R for u in S]

        def zipIsNearBoston(locationpoint):
                lowestBostonZip = 1840
                highestBostonZip = 2299
                return int(locationpoint['zip_code'])>lowestBostonZip and int(locationpoint['zip_code'])<highestBostonZip

                
        @staticmethod
        def execute(trial=False):
                startTime = datetime.datetime.now()

                #set up the datebase connection
                client = dml.pymongo.MongoClient()
                repo = client.repo
                repo.authenticate('asambors_maxzm','asambors_maxzm')

                #loads
                hospitals = repo['asambors_maxzm.hospitals']
                ziptoincome = repo['asambors_maxzm.ziptoincome']

                #run the select on zip code to income (boston zip codes)
                selectedZipToIncome = incomeOfHospitals.select(ziptoincome.find({},{'_id': False}), incomeOfHospitals.zipIsNearBoston) 

                #map all the hospitals to the proper income
                #product
                allCombos = incomeOfHospitals.product(hospitals.find({},{'_id': False}),selectedZipToIncome)
                print(allCombos)

                #select ones with equal zip codes
                selectedCombos = incomeOfHospitals.select(allCombos,lambda t: int(t[0]['location_zip'])==int(t[1]['zip_code']))

                #last project to proper form
                incomeOfHospitalData = incomeOfHospitals.project(selectedCombos, lambda x: {**x[0],**x[1]})
                print(incomeOfHospitalData)

                # FUCK-Y
                repo.dropCollection("incomeofhospitals")
                repo.createCollection("incomeofhospitals")
                repo['asambors_maxzm.incomeofhospitals'].insert_many(incomeOfHospitalData)
                repo['asambors_maxzm.incomeofhospitals'].metadata({'complete':True})

                print("DATA IS UPLOADED")

                endTime = datetime.datetime.now

                return {"start":startTime,"end":endTime}


        @staticmethod
        def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
            # reads = ['asambors_maxzm.hospitals','asambors_maxzm.ziptoincome']
            # writes = ['asambors_maxzm.incomeofhospitals']

            # Set up the database connection.
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('asambors_maxzm', 'asambors_maxzm')

            doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
            doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
            doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
            doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

            this_script = doc.agent('alg:asambors_maxzm#incomeOfHospitals', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

            # DATAMECHANICS.IO DATA
            zip_to_income_resource = doc.entity('dat:asambors_maxzm', {'prov:label':'Zip code to estimated income', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
            lat_to_zip_resource = doc.entity('dat:asambors_maxzm', {'prov:label':'Latitude, longitude to zip code', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
            income_of_hospitals_resource = doc.entity('dat:asambors_maxzm', {'prov:label':'What is the income of employees of hospitals in a particular zipcode', prov.model.PROV_TYPE:'ont:DataResource'})

            get_zip_to_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)  
            get_lat_to_zip = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime) 
            get_income_of_hospitals = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

            doc.wasAssociatedWith(get_zip_to_income, this_script)
            doc.wasAssociatedWith(get_lat_to_zip, this_script) 
            doc.wasAssociatedWith(get_income_of_hospitals, this_script)

            doc.usage(get_zip_to_income, zip_to_income_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
            doc.usage(get_lat_to_zip, lat_to_zip_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
            doc.usage(get_income_of_hospitals, income_of_hospitals_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

            ZipToIncome = doc.entity('dat:asambors_maxzm#ziptoincome', {prov.model.PROV_LABEL:'Zip code to estimated income', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(ZipToIncome, this_script)
            doc.wasGeneratedBy(ZipToIncome, get_zip_to_income, endTime)
            doc.wasDerivedFrom(ZipToIncome, zip_to_income_resource, get_zip_to_income, get_zip_to_income, get_zip_to_income)

            ZipcodeToLatLong = doc.entity('dat:asambors_maxzm#zipcodetolatlong', {prov.model.PROV_LABEL:'Latitude, longitude to zip code', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(ZipcodeToLatLong, this_script)
            doc.wasGeneratedBy(ZipcodeToLatLong, get_lat_to_zip, endTime)
            doc.wasDerivedFrom(ZipcodeToLatLong, lat_to_zip_resource, get_lat_to_zip, get_lat_to_zip, get_lat_to_zip)

            IncomeOfHospitals = doc.entity('dat:asambors_maxzm#incomeofhospitals', {prov.model.PROV_LABEL:'What is the income of employees of hospitals in a particular zipcode', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(IncomeOfHospitals, this_script)
            doc.wasGeneratedBy(IncomeOfHospitals, get_income_of_hospitals, endTime)
            doc.wasDerivedFrom(IncomeOfHospitals, income_of_hospitals_resource, get_income_of_hospitals, get_income_of_hospitals, get_income_of_hospitals)

            repo.logout()
                      
            return doc


incomeOfHospitals.execute()
doc = incomeOfHospitals.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
