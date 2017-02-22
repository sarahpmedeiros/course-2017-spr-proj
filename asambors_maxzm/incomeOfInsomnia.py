import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import math

class incomeOfInsomnia(dml.Algorithm):
    contributor = 'asambors_maxzm'
    reads = ['asambors_maxzm.nosleep','asambors_maxzm.ziptoincome','asambors_maxzm.zipcodetolatlong']
    writes = ['asambors_maxzm.incomeofinsomnia']

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

    def pickCloserZip(attemptAtZip):
            closestDist = -1
            closestObj = None
            if attemptAtZip[0][0]['uniqueid']=='59':
                return None #handles the first 2 bs data entries in the dataset
            for combo in attemptAtZip:
                    lat1 = float(combo[0]['geolocation']['latitude'])
                    lat2 = float(combo[1]['lat'])
                    long1 = float(combo[0]['geolocation']['longitude'])
                    long2 = float(combo[1]['long'])
                    distApart = math.sqrt((lat2-lat1)**2+(long2-long1)**2)
                    #distance formula
                    if closestDist == -1 or distApart<closestDist:
                            closestDist = distApart
                            closestObj = combo
            return combo 


    @staticmethod
    def execute(trial=False):
            startTime = datetime.datetime.now()

            #set up the datebase connection
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('asambors_maxzm','asambors_maxzm')

            #loads
            nosleep = repo['asambors_maxzm.nosleepma']
            ziptoincome = repo['asambors_maxzm.ziptoincome']
            zipcodetolatlong = repo['asambors_maxzm.zipcodetolatlong']
            
            #run select on the zip lat long dataset so that its only zip codes that are near boston
            selectedZipToLatLong = incomeOfInsomnia.select(zipcodetolatlong.find({},{'_id': False}), incomeOfInsomnia.zipIsNearBoston) 

            #run the same select on zip code to income
            selectedZipToIncome = incomeOfInsomnia.select(ziptoincome.find({},{'_id': False}), incomeOfInsomnia.zipIsNearBoston) 

            #now it is time to map the lat long zip codes to money
            #map all together
            allIncomeLatLongPairs = incomeOfInsomnia.product(selectedZipToLatLong,selectedZipToIncome)

            #now select right ones in a reduce style fashion
            onlyRealIncomeLatLongPairs = incomeOfInsomnia.select(allIncomeLatLongPairs,lambda t: int(t[0]['zip_code'])==int(t[1]['zip_code']))
            zipLatLongIncome = incomeOfInsomnia.project(onlyRealIncomeLatLongPairs, lambda t: {**t[0],**t[1]})
            
            #map each no sleep person to corosponding zip based on lat long  (this will also map to income because we mapped income to zip)
            #product
            allCombos = incomeOfInsomnia.product(nosleep.find({},{'_id': False}),zipLatLongIncome)

            #project each keys lat and long to be coppied as part of the value
            projectedCombos = incomeOfInsomnia.project(allCombos, lambda t: (t[0]['uniqueid'],(t[0],t[1])))

            #agregate
            aggregatedData = incomeOfInsomnia.aggregate(projectedCombos,incomeOfInsomnia.pickCloserZip)

            #last project to proper form
            #Some data adjustment 
            incomeOfInsomniaData = []
            for (a,b) in aggregatedData:
                    try:
                            b[0].update(b[1])
                            incomeOfInsomniaData.append(b[0])
                    except TypeError:
                            print((a,b))

            print(incomeOfInsomniaData)

            repo.dropCollection("incomeofinsomnia")
            repo.createCollection("incomeofinsomnia")
            repo['asambors_maxzm.incomeofinsomnia'].insert_many(incomeOfInsomniaData)
            repo['asambors_maxzm.incomeofinsomnia'].metadata({'complete':True})

            print("DATA IS UPLOADED")

            endTime = datetime.datetime.now

            return {"start":startTime,"end":endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None): 
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asambors_maxzm', 'asambors_maxzm')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        
        # ADD CDC DATA SOURCE
        doc.add_namespace('cdc', 'https://chronicdata.cdc.gov/resource/') # CDC Data Portal

        this_script = doc.agent('alg:asambors_maxzm#incomeOfInsomnia', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # CDC DATA
        sleep_resource = doc.entity('cdc:eqbn-8mpz', {'prov:label':'Sleeping less than 7 hours among adults aged >=18 years', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        # DATAMECHANICS.IO DATA
        zip_to_income_resource = doc.entity('dat:asambors_maxzm', {'prov:label':'Zip code to estimated income', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        lat_to_zip_resource = doc.entity('dat:asambors_maxzm', {'prov:label':'Latitude, longitude to zip code', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        income_of_insomnia_resource = doc.entity('dat:asambors_maxzm', {'prov:label':'What is the income of those who sleep less than 7 hours a night', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_sleep = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime) 
        get_zip_to_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)  
        get_lat_to_zip = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime) 
        get_income_of_insomnia = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_sleep, this_script)
        doc.wasAssociatedWith(get_zip_to_income, this_script)
        doc.wasAssociatedWith(get_lat_to_zip, this_script) 

        doc.usage(get_sleep, sleep_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_zip_to_income, zip_to_income_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_lat_to_zip, lat_to_zip_resource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

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

        IncomeOfInsomnia = doc.entity('dat:asambors_maxzm#incomeofinsomnia', {prov.model.PROV_LABEL:'What is the income of those who sleep less than 7 hours a night', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(IncomeOfInsomnia, this_script)
        doc.wasGeneratedBy(IncomeOfInsomnia, get_income_of_insomnia, endTime)
        doc.wasDerivedFrom(IncomeOfInsomnia, income_of_insomnia_resource, get_income_of_insomnia, get_income_of_insomnia, get_income_of_insomnia)

        repo.logout()
                  
        return doc


incomeOfInsomnia.execute()
doc = incomeOfInsomnia.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
