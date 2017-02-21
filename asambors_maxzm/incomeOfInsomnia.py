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



            #fucky data fixing
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

            print("data is uploaded")


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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
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


incomeOfInsomnia.execute()
