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

                print(hospitals.find())
                
                allCombos = incomeOfHospitals.product(hospitals.find({},{'_id': False}),ziptoincome)

                #select ones with equal zip codes
                selectedCombos = incomeOfHospitals.select(allCombos,lambda t: t[0]['zipcode']==t[1]['zipcode'])


                #last project to proper form

                incomeOfHospitalData = incomeOfHospitals.project(selectedCombos, lambda x: {**x[0],**x[1]})

                print(incomeOfHospitalData)

                #fucky data fixing
                '''
                incomeOfInsomniaData = []
                for (a,b) in aggregatedData:
                        try:
                                b[0].update(b[1])
                                incomeOfInsomniaData.append(b[0])
                        except TypeError:
                                print((a,b))



                print(incomeOfInsomniaData)
                '''

                repo.dropCollection("incomeofhospitals")
                repo.createCollection("incomeofhospitals")
                repo['asambors_maxzm.incomeofhospitals'].insert_many(incomeOfHospitalData)
                repo['asambors_maxzm.incomeofhospitals'].metadata({'complete':True})

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


incomeOfHospitals.execute()
