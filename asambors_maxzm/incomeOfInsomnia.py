import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class incomeOfInsomnia(dml.Algorithm):
        contributor = 'asambors_maxzm'
        reads = ['asambors_maxzm.nosleep','asambors_maxzm.ziptoincome','asambors_maxzm.zipcodetolatlong']
        writes = ['asambors_maxzm.incomeofinsomnia']


        def select(R, s):
                return [t for t in R if s(t)]
        @staticmethod
        def execute(trial=False):
                startTime = datetime.datetime.now()

                #set up the datebase connection
                client = dml.pymongo.MongoClient()
                repo = client.repo
                repo.authenticate('asambors_maxzm','asambors_maxzm')

                
                #loads
                nosleep = repo['asambors_maxzm.nosleep']
                ziptoincome = repo['asambors_maxzm.ziptoincome']
                zipcodetolatlong = repo['asambors_maxzm.zipcodetolatlong']

                
                #run select on the zip lat long dataset so that its only zip codes that are near boston
                selectedZipToIncomes = []

                lowestBostonZip = 1840
                highestBostonZip = 2299
                for locationpoint in zipcodetolatlong.find():
                        if(int(locationpoint['zip_code'])>lowestBostonZip and int(locationpoint['zip_code'])<highestBostonZip):
                                selectedZipToIncomes.append(locationpoint)
                

                #map each no sleep person to corosponding zip based on lat long



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
