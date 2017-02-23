import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code
from bson.son import SON

class transformation2(dml.Algorithm):
    contributor = 'tigerlei'
    reads = ['tigerlei.crime','tigerlei.police', 'tigerlei.homicides']
    writes = ['tigerlei.crime_distribution', 'tigerlei.homicides_distribution']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('tigerlei', 'tigerlei')
        print("Transformation 2 starts!") 

        crime_datasets = {'crime', 'homicides'}

        for dataset in crime_datasets: 
            repo.dropCollection(dataset + "_distribution")
            repo.createCollection(dataset + "_distribution")

            # Group crime incidents by district ID
            mapper = Code('''function() {
                emit(this.district,1);
            }''')

            reducer = Code('''function(key,values){
                return Array.sum(values);        
            }''')

            repo['tigerlei.'+ dataset].map_reduce(mapper, reducer, 'tigerlei.'+ dataset+ '_distribution')

            # Add key 'zipcode'
            for item in repo['tigerlei.'+ dataset+ '_distribution'].find():
                repo['tigerlei.'+ dataset+ '_distribution'].update(
                    {'_id': item['_id']},
                    {'$set': {'zipcode': 0}})        
            
            # Extract (district, zipcode) pairs from police
            keys = []
            for item in repo['tigerlei.police'].find():
                temp = {}
                temp = (item['district'], item['zipcode'])
                keys.append(temp)

            # Update 'zipcode' in crime_distribution from keys
            for key in keys:
                for item in repo['tigerlei.'+ dataset+ '_distribution'].find():
                    if item['_id'] == key[0]:
                        repo['tigerlei.'+ dataset+ '_distribution'].update(
                            {'_id': item['_id']},
                            {'$set': {'zipcode': key[1]}})

            # repo['tigerlei.'+ dataset+ '_distribution'].aggregate(
            #     [
            #         {'$sort': SON([('value', -1)])}
            #     ]
            # )
 
        repo.logout()
        print("Transformation 2 has been done!") 
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('tigerlei', 'tigerlei')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:tigerlei#transformation2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)

        crime = doc.entity('dat:tigerlei#crime', {prov.model.PROV_LABEL:'crime incidents 2012-2016', prov.model.PROV_TYPE:'ont:DataSet'})
        police = doc.entity('dat:tigerlei#police', {'prov:label':'police station in boston', prov.model.PROV_TYPE:'ont:DataSet'})
        homicides = doc.entity('dat:tigerlei#homicides', {'prov:label':'homicides in boston', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.usage(this_run, crime, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(this_run, police, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(this_run, homicides, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})        

        crime_distribution = doc.entity('dat:tigerlei#crime_distribution', {prov.model.PROV_LABEL:'crime incidents distribution by district & zipcode ', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime_distribution, this_script)
        doc.wasGeneratedBy(crime_distribution, this_run, endTime)
        doc.wasDerivedFrom(crime_distribution, crime, this_run, this_run, this_run)
        doc.wasDerivedFrom(crime_distribution, police, this_run, this_run, this_run)

        homicides_distribution = doc.entity('dat:tigerlei#homicides_distribution', {prov.model.PROV_LABEL:'homicides distribution by district & zipcode', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(homicides_distribution, this_script)
        doc.wasGeneratedBy(homicides_distribution, this_run, endTime)
        doc.wasDerivedFrom(homicides_distribution, homicides, this_run, this_run, this_run)
        doc.wasDerivedFrom(homicides_distribution, police, this_run, this_run, this_run)

        repo.logout()
                  
        return doc

# transformation2.execute()
# doc = transformation2.provenance()
# print(doc.get_provn())

# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
