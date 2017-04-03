import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code

class transformation3(dml.Algorithm):
    contributor = 'tigerlei'
    reads = ['tigerlei.university', 'tigerlei.police']
    writes = ['tigerlei.neighborhood']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('tigerlei', 'tigerlei')
        repo.dropCollection('neighborhood')
        repo.createCollection('neighborhood')
        print("Transformation 3 starts!") 

        # Transform json to Geojson
        for item in repo['tigerlei.police'].find():
            repo['tigerlei.police'].update(
                {'_id': item['_id']},
                {'$set': 
                    {
                        'geometry.coordinates': item['coordinates'],
                        "geometry.type": "Point",
                        "properties.district": item['district'],
                        "properties.zipcode": item['zipcode'],
                        "properties.type": "Feature"
                    } 
                })

        for item in repo['tigerlei.university'].find():
            repo['tigerlei.university'].update(
                {'_id': item['_id']},
                {'$set': 
                    {
                        'geometry.coordinates': item['coordinates'],
                        "geometry.type": "Point",
                        "properties.name": item['name'],
                        "properties.zipcode": item['zipcode'],
                        "properties.type": "Feature"
                    } 
                })
        # Add geosphere index
        repo['tigerlei.university'].ensure_index([('geometry', dml.pymongo.GEOSPHERE)])
        repo['tigerlei.police'].ensure_index([('geometry', dml.pymongo.GEOSPHERE)])

        # Find police stations within 2km for every university  
        universities = repo['tigerlei.university'].find()
        for item in universities:
            neighbors = repo['tigerlei.police'].find(
                {
                    'geometry': {
                        '$near': {
                            '$geometry': {
                                'type': 'Point',
                                'coordinates': item['geometry']['coordinates']
                            },
                            '$maxDistance': 2000,
                            '$minDistance': 0
                            }
                        }
                })
            # Store university name and corresponding police station into neighborhood 
            if neighbors:
                neighbor_police_staion = []
                for neighbor in neighbors:
                    temp = {}
                    temp = neighbor['district']
                    neighbor_police_staion.append(temp)
                repo['tigerlei.neighborhood'].insert({
                    '_id': item['_id'],
                    'name': item['name'],
                    'neighbor_police_staion': neighbor_police_staion
                    })

        repo.logout()

        endTime = datetime.datetime.now()
        print("Transformation 3 has been done!") 
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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:tigerlei#transformation3', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)

        university = doc.entity('dat:tigerlei#university', {'prov:label':'colleges and universities in boston', prov.model.PROV_TYPE:'ont:DataSet'})
        police = doc.entity('dat:tigerlei#police', {'prov:label':'police station in boston', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.usage(this_run, police, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(this_run, university, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})        

        neighborhood = doc.entity('dat:tigerlei#neighborhood', {prov.model.PROV_LABEL:'universities with neighborhood police stations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhood, this_script)
        doc.wasGeneratedBy(neighborhood, this_run, endTime)
        doc.wasDerivedFrom(neighborhood, university, this_run, this_run, this_run)
        doc.wasDerivedFrom(neighborhood, police, this_run, this_run, this_run)

        repo.logout()
                  
        return doc

# transformation3.execute()
# doc = transformation3.provenance()
# print(doc.get_provn())

# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
