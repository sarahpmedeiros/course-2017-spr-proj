import urllib.request
import sodapy
import json
import dml
import prov.model
import datetime
import uuid

class fetch_data(dml.Algorithm):
    contributor = 'ajr10_williami'
    reads = []
    writes = ['ajr10_williami.lost', 'ajr10_williami.found']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets and store in mongodb collections.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_williami', 'ajr10_williami')

        # data_targets is formatted as follows
        # "targetname" : ["url_to_target", "target_key"]
        data_targets = {
         "city_of_boston": ["data.cityofboston.gov", "awu8-dc52"],
         "lapets": ["http://cs-people.bu.edu/lapets/591/examples/lost.json", ""]
         }

        for target_name in data_targets:
            target_info = data_targets[target_name]
            # do requests here
            if target_info[1] != "":
                client = sodapy.Socrata(target_info[0], None)
                response = client.get(target_info[1], limit=10)

                response_data = json.dumps(response, sort_keys=True, indent=2)
                print(response_data)
            else:
                response = urllib.request.urlopen(target_info[0]).read().decode("utf-8")
                response_data = json.dumps(json.loads(response), sort_keys=True, indent=2)
                print(response_data)
               
            
            repo.dropCollection(target_name)
            repo.createCollection(target_name)

            repo[target_name].insert_many()
            repo[target_name].metadata({'complete':True})
            print(repo[target_name].metadata())
            
            
        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
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
        repo.authenticate('ajr10_williami', 'ajr10_williami')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:ajr10_williami#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
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

        lost = doc.entity('dat:ajr10_williami#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:ajr10_williami#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

example.execute()
'''
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
