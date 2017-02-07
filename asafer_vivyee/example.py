import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'asafer_vivyee'
    reads = []
    writes = ['asafer_vivyee.lost', 'asafer_vivyee.found']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asafer_vivyee', 'asafer_vivyee')

        url = 'http://cs-people.bu.edu/lapets/591/examples/lost.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("lost")
        repo.createCollection("lost")
        repo['asafer_vivyee.lost'].insert_many(r)
        repo['asafer_vivyee.lost'].metadata({'complete':True})
        print(repo['asafer_vivyee.lost'].metadata())

        url = 'http://cs-people.bu.edu/lapets/591/examples/found.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("found")
        repo.createCollection("found")
        repo['asafer_vivyee.found'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
