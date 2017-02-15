import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class FetchData(dml.Algorithm):
    contributor = 'asafer_vivyee'
    reads = []
    writes = ['asafer_vivyee.orchards', 'asafer_vivyee.corner_stores', 'asafer_vivyee.obesity', 'asafer_vivyee.nutrition_prog', 'asafer_vivyee.mbta']

    @staticmethod
    def setup():
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asafer_vivyee', 'asafer_vivyee')
        return repo

    @staticmethod
    def store(repo, url, collection):
        response = requests.get(url)
        if response.status_code == 200:
            data = [response.json()]
            repo.dropPermanent(collection)
            repo.createPermanent(collection)
            repo[collection].insert_many(data)

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets'''
        startTime = datetime.datetime.now()

        repo = FetchData.setup()

        mbta_key = dml.auth['services']['mbtadeveloperportal']['key']
        cityofboston_token = dml.auth['services']['cityofbostondataportal']['token']
        cdc_token = dml.auth['services']['cdcdataportal']['token']

        datasets = {
            'asafer_vivyee.orchards': 'https://data.cityofboston.gov/resource/8tmm-wjbw.json$$app_token=' + cityofboston_token,
            'asafer_vivyee.corner_stores': 'https://data.cityofboston.gov/resource/ybm6-m5qd.json??app_token=' + cityofboston_token,
            'asafer_vivyee.obesity': 'https://chronicdata.cdc.gov/resource/a2ye-t2pa.json??app_token=' + cdc_token,
            'asafer_vivyee.nutrition_prog': 'https://data.cityofboston.gov/resource/ahjc-pw5e.json??app_token=' + cityofboston_token,
            'asafer_vivyee.mbta': 'http://realtime.mbta.com/developer/api/v2/routes?api_key=' + mbta_key + '&format=json'
        }

        for collection, url in datasets.items():
            FetchData.store(repo, url, collection)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # IDK WHAT ANY OF THIS DOES LOLLLLLLL
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asafer_vivyee', 'asafer_vivyee')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        repo.logout()

        return doc

FetchData.execute()
doc = FetchData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof