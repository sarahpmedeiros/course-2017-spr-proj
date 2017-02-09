import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class FetchData(dml.Algorithm):
    contributor = 'asafer_vivyee'
    reads = []
    writes = ['asafer_vivyee.orchards', 'asafer_vivyee.corner_stores', 'asafer_vivyee.obesity', 'asafer_vivyee.nutrition_prog', 'asafer_vivyee.mbta']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('asafer_vivyee', 'asafer_vivyee')

        datasets = {
            'asafer_vivyee.orchards': 'https://data.cityofboston.gov/resource/8tmm-wjbw.json',
            'asafer_vivyee.corner_stores': 'https://data.cityofboston.gov/resource/ybm6-m5qd.json',
            'asafer_vivyee.obesity': 'https://chronicdata.cdc.gov/resource/a2ye-t2pa.json',
            'asafer_vivyee.nutrition_prog': 'https://data.cityofboston.gov/resource/ahjc-pw5e.json',
            'asafer_vivyee.mbta': ???
        }

        for collection, url in datasets.items():
            response = request.get(url)
            if response.status_code == 200:
                data = json.loads(response)
                repo.dropPermanent(collection)
                repo.createPermanent(collection)
                repo[collection].insert_many(data)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
