import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy


class mapReduce1(dml.Algorithm):
    contributor = 'pt0713_silnuext'
    reads = []
    writes = ['pt0713_silnuext.property_2015']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pt0713_silnuext', 'pt0713_silnuext')

        client = sodapy.Socrata("data.cityofboston.gov", None)
        response = client.get("n7za-nsjh")
        #r = json.loads(response)
        s = json.dumps(response, sort_keys=True, indent=2)
        print(s)
        repo.dropCollection("property_2015")
        repo.createCollection("property_2015")
        repo['pt0713_silnuext.property_2015'].insert_many(response)
        repo['pt0713_silnuext.property_2015'].metadata({'complete':True})
        print(repo['pt0713_silnuext.property_2015'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
