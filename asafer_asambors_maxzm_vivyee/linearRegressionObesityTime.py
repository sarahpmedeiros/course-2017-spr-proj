import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests

class linearRegressionObesityTime(dml.Algorithm):
        contributor = 'asafer_asambors_maxzm_vivyee'
        reads = ['asager_asambors_maxzm_vivyee.obesity_time']
        writes = ['asager_asambors_maxzm_vivyee.obesity_time_linear_reagression_data']

        @staticmethod
        def execute(trial=False):
                startTime = datetime.datetime.now()

                #set up the connection
                client = dml.pymongo.MongoClient()
                repo = client.repo
                repo.authenticate('asafer_asambors_maxzm_vivyee','asafer_asambors_maxzm_vivyee')


                #loads
                obesity_time = repo['asafer_asambors_maxzm_vivyee.obesty_time']


                return {"start":startTime, "end":endtime}

        @staticmethod
        def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
                #TODO write this :/

