import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class trans_hospital(dml.Algorithm):
    contributor = 'heming'
    reads = ['heming.hospital']
    writes = ['heming.regionhospital']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('heming', 'heming')

        repo.dropCollection("regionhospital")
        repo.createCollection("regionhospital")

        items = {}

        # select zip code and hospital
        # count number of hospitals for each region
        hospital = repo.heming.hospital.find()
        for x in hospital:
            zipcode = x['zipcode'].split('-')[0]
            if len(zipcode) == 4:
                zipcode = '0' + zipcode
            items[zipcode] = items.get(zipcode, 0) + 1
        r = [{'zipcode': zipcode, 'num': items[zipcode]} for zipcode in items]
        #print(r)
        repo['heming.regionhospital'].insert_many(r)

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
        repo.authenticate('heming', 'heming')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/') # City of Boston Data Portal
        doc.add_namespace('bwod', 'https://boston.opendatasoft.com/explore/dataset/') # Boston Wicked Open Data
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/') # BostonMaps: Open Data

        this_script = doc.agent('alg:heming#trans_hospital', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        hospital = doc.entity('dat:heming#hospital', {'prov:label':'hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        get_regionhospital = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get region hospital'})
        doc.wasAssociatedWith(get_regionhospital, this_script)

        regionhospital = doc.entity('dat:heming#regionhospital', {prov.model.PROV_LABEL:'region hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(regionhospital, this_script)
        doc.wasGeneratedBy(regionhospital, get_regionhospital, endTime)
        doc.wasDerivedFrom(regionhospital, hospital, get_regionhospital, get_regionhospital, get_regionhospital)

        repo.record(doc.serialize())
        repo.logout()
                  
        return doc

trans_hospital.execute()
doc = trans_hospital.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof