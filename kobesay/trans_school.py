import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class trans_school(dml.Algorithm):
    contributor = 'kobesay'
    reads = ['kobesay.publicschool', 'kobesay.nonpublicschool']
    writes = ['kobesay.regionschool', 'kobesay.regionpublicschool', 'kobesay.regionnonpublicschool']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kobesay', 'kobesay')

        repo.dropCollection("regionschool")
        repo.createCollection("regionschool")
        repo.dropCollection("regionpublicschool")
        repo.createCollection("regionpublicschool")
        repo.dropCollection("regionnonpublicschool")
        repo.createCollection("regionnonpublicschool")

        # select zip code and school of public schools
        # count number of public schools for each region
        items_publicschool = {}
        publicschool = repo.kobesay.publicschool.find()
        for x in publicschool:
            zipcode = x['fields']['zipcode'].split('-')[0]
            items_publicschool[zipcode] = items_publicschool.get(zipcode, 0) + 1
        r = [{'zipcode': zipcode, 'num': items_publicschool[zipcode]} for zipcode in items_publicschool]
        repo['kobesay.regionpublicschool'].insert_many(r)

        # select zip code and school of nonpublic schools
        # count number of nonpublic schools for each region
        items_nonpublicschool = {}
        nonpublicschool = repo.kobesay.nonpublicschool.find()
        for x in nonpublicschool:
            zipcode = x['properties']['ZIP'].split('-')[0]
            items_nonpublicschool[zipcode] = items_nonpublicschool.get(zipcode, 0) + 1
        r = [{'zipcode': zipcode, 'num': items_nonpublicschool[zipcode]} for zipcode in items_nonpublicschool]
        repo['kobesay.regionnonpublicschool'].insert_many(r)

        # merge result of public school and private school
        # count number of all public/private schools for each region
        items = {}
        for zipcode in items_publicschool:
            items[zipcode] = items.get(zipcode, 0) + items_publicschool[zipcode]
        for zipcode in items_nonpublicschool:
            items[zipcode] = items.get(zipcode, 0) + items_nonpublicschool[zipcode]
        r = [{'zipcode': zipcode, 'num': items[zipcode]} for zipcode in items]
        repo['kobesay.regionschool'].insert_many(r)

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
        repo.authenticate('kobesay', 'kobesay')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/') # City of Boston Data Portal
        doc.add_namespace('bwod', 'https://boston.opendatasoft.com/explore/dataset/') # Boston Wicked Open Data
        doc.add_namespace('bod', 'http://bostonopendata.boston.opendata.arcgis.com/datasets/') # BostonMaps: Open Data

        this_script = doc.agent('alg:kobesay#trans_school', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        publicschool = doc.entity('dat:kobesay#publicschool', {'prov:label':'public school', prov.model.PROV_TYPE:'ont:DataSet'})
        nonpublicschool = doc.entity('dat:kobesay#nonpublicschool', {'prov:label':'non public school', prov.model.PROV_TYPE:'ont:DataSet'})
        get_regionschool = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get region school'})
        get_regionpublicschool = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get region public school'})
        get_regionnonpublicschool = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get region non public school'})
        doc.wasAssociatedWith(get_regionschool, this_script)
        doc.wasAssociatedWith(get_regionpublicschool, this_script)
        doc.wasAssociatedWith(get_regionnonpublicschool, this_script)

        regionschool = doc.entity('dat:kobesay#regionschool', {prov.model.PROV_LABEL:'region school', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(regionschool, this_script)
        doc.wasGeneratedBy(regionschool, get_regionschool, endTime)
        doc.wasDerivedFrom(regionschool, publicschool, get_regionschool, get_regionschool, get_regionschool)
        doc.wasDerivedFrom(regionschool, nonpublicschool, get_regionschool, get_regionschool, get_regionschool)

        regionpublicschool = doc.entity('dat:kobesay#regionschool', {prov.model.PROV_LABEL:'region public school', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(regionpublicschool, this_script)
        doc.wasGeneratedBy(regionpublicschool, get_regionpublicschool, endTime)
        doc.wasDerivedFrom(regionpublicschool, publicschool, get_regionpublicschool, get_regionpublicschool, get_regionpublicschool)

        regionnonpublicschool = doc.entity('dat:kobesay#regionnonpublicschool', {prov.model.PROV_LABEL:'region non public school', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(regionnonpublicschool, this_script)
        doc.wasGeneratedBy(regionnonpublicschool, get_regionnonpublicschool, endTime)
        doc.wasDerivedFrom(regionnonpublicschool, nonpublicschool, get_regionnonpublicschool, get_regionnonpublicschool, get_regionnonpublicschool)

        repo.record(doc.serialize())
        repo.logout()
                  
        return doc

trans_school.execute()
doc = trans_school.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof