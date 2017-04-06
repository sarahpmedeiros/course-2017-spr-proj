import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class trans_income(dml.Algorithm):
    contributor = 'heming'
    reads = ['heming.income2013', 'heming.income2014']
    writes = ['heming.regionincome']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('heming', 'heming')

        repo.dropCollection("regionincome")
        repo.createCollection("regionincome")

        items = {}

        # select zip code and total earnings
        # merge result of income2013 and income2014
        # calculate average income of each region
        income2013 = repo.heming.income2013.find()
        income2014 = repo.heming.income2014.find()
        for x in income2013:
            zipcode = x['zip'].split('-')[0]
            income = float(x['total_earnings'])
            if zipcode in items:
                items[zipcode].append(income)
            else:
                items[zipcode] = [income]
        for x in income2014:
            zipcode = x['zip'].split('-')[0]
            income = float(x['total_earnings'])
            if zipcode in items:
                items[zipcode].append(income)
            else:
                items[zipcode] = [income]
        r = [{'zipcode': zipcode, 'income': sum(items[zipcode]) / len(items[zipcode])} for zipcode in items]
        repo['heming.regionincome'].insert_many(r)

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

        this_script = doc.agent('alg:heming#trans_income', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        income2013 = doc.entity('dat:heming#income2013', {'prov:label':'income 2013', prov.model.PROV_TYPE:'ont:DataSet'})
        income2014 = doc.entity('dat:heming#income2014', {'prov:label':'income 2014', prov.model.PROV_TYPE:'ont:DataSet'})
        get_regionincome = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get region income'})
        doc.wasAssociatedWith(get_regionincome, this_script)

        regionincome = doc.entity('dat:heming#regionincome', {prov.model.PROV_LABEL:'region income', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(regionincome, this_script)
        doc.wasGeneratedBy(regionincome, get_regionincome, endTime)
        doc.wasDerivedFrom(regionincome, income2013, get_regionincome, get_regionincome, get_regionincome)
        doc.wasDerivedFrom(regionincome, income2014, get_regionincome, get_regionincome, get_regionincome)

        repo.record(doc.serialize())
        repo.logout()
                  
        return doc

trans_income.execute()
doc = trans_income.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof