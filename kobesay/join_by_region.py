import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class join_by_region(dml.Algorithm):
    contributor = 'kobesay'
    reads = [
        'kobesay.regionincome',
        'kobesay.regionhospital',
        'kobesay.regionschool',
        'kobesay.regionpublicschool',
        'kobesay.regionnonpublicschool'
    ]
    writes = [
        'kobesay.income_infrastructure',
    ]

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kobesay', 'kobesay')

        repo.dropCollection("income_infrastructure")
        repo.createCollection("income_infrastructure")

        regionincome = repo.kobesay.regionincome.find()
        regionincome_info = {x['zipcode']: x['income'] for x in regionincome}

        regionhospital = repo.kobesay.regionhospital.find()
        regionhospital_info = {x['zipcode']: x['num'] for x in regionhospital}

        regionschool = repo.kobesay.regionschool.find()
        regionschool_info = {x['zipcode']: x['num'] for x in regionschool}

        regionpublicschool = repo.kobesay.regionpublicschool.find()
        regionpublicschool_info = {x['zipcode']: x['num'] for x in regionpublicschool}

        regionnonpublicschool = repo.kobesay.regionnonpublicschool.find()
        regionnonpublicschool_info = {x['zipcode']: x['num'] for x in regionnonpublicschool}

        zipcodes = regionincome_info.keys() | regionhospital_info.keys() | regionschool_info.keys() | regionpublicschool_info.keys() | regionnonpublicschool_info.keys()
        region_info = {}
        for zipcode in zipcodes:
            region_info[zipcode] = {}
            region_info[zipcode]['income'] = regionincome_info.get(zipcode, 0)
            region_info[zipcode]['num_hospital'] = regionhospital_info.get(zipcode, 0)
            region_info[zipcode]['num_school'] = regionschool_info.get(zipcode, 0)
            region_info[zipcode]['num_publicschool'] = regionpublicschool_info.get(zipcode, 0)
            region_info[zipcode]['num_nonpublicschool'] = regionnonpublicschool_info.get(zipcode, 0)
        r = []
        for zipcode in region_info:
            item = {
                'zipcode': zipcode,
                'income': region_info[zipcode]['income'],
                'num_hospital': region_info[zipcode]['num_hospital'],
                'num_school': region_info[zipcode]['num_school'],
                'num_publicschool': region_info[zipcode]['num_publicschool'],
                'num_nonpublicschool': region_info[zipcode]['num_nonpublicschool']
            }
            r.append(item)
        #print(r)
        repo['kobesay.income_infrastructure'].insert_many(r)

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

        this_script = doc.agent('alg:kobesay#join_by_region', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        regionincome = doc.entity('dat:kobesay#regionincome', {'prov:label':'region income', prov.model.PROV_TYPE:'ont:DataSet'})
        regionhospital = doc.entity('dat:kobesay#regionhospital', {'prov:label':'region hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        regionschool = doc.entity('dat:kobesay#regionschool', {'prov:label':'region school', prov.model.PROV_TYPE:'ont:DataSet'})
        regionpublicschool = doc.entity('dat:kobesay#regionpublicschool', {'prov:label':'region public school', prov.model.PROV_TYPE:'ont:DataSet'})
        regionnonpublicschool = doc.entity('dat:kobesay#regionnonpublicschool', {'prov:label':'region non public school', prov.model.PROV_TYPE:'ont:DataSet'})
        get_income_infrastructure = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'get relation of region income and infrastructure'})
        doc.wasAssociatedWith(get_income_infrastructure, this_script)

        income_infrastructure = doc.entity('dat:kobesay#income_infrastructure', {prov.model.PROV_LABEL:'region income and infrastructure', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(income_infrastructure, this_script)
        doc.wasGeneratedBy(income_infrastructure, get_income_infrastructure, endTime)
        doc.wasDerivedFrom(income_infrastructure, regionincome, get_income_infrastructure, get_income_infrastructure, get_income_infrastructure)
        doc.wasDerivedFrom(income_infrastructure, regionhospital, get_income_infrastructure, get_income_infrastructure, get_income_infrastructure)
        doc.wasDerivedFrom(income_infrastructure, regionschool, get_income_infrastructure, get_income_infrastructure, get_income_infrastructure)
        doc.wasDerivedFrom(income_infrastructure, regionpublicschool, get_income_infrastructure, get_income_infrastructure, get_income_infrastructure)
        doc.wasDerivedFrom(income_infrastructure, regionnonpublicschool, get_income_infrastructure, get_income_infrastructure, get_income_infrastructure)

        repo.record(doc.serialize())
        repo.logout()
                  
        return doc

join_by_region.execute()
doc = join_by_region.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof