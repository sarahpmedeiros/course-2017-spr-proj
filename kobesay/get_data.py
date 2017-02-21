import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

def fetch_json(url):
    print(url)
    response = urllib.request.urlopen(url).read().decode("utf-8")
    r = json.loads(response)
    return r

def fetch_json_local(filename):
    #print(filename)
    with open(filename, 'r') as fin:
        r = json.loads(fin.read())
    return r

class get_data(dml.Algorithm):
    contributor = ‘kobesay’
    reads = []
    writes = [
        'kobesay.income2013',
        'kobesay.income2014',
        'kobesay.hospital',
        'kobesay.publicschool',
        'kobesay.nonpublicschool'
    ]

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('kobesay', 'kobesay')

        # income 2013
        r = fetch_json('http://data.cityofboston.gov/resource/54s2-yxpg.json')
        #r = fetch_json_local('income2013.json')
        repo.dropCollection("income2013")
        repo.createCollection("income2013")
        repo['kobesay.income2013'].insert_many(r)

        # income 2014
        r = fetch_json('http://data.cityofboston.gov/resource/4swk-wcg8.json')
        #r = fetch_json_local('income2014.json')
        repo.dropCollection("income2014")
        repo.createCollection("income2014")
        repo['kobesay.income2014'].insert_many(r)

        # hospital
        r = fetch_json('http://data.cityofboston.gov/resource/46f7-2snz.json')
        #r = fetch_json_local('hospital.json')
        repo.dropCollection("hospital")
        repo.createCollection("hospital")
        repo['kobesay.hospital'].insert_many(r)

        # public school
        r = fetch_json('http://boston.opendatasoft.com/explore/dataset/public-schools/download/?format=json')
        #r = fetch_json_local('publicschool.json')
        repo.dropCollection("publicschool")
        repo.createCollection("publicschool")
        repo['kobesay.publicschool'].insert_many(r)

        # non public school
        r = fetch_json('http://bostonopendata.boston.opendata.arcgis.com/datasets/0046426a3e4340a6b025ad52b41be70a_1.geojson')
        #r = fetch_json_local('nonpublicschool.json')
        r = r['features']
        repo.dropCollection("nonpublicschool")
        repo.createCollection("nonpublicschool")
        repo['kobesay.nonpublicschool'].insert_many(r)

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

        this_script = doc.agent('alg:kobesay#get_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource_income2013 = doc.entity('bdp:54s2-yxpg', {'prov:label':'Employee Earnings Report 2013', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_income2013 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_income2013, this_script)
        doc.usage(get_income2013, resource_income2013, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
            )
        
        resource_income2014 = doc.entity('bdp:4swk-wcg8', {'prov:label':'Employee Earnings Report 2014', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_income2014 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_income2014, this_script)
        doc.usage(get_income2014, resource_income2014, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
            )

        resource_hospital = doc.entity('bdp:46f7-2snz', {'prov:label':'Hospital Locations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_hospital = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_hospital, this_script)
        doc.usage(get_hospital, resource_hospital, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
            )

        resource_publicschool = doc.entity('bwod:public-schools', {'prov:label':'Public Schools', prov.model.PROV_TYPE:'ont:DataResource'})
        get_publicschool = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_publicschool, this_script)
        doc.usage(get_publicschool, resource_publicschool, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
            )

        resource_nonpublicschool = doc.entity('bod:0046426a3e4340a6b025ad52b41be70a_1', {'prov:label':'NON PUBLIC SCHOOLS', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        get_nonpublicschool = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_nonpublicschool, this_script)
        doc.usage(get_nonpublicschool, resource_nonpublicschool, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
            )

        income2013 = doc.entity('dat:kobesay#income2013', {prov.model.PROV_LABEL:'income 2013', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(income2013, this_script)
        doc.wasGeneratedBy(income2013, get_income2013, endTime)
        doc.wasDerivedFrom(income2013, resource_income2013, get_income2013, get_income2013, get_income2013)

        income2014 = doc.entity('dat:kobesay#income2014', {prov.model.PROV_LABEL:'income 2014', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(income2014, this_script)
        doc.wasGeneratedBy(income2014, get_income2014, endTime)
        doc.wasDerivedFrom(income2014, resource_income2014, get_income2014, get_income2014, get_income2014)

        hospital = doc.entity('dat:kobesay#hospital', {prov.model.PROV_LABEL:'hospital', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(hospital, this_script)
        doc.wasGeneratedBy(hospital, get_hospital, endTime)
        doc.wasDerivedFrom(hospital, resource_hospital, get_hospital, get_hospital, get_hospital)

        publicschool = doc.entity('dat:kobesay#publicschool', {prov.model.PROV_LABEL:'public school', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(publicschool, this_script)
        doc.wasGeneratedBy(publicschool, get_publicschool, endTime)
        doc.wasDerivedFrom(publicschool, resource_publicschool, get_publicschool, get_publicschool, get_publicschool)

        nonpublicschool = doc.entity('dat:kobesay#nonpublicschool', {prov.model.PROV_LABEL:'non public school', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(nonpublicschool, this_script)
        doc.wasGeneratedBy(nonpublicschool, get_nonpublicschool, endTime)
        doc.wasDerivedFrom(nonpublicschool, resource_nonpublicschool, get_nonpublicschool, get_nonpublicschool, get_nonpublicschool)

        repo.record(doc.serialize())
        repo.logout()
                  
        return doc

get_data.execute()
doc = get_data.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof