import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class schoolrestaurant(dml.Algorithm):
    contributor = 'cici_fyl'
    reads = []
    writes = ['school', 'restaurant']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cici_fyl', 'cici_fyl')

        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/1d9509a8b2fd485d9ad471ba2fdb1f90_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("school")
        repo.createCollection("school")
       
        r=r["features"]

        repo['cici_fyl.school'].insert_many(r)
        repo['cici_fyl.school'].metadata({'complete':True})
        print(repo['cici_fyl.school'].metadata())

        url = 'https://data.cityofboston.gov/resource/fdxy-gydq.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("restaurant")
        repo.createCollection("restaurant")
        repo['cici_fyl.restaurant'].insert_many(r)
        repo['cici_fyl.restaurant'].metadata({'complete':True})
        print(repo['cici_fyl.restaurant'].metadata())


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
        repo.authenticate('cici_fyl', 'cici_fyl')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod','http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:cici_fyl#schoolrestaurant', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        dataset1 = doc.entity('bdp:fdxy-gydq', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'json'})
        dataset2 = doc.entity('bod:1d9509a8b2fd485d9ad471ba2fdb1f90_0', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'geojson'})

        school_data = doc.entity('dat:cici_fyl#school', {'prov:label':'Boston Public Schools', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        restaurant_data = doc.entity('dat:cici_fyl#restaurant', {prov.model.PROV_LABEL:'Boston Restaurants', prov.model.PROV_TYPE:'ont:DataSet'})
        get_school = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_restaurant = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_school, this_script)
        doc.wasAssociatedWith(get_restaurant, this_script)
        #Query might need to be changed
        doc.usage(get_school,dataset2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_restaurant, dataset1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        doc.wasAttributedTo(school_data, this_script)
        doc.wasGeneratedBy(school_data, get_school, endTime)
        doc.wasDerivedFrom(school_data, dataset2, get_school, get_school)



        doc.wasAttributedTo(restaurant_data, this_script)
        doc.wasGeneratedBy(restaurant_data, get_restaurant, endTime)
        doc.wasDerivedFrom(restaurant_data, dataset1, get_restaurant)
        

        repo.logout()
                  
        return doc

doc = schoolrestaurant.provenance()

