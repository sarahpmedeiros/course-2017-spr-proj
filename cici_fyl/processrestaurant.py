import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import methods

class processrestaurant(dml.Algorithm):
    contributor = 'cici_fyl'
    reads = ['property', 'restaurant']
    writes = ['property_restaurant']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cici_fyl', 'cici_fyl')

        propertydata = repo['cici_fyl.property'].find()
        restaurantdata= repo['cici_fyl.restaurant'].find()

        coor= methods.selectcoordinate(restaurantdata)

        x=methods.appendattribute(propertydata,"NumRestaurant")

        x=methods.inrange(x, coor, 0.002,'NumRestaurant',"location")

        repo.dropCollection("property_restaurant")
        repo.createCollection("property_restaurant")
        repo['cici_fyl.property_restaurant'].insert_many(x)

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

        this_script = doc.agent('alg:cici_fyl#processrestaurant', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        property_data = doc.entity('dat:property', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        pro_rest_w = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        rest_data = doc.entity('dat:restaurant', {prov.model.PROV_LABEL:'Boston restaurant', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAssociatedWith(pro_rest_w, this_script)

        pro_rest = doc.entity('dat:property_restaurant', {prov.model.PROV_LABEL:'Boston restaurant near properties', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(pro_rest, this_script)
        doc.wasGeneratedBy(pro_rest, pro_rest_w, endTime)
        doc.wasDerivedFrom(pro_rest, property_data, pro_rest_w, pro_rest_w, pro_rest_w)
        doc.wasDerivedFrom(pro_rest, rest_data, pro_rest_w, pro_rest_w, pro_rest_w)
      
        repo.logout()
                  
        return doc