import urllib.request
import sodapy
import json
import dml
import prov.model
import datetime
import uuid

class unionize_neighborhood_data(dml.Algorithm):
    contributor = 'ajr10_chamathd_williami'
    reads = ['ajr10_chamathd_williami.neighborhood_pop_boston', \
              'ajr10_chamathd_williami.neighborhood_pop_cambridge']
    writes = ['ajr10_chamathd_williami.neighborhood_pop']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets for the MongoDB collection.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_chamathd_williami', 'ajr10_chamathd_williami')

        # Perform initialization for the new repository
        colName = "ajr10_chamathd_williami.neighborhood_pop"
        repo.dropCollection(colName)
        repo.createCollection(colName)

        # Retrieve data from the Boston neighborhood collection
        print("Retrieving data from the Boston neighborhood collection")

        boston_nhood_col = repo["ajr10_chamathd_williami.neighborhood_pop_boston"].find().limit(50)

        print("Inserting Boston data into collection", colName)
        for nhood in boston_nhood_col:
            # Just add city reference, otherwise keep data as is
            nhood_dict = {}
            nhood_dict["name"] = nhood["name"]
            nhood_dict["city"] = "Boston"
            nhood_dict["population"] = nhood["population"]

            repo[colName].insert(nhood_dict)
            
        print("Finished writing Boston data to", colName)
        print()

        # Retrieve data from the Cambridge neighborhood collection
        print("Retrieving data from the Cambridge neighborhood collection")

        cambridge_nhood_col = repo["ajr10_chamathd_williami.neighborhood_pop_cambridge"].find().limit(50)

        print("Inserting Cambridge data into collection", colName)
        for nhood in cambridge_nhood_col:
            # Cambridge data is more complicated, so project the data we need
            # and then unionize it toward the generalized set
            name = nhood["neighborhood_1"]
            population = int(nhood["total_population"])

            # Small conversion in order to guarantee data set unionization
            if name == "MIT":
                name = "Area 2/MIT"
            
            nhood_dict = {}
            nhood_dict["name"] = name
            nhood_dict["city"] = "Cambridge"
            nhood_dict["population"] = population

            repo[colName].insert(nhood_dict)
            
        print("Finished writing Cambridge data to", colName)
        print()

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
        repo.authenticate('ajr10_chamathd_williami', 'ajr10_chamathd_williami')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('acw', 'ajr10_chamathd_williami')

        this_script = doc.agent('alg:ajr10_chamathd_williami#unionize_neighborhood_data', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        neighborhood_pop_boston_res = doc.entity('acw:neighborhood_pop_boston', {'prov:label':'Boston Neighborhood Population Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        neighborhood_pop_cambridge_res = doc.entity('acw:neighborhood_pop_cambridge', {'prov:label':'Cambridge Neighborhood Population Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_neighborhood_pop_boston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_neighborhood_pop_cambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_neighborhood_pop_boston, this_script)
        doc.wasAssociatedWith(get_neighborhood_pop_cambridge, this_script)
        
        doc.usage(get_neighborhood_pop_boston, neighborhood_pop_boston_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Neighborhood+Pop+Boston'
                  }
                  )
        doc.usage(get_neighborhood_pop_cambridge, neighborhood_pop_cambridge_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Neighborhood+Pop+Cambridge'
                  }
                  )

        neighborhood_pop = doc.entity('dat:ajr10_chamathd_williami#neighborhood_pop', {prov.model.PROV_LABEL:'Unionized Boston-Area Neighborhood Population Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhood_pop, this_script)
        doc.wasGeneratedBy(neighborhood_pop, get_neighborhood_pop_boston, endTime)
        doc.wasGeneratedBy(neighborhood_pop, get_neighborhood_pop_cambridge, endTime)
        
        repo.logout()
                  
        return doc

##unionize_neighborhood_data.execute()
##doc = unionize_neighborhood_data.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
