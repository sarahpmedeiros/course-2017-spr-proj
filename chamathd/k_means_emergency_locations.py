import urllib.request
import sodapy
import json
import dml
import prov.model
import datetime
import uuid

# Functions below courtesy of Prof. Lapets

def dist(p, q):
    (x1,y1) = p
    (x2,y2) = q
    return (x1-x2)**2 + (y1-y2)**2

def plus(args):
    p = [0,0]
    for (x,y) in args:
        p[0] += x
        p[1] += y
    return tuple(p)

def product(R, S):
    return [(t,u) for t in R for u in S]

def scale(p, c):
    (x,y) = p
    return (x/c, y/c)

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

def k_means(P,M):
    OLD = []
    while OLD != M:
        OLD = M

        MPD = [(m, p, dist(m,p)) for (m, p) in product(M, P)]
        PDs = [(p, dist(m,p)) for (m, p, d) in MPD]
        PD = aggregate(PDs, min)
        MP = [(m, p) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
        MT = aggregate(MP, plus)

        M1 = [(m, 1) for ((m,p,d), (p2,d2)) in product(MPD, PD) if p==p2 and d==d2]
        MC = aggregate(M1, sum)

        M = [scale(t,c) for ((m,t),(m2,c)) in product(MT, MC) if m == m2]
    return sorted(M)

class k_means_emergency_locations(dml.Algorithm):
    contributor = 'chamathd'
    reads = ['chamathd.neighborhood_sea_level_data']
    writes = ['chamathd.k_means_emergency']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets for the MongoDB collection.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('chamathd', 'chamathd')

        # Perform initialization for the new repository
        colName = "chamathd.neighborhood_pop"
        repo.dropCollection(colName)
        repo.createCollection(colName)

        # Retrieve data from the Boston neighborhood collection
        print("Retrieving data from the Boston neighborhood collection")

        boston_nhood_col = repo["chamathd.neighborhood_pop_boston"].find().limit(50)

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

        cambridge_nhood_col = repo["chamathd.neighborhood_pop_cambridge"].find().limit(50)

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
        repo.authenticate('chamathd', 'chamathd')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('cha', 'chamathd')

        this_script = doc.agent('alg:chamathd#k_means_emergency_locations', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        neighborhood_pop_boston_res = doc.entity('cha:neighborhood_pop_boston', {'prov:label':'Boston Neighborhood Population Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        neighborhood_pop_cambridge_res = doc.entity('cha:neighborhood_pop_cambridge', {'prov:label':'Cambridge Neighborhood Population Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

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

        neighborhood_pop = doc.entity('dat:chamathd#neighborhood_pop', {prov.model.PROV_LABEL:'Unionized Boston-Area Neighborhood Population Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhood_pop, this_script)
        doc.wasGeneratedBy(neighborhood_pop, get_neighborhood_pop_boston, endTime)
        doc.wasGeneratedBy(neighborhood_pop, get_neighborhood_pop_cambridge, endTime)
        
        repo.logout()
                  
        return doc

k_means_emergency_locations.execute()
doc = k_means_emergency_locations.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
