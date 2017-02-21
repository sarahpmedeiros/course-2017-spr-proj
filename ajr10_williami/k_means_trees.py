import urllib.request
import sodapy
import json
import dml
import prov.model
import datetime
import uuid
import bson.code
from bson.json_util import dumps


# Credit to professor Lapets for these functions

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

class k_means_trees(dml.Algorithm):
    contributor = 'ajr10_williami'
    reads = ['ajr10_williami.cleaned_trees_cambridge',\
              'ajr10_williami.cleaned_trees_boston']
    writes = ['ajr10_williami.k_means_trees']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets and store in mongodb collections.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_williami', 'ajr10_williami')

        # Perform transformation here

        repo.dropCollection("ajr10_williami.k_means_trees")
        repo.createCollection("ajr10_williami.k_means_trees")        
        
        trees_boston = repo["ajr10_williami.cleaned_trees_boston"].find()
        boston_data = []
        for element in trees_boston:
            boston_data += [(element["longitude"], element["latitude"])]
        boston_result = k_means(boston_data, [(-74,33), (-83, 24)])

        trees_cambridge = repo["ajr10_williami.cleaned_trees_cambridge"].find()
        cambridge_data = []
        for element in trees_cambridge:
            cambridge_data += [(element["longitude"], element["latitude"])]
        cambridge_result = k_means(cambridge_data, [(-74,33), (-83, 24)])

        result = {}
        result["cambridge_k_means"] = cambridge_result
        result["boston_k_means"] = boston_result
        
        repo["ajr10_williami.k_means_trees"].insert(result)

        # logout and return start and end times
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
        repo.authenticate('ajr10_williami', 'ajr10_williami')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('awc', 'ajr10_williami')

        this_script = doc.agent('alg:ajr10_williami#k_means_trees', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        clean_trees_cambridge_resource = doc.entity('awc:cleaned_trees_cambridge', {'prov:label':'ajr10_williami.cleaned_trees_cambridge', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        clean_trees_boston_resource = doc.entity('awc:trees_boston', {'prov:label':'cleaned_trees_boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        calculate_k_means_trees_cambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        calculate_k_means_trees_boston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(calculate_k_means_trees_cambridge, this_script)
        doc.wasAssociatedWith(calculate_k_means_trees_boston, this_script)

        doc.usage(calculate_k_means_trees_cambridge, clean_trees_cambridge_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Caculate+K+Means+Trees+Cambridge'
                  }
                  )
        doc.usage(calculate_k_means_trees_boston, clean_trees_boston_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Caculate+K+Means+Trees+Boston'
                  }
                  )

        k_means_tree_cambridge = doc.entity('dat:ajr10_williami#cleaned_trees_cambridge', {prov.model.PROV_LABEL:'Cleaned Trees Cambridge', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(k_means_tree_cambridge, this_script)
        doc.wasGeneratedBy(k_means_tree_cambridge, calculate_k_means_trees_cambridge, endTime)
        doc.wasDerivedFrom(k_means_tree_cambridge, clean_trees_cambridge_resource, calculate_k_means_trees_cambridge, calculate_k_means_trees_cambridge, calculate_k_means_trees_cambridge)

        k_means_boston = doc.entity('dat:ajr10_williami#cleaned_trees_boston', {prov.model.PROV_LABEL:'Cleaned Trees Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(k_means_boston, this_script)
        doc.wasGeneratedBy(k_means_boston, calculate_k_means_trees_boston, endTime)
        doc.wasDerivedFrom(k_means_boston, clean_trees_boston_resource, calculate_k_means_trees_boston, calculate_k_means_trees_boston, calculate_k_means_trees_boston)

        repo.logout()

        return doc
'''    
k_means_trees.execute()

doc = clean_trees.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
