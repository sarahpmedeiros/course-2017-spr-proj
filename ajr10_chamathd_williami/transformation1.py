import json
import dml
import prov.model
import datetime
import random
import uuid
from numpy.random import choice

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
    for x in range(1000):
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

class transformation1(dml.Algorithm):
    contributor = 'ajr10_chamathd_williami'
    reads = ['ajr10_chamathd_williami.neighborhood_sea_level_data']
    writes = ['ajr10_chamathd_williami.transformation1']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets for the MongoDB collection.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_chamathd_williami', 'ajr10_chamathd_williami')

        # Perform initialization for the new repository
        colName = "ajr10_chamathd_williami.transformation1"
        repo.dropCollection(colName)
        repo.createCollection(colName)

        # Operational code here:
        nhood_data = repo["ajr10_chamathd_williami.neighborhood_sea_level_data"].find({}, {"center_x": 1, "center_y": 1}).limit(50)
        points = []
        for nhood in nhood_data:
            points += [(nhood["center_x"], nhood["center_y"])]

        # Run k-means++ to generate good seed values
        seeds = []
        centers = points.copy()
        random.shuffle(centers)
        seeds += [centers.pop()]
        k = 1
        while k < 12:
            distances = []
            for index in range(len(centers)):
                min_dist = dist(centers[index], seeds[0])
                for seed in seeds:
                    distance = dist(centers[index], seed)
                    if distance < min_dist:
                        min_dist = distance
                D = min_dist ** 2
                distances += [D]
            total_prob = sum(distances)
            probs = []
            for distance in distances:
                prob = distance / total_prob
                probs += [prob]
            index = choice(list(range(len(centers))), 1, probs)[0]
            seeds += [centers.pop(index)]
            k += 1

        means = k_means(points, seeds)
        
        # Logout and end
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

        this_script = doc.agent('alg:ajr10_chamathd_williami#append_polygon_and_centerpoint', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        neighborhood_area_boston_res = doc.entity('acw:neighborhood_area_boston', {'prov:label':'Boston Neighborhood Population Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        neighborhood_area_cambridge_res = doc.entity('acw:neighborhood_area_cambridge', {'prov:label':'Cambridge Neighborhood Population Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_neighborhood_area_boston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_neighborhood_area_cambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_neighborhood_area_boston, this_script)
        doc.wasAssociatedWith(get_neighborhood_area_cambridge, this_script)
        
        doc.usage(get_neighborhood_area_boston, neighborhood_area_boston_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Neighborhood+Area+Boston'
                  }
                  )
        doc.usage(get_neighborhood_area_cambridge, neighborhood_area_cambridge_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Neighborhood+Area+Cambridge'
                  }
                  )

        neighborhood_info = doc.entity('dat:ajr10_chamathd_williami#neighborhood_info', {prov.model.PROV_LABEL:'Boston-Area Neighborhood Information', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(neighborhood_info, this_script)
        doc.wasGeneratedBy(neighborhood_info, get_neighborhood_area_boston, endTime)
        doc.wasGeneratedBy(neighborhood_info, get_neighborhood_area_cambridge, endTime)
        
        repo.logout()
                  
        return doc

transformation1.execute()
##doc = transformation1.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
