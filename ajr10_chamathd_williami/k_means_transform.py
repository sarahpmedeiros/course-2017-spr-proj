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

def k_means(P, M):
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

class k_means_transform(dml.Algorithm):
    contributor = 'ajr10_chamathd_williami'
    reads = ['ajr10_chamathd_williami.neighborhood_sea_level_data']
    writes = ['ajr10_chamathd_williami.k_means']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets for the MongoDB collection.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_chamathd_williami', 'ajr10_chamathd_williami')

        # Perform initialization for the new repository
        colName = "ajr10_chamathd_williami.k_means"
        repo.dropCollection(colName)
        repo.createCollection(colName)

        # --- Operational code ---

        kMeansDict = {}

        NUM_MEANS = 4    # Modify this value to vary number of means
        print("Retrieving data from the neighborhood sea level data collection")
        nhood_data = repo["ajr10_chamathd_williami.neighborhood_sea_level_data"].find({}, {"center_x": 1, "center_y": 1}).limit(50)
        print()
        points = []
        for nhood in nhood_data:
            points += [(nhood["center_x"], nhood["center_y"])]

        while NUM_MEANS < 21:
            print("Running k-means with " + str(NUM_MEANS) + " means")
            # Run k-means++ to generate good seed values
            seeds = []
            centers = points.copy()
            random.shuffle(centers)
            # Pop a random neighborhood as the initial seed
            seeds += [centers.pop()]
            k = 1
            while k < NUM_MEANS:
                distances = []
                # Check each center for the nearest seed
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
                # Generate probabilities for each center
                prob = distance / total_prob
                probs += [prob]
                # Select random center with probability proportional to distance
                # Pop that center onto the seed list
                index = choice(list(range(len(centers))), 1, probs)[0]
                seeds += [centers.pop(index)]
                k += 1

            # Run k-means on the result to get the actual means
            means = k_means(points, seeds)
            kMeansDict[str(NUM_MEANS) + "_means"] = means
            NUM_MEANS += 2

        print()
        print("Inserting k-means information into database")
        repo[colName].insert(kMeansDict)
        
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

        this_script = doc.agent('alg:ajr10_chamathd_williami#k_means_transform', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        neighborhood_sea_level_data_res = doc.entity('acw:neighborhood_sea_level_data', {'prov:label':'Boston Area Neighborhood Sea Level Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_neighborhood_sea_level_data = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_neighborhood_sea_level_data, this_script)
        
        doc.usage(get_neighborhood_sea_level_data, neighborhood_sea_level_data_res, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Neighborhood+Sea+Level+Data'
                  }
                  )

        k_means = doc.entity('dat:ajr10_chamathd_williami#k_means', {prov.model.PROV_LABEL:'K-Means Transformation for Various Means', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(k_means, this_script)
        doc.wasGeneratedBy(k_means, get_neighborhood_sea_level_data, endTime)
        doc.wasDerivedFrom(k_means, neighborhood_sea_level_data_res, get_neighborhood_sea_level_data, get_neighborhood_sea_level_data, get_neighborhood_sea_level_data)
        
        repo.logout()
                  
        return doc

k_means_transform.execute()
##doc = transformation1.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
