import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

def union(R, S):
    return R + S

def difference(R, S):
    return [t for t in R if t not in S]

def intersect(R, S):
    return [t for t in R if t in S]

def project(R, p):
    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
 
def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]
    
def reduce(f, R):
    keys = {k for (k,v) in R}
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]

def normalizeDict(X, keyName, valName):
    justVals = [int(i.get(valName)) for i in X]
    justKeys = [i.get(keyName) for i in X]
    avg = sum(justVals)/len(justVals)
    normalizedVals = project(justVals, lambda x: x/avg)
    Y = [{justKeys[i]:normalizedVals[i]} for i in range(len(justKeys))]
    return Y    

def createTiers(X, A, k):
    minMax =  [(min([int(i.get(j)) for i in X]),max([int(i.get(j)) for i in X])) for j in A]
    Y = [0]*(k+1)
    for i in range(len(minMax)):
        interval = ((minMax[i][1] - minMax[i][0])/k)
        Y[i] = [minMax[i][0]+(interval*j) for j in range(k+1)]
    Z = [{A[j]:Y[j]} for j in range(len(A))]
    return Z

def zipToRent(X, A):
    Y = [(i.get("Zip "),i.get(A[j])) for i in X for j in range(len(A))]
    return Y

def assignTier(X, Y, A):
    housingTiers = Y[0].get(A[0])
    Z = [0]*len(X)
    for i in range(len(X)):
        for j in range(len(housingTiers)-1):
            if int(X[i][1]) >= int(housingTiers[j]) and int(X[i][1]) <= int(housingTiers[j+1]):
                Z[i] = {X[i][0]:housingTiers[j]}
                break
    return Z

def pullGrad(X, keyName, valName):
    justVals = [float(i.get(valName)) for i in X]
    justKeys = [i.get(keyName) for i in X]
    Y = [{justKeys[i]:justVals[i]} for i in range(len(justKeys))]
    
    return Y

def pullAges(X, keyName, valName):
    justVals = [float(i.get(valName)) for i in X]
    justKeys = [i.get(keyName) for i in X]
    Y = [{justKeys[i]:justVals[i]} for i in range(len(justKeys))]
    
    return Y

def pullNeighborhood(X, keyName, valName):
    justVals = [i.get(valName) for i in X]
    justKeys = [i.get(keyName) for i in X]
    Y = [{justKeys[i]:justVals[i]} for i in range(len(justKeys))]
    
    return Y

def maxRate(X):
    max1 = 0
    max2 = 0
    max3 = 0
    max4 = 0
    max5 = 0
    for i in range(len(X)):
        if X[i][0] == 1998:
            max1 = max(max1,X[i][1])
        if X[i][0] == 1806:
            max2 = max(max2,X[i][1])
        if X[i][0] == 1614:
            max3 = max(max3,X[i][1])
        if X[i][0] == 1422:
            max4 = max(max4,X[i][1])
        if X[i][0] == 1230:
            max5 = max(max5,X[i][1])
            
    
    tierToMax = [(1998,max1),(1806,max2),(1614,max3),(1422,max4),(1230,max5)]
    
    return tierToMax


class transformation3(dml.Algorithm):
    contributor = 'jspinell_mpinheir'
    reads = ['jspinell_mpinheir.ageRanges',
             'jspinell_mpinheir.housingRates',
             'jspinell_mpinheir.neighborhoods']
    writes = ['jspinell_mpinheir.ageByTier']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
    
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jspinell_mpinheir', 'jspinell_mpinheir')

        ageRanges = list(repo.jspinell_mpinheir.ageRanges.find())
        housingRates = list(repo.jspinell_mpinheir.housingRates.find())
        neighborhoods = list(repo.jspinell_mpinheir.neighborhoods.find())
        
        #Break housing prices into tiers
        typesOfHomes = ["Average Rent 2 Bedroom"]
        housingTiers = createTiers(housingRates, typesOfHomes, 5)
        
        #Pull zips with population info on 15 to 19 years of age
        
        newAgeRanges = pullAges(ageRanges, "Zip ", "15_to_19_years")
        #print(newAgeRanges)
        #Isolate Zips with a certain home
        zipAndRent = zipToRent(housingRates, typesOfHomes)
        
        
        #Assign Tiers
        zipAndTier = assignTier(zipAndRent, housingTiers, typesOfHomes)
        
        #MapReduce Tiers to average population of 15 to 19 year olds in that Tier
        zipAndTier = [(k,v) for i in range(len(zipAndTier)) for k,v in zipAndTier[i].items()]
        newAgeRanges = [(k,v) for i in range(len(newAgeRanges)) for k,v in newAgeRanges[i].items()]
        
        mapped = [(a[1], b[1]) for a in zipAndTier for b in newAgeRanges if a[0] == b[0]]
        
        reduced = reduce(lambda k,v:(k,sum(v)/len(v)), mapped)
        reduced.sort()
        toPush = [{"Price Range":str(reduced[i][0]) + "-" + str(reduced[i+1][0]), 
        "Average Age":str(reduced[i][1])} for i in range(len(reduced) - 1)]
    
        repo.dropCollection('ageByTier')
        repo.createCollection('ageByTier')
        repo['jspinell_mpinheir.ageByTier'].insert_many(toPush)
        
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
        repo.authenticate('jspinell_mpinheir', 'jspinell_mpinheir')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jspinell_mpinheir#transformation3', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        ageByTier = doc.entity('dat:jspinell_mpinheir#ageRanges', {prov.model.PROV_LABEL:'Age By Tier', prov.model.PROV_TYPE:'ont:DataSet'})
        this_ageByTier = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(this_ageByTier, this_script)
        
        doc.used(this_ageByTier, ageByTier, startTime)
        doc.wasAttributedTo(ageByTier, this_script)
        doc.wasGeneratedBy(ageByTier, this_ageByTier, endTime)
        
        #repo.record(doc.serialize())

        repo.logout()
                  
        return doc
    

"""
transformation3.execute()
doc = transformation3.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
"""

## eof

