# some kmeans shenanigans

import json
import dml
import prov.model
import datetime
import uuid
import ast
import sodapy
import time 

# this transformation will check how many comm gardens and food pantries there are for each area
# we want to take (zipcode, #comm gardens) (zipcode, #food pantries) --> (area, #food pantries#comm gardens)
# I think this is a selection and aggregation

class transformation_two(dml.Algorithm):

    contributor = 'mrhoran_rnchen'

    reads = ['mrhoran_rnchen.farmers_market']

    writes = ['mrhoran_rnchen.local_fm']


    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()

        repo = client.repo
        
        repo.authenticate('mrhoran_rnchen', 'mrhoran_rnchen')
        ## code here

        ## get town name and lat, long coordinates
        X = select((project([o for o in repo.mrhoran_rnchen.farmers_market.find({})], get_correct_locations)), lambda t: t != None)

        repo.dropCollection('mrhoran_rnchen.local_fm')
        repo.createCollection('mrhoran_rnchen.local_fm')

        repo.mrhoran_rnchen.local_fm.insert(dict(X))

        ## Now to do K means
        
        Y = (project(X, lambda t: (t[1][0], t[1][1])))

        M = [Y[0], Y[1]]

        P = Y[1:]

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
            #print(sorted(M))

        print(sorted(M))

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
        repo.authenticate('mrhoran_rnchen', 'mrhoran_rnchen')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:mrhoran_rnchen#transformation_two', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource1 = doc.entity('bdp:66t5-f563', {'prov:label':'Farmers Market', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_farmers_market = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_farmers_market, this_script)

        doc.usage(get_farmers_market, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  }
                  )
        farmers_market = doc.entity('dat:mrhoran_rnchen#farmers_market', {prov.model.PROV_LABEL:'Food Pantries', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        doc.wasAttributedTo(farmers_market, this_script)
        doc.wasGeneratedBy(farmers_market, get_farmers_market, endTime)
        doc.wasDerivedFrom(farmers_market, resource1, get_farmers_market, get_farmers_market, get_farmers_market)
        repo.logout()
                  
        return doc

def get_correct_locations(farmer_market): # want to return the coordinates of the towns in and around Boston

    x = farmer_market['town']

    if (x == 'Cambridge' or x == 'Natick'or x == 'Newton' or x== 'Somerville'or x =='Boston' or x =='Jamaica Plain' or
        x == 'Charlestown' or x=='South Boston' or x =='Harvard'or x=="Dorchester"or x == "Allston" or x=='Brookline'
        or x == 'Roxbury' or x == 'West Roxbury' or x=='Watertown'):

        return(farmer_market['town'],farmer_market['location']['coordinates'])
 
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
def scale(p, c):
    (x,y) = p
    return (x/c, y/c)

def project(R, p):

    return [p(t) for t in R]

def select(R, s):
    return [t for t in R if s(t)]
def product(R, S):
    return [(t,u) for t in R for u in S]
def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]
    
transformation_two.execute()
doc = transformation_two.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
