import urllib.request
import sodapy
import json
import dml
import prov.model
import datetime
import uuid
import bson.code
from bson.json_util import dumps
from tqdm import tqdm

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

def product(R, S):
    return [(t,u) for t in R for u in S]

def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]

def k_means(M,P):
    OLD = []
    i=0
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
        M = sorted(M)
    return sorted(M)

class k_means_crime(dml.Algorithm):
    contributor = 'johnt3_rsromero'
    reads = ['johnt3_rsromero.newbpdcrimefio']
    writes = ['johnt3_rsromero.k_means_crime']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets and store in mongodb collections.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('johnt3_rsromero', 'johnt3_rsromero')

        # Perform transformation here

        bpdcrimefiodata = repo["johnt3_rsromero.newbpdcrimefio"].find()
        crimedata = []
        for file in bpdcrimefiodata:
        	coordinates = file["coordinates"]
        	if not coordinates[0]<0.001:
                    crimedata.append((coordinates[0], coordinates[1]))
  
       
        result = k_means([(42.271639799999996, -71.11138439999999), (42.311262, -71.0516695), (42.314266399999994, -71.07462699999999), (42.32631528571428, -71.08366528571429), (42.343675999999995, -71.09928833333333), (42.34932933333333, -71.14292833333333), (42.351278, -71.058361)], crimedata)
        
        print("result")
        print(result)
        
        
        insertresult = {}
        insertresult["k_means"] = result
        repo.dropPermanent("johnt3_rsromero.k_means_crime")
        repo.createPermanent("johnt3_rsromero.k_means_crime")
        repo['johnt3_rsromero.k_means_crime'].insert(insertresult)
        repo['johnt3_rsromero.k_means_crime'].metadata({'complete':True})

        print(repo['johnt3_rsromero.k_means_crime'].metadata())

        # logout and return start and end times
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}
        
    @staticmethod
    def execute(trial = True):
        '''Retrieve some data sets and store in mongodb collections.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('johnt3_rsromero', 'johnt3_rsromero')

        # Perform transformation here

        bpdcrimefiodata = repo["johnt3_rsromero.newbpdcrimefio"].find()
        crimedata = []
        for file in bpdcrimefiodata[:100]:
        	coordinates = file["coordinates"]
        	if not coordinates[0]<0.001:
                    crimedata.append((coordinates[0], coordinates[1]))
  
       
        result = k_means([(42.2732799999722, -71.09420999999134), (42.34142366030704, -71.05493637683747), (42.37110563410766, -71.03891371591536), (42.34901393823429, -71.15066920685649), (42.36159545342423, -71.06016143782773), (42.30963262657127, -71.1044080816693), (42.3503200003679, -71.07536999992539), (42.25643814120341, -71.12402332161341), (42.32871624003303, -71.1485675478997), (42.297771135772116, -71.05911507704647), (42.32871624003303, -71.08418232923032)], crimedata)
        
        
        ##print("result")
        ##print(result)
        
        
        insertresult = {}
        insertresult["k_means"] = result
        repo.dropPermanent("johnt3_rsromero.k_means_crime")
        repo.createPermanent("johnt3_rsromero.k_means_crime")
        repo['johnt3_rsromero.k_means_crime'].insert(insertresult)
        repo['johnt3_rsromero.k_means_crime'].metadata({'complete':True})

        print(repo['johnt3_rsromero.k_means_crime'].metadata())

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
        repo.authenticate('johnt3_rsromero', 'johnt3_rsromero')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/') #city of boston data portal

        this_script = doc.agent('alg:johnt3_rsromero#k_means_crime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        newbpdcrimefio_resource = doc.entity('bdp:2pem-965w+ufcx-3fdn', {'prov:label':'Combined BPD FIO + Crime Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_bpdcrimefio = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_bpdcrimefio, this_script)
        
        doc.usage(get_bpdcrimefio, newbpdcrimefio_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Run+K_means'
                  }
                  )

        k_means_crime = doc.entity('dat:johnt3_rsromero#k_means_crime', {prov.model.PROV_LABEL:'K_Means Clustering on Crime occurrence', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(k_means_crime, this_script)
        doc.wasGeneratedBy(k_means_crime, get_bpdcrimefio, endTime)
        doc.wasDerivedFrom(k_means_crime, newbpdcrimefio_resource, get_bpdcrimefio, get_bpdcrimefio, get_bpdcrimefio)

        repo.logout()
                  
        return doc
    

##k_means_crime.execute()
##doc = k_means_crime.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof
