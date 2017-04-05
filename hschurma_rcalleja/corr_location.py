import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

from math import sin, cos, sqrt, atan2, radians


def distance(x,y):
    [lat1,lon1] = x
    [lat2,lon2] = y

    lat1 = float(lat1)
    lat2 = float(lat2)
    lon1 = float(lon1)
    lon2 = float(lon2)

    R = 6373.0
    
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    return distance


def shortest_dist(x):
    shortest = []
    for i in x:
        newlist = sorted(i['Distance'], key=lambda k: k['Distance'])
        shortest.append({'School Name': i['School Name'], 'Closest': newlist[:3]})
    return shortest
    #return sorted(x, key=x.get, reverse=True)[:3]


#find distances then sort list and take the last 3 (top 3)


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



class corr_location(dml.Algorithm):
    contributor = 'hschurma_rcalleja'
    reads = ['hschurma_rcalleja.funding_location','hschurma_rcalleja.funding_SAT', 'hschurma_rcalleja.funding_gradrates']
    writes = ['hschurma_rcalleja.corr_location']


    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hschurma_rcalleja', 'hschurma_rcalleja')

        repo.dropPermanent("corr_location")
        repo.createPermanent("corr_location")


        fund_loc = list(repo.hschurma_rcalleja.funding_location.aggregate([{"$project":{"_id":0}}]))
        #print(fund_loc)


        fund_SAT = list(repo.hschurma_rcalleja.funding_SAT.aggregate([{"$project": {"_id": 0}}]))
        #print(fund_SAT)

    
        fund_grad = list(repo.hschurma_rcalleja.funding_gradrates.aggregate([{"$project":{"_id":0}}]))
        #print(fund_grad)


        P = product(fund_SAT, fund_grad)
        #print(P)
        S = select(P, lambda t: t[0]['Name'] == t[1]['Name'])
        #print(S)
        PR = project(S, lambda t: {'School Name': t[0]['Name'], 'SAT': t[0]['SAT'], 'Grad Rates': t[1]['Graduation Rates']})
        #print(PR)
        

        coord = []
        for i in fund_loc:
            coord.append({'School Name': i['Name'], 'Location': i['location'][1]})

        #print(coord)

        dists = []
        for c in coord:
            locs = []
            for s in coord:
                if c['School Name'] != s['School Name']:
                    locs.append({'School Name': s['School Name'], 'Distance': distance(c['Location'],s['Location'])})
            
            dists.append({'School Name': c['School Name'], 'Distance': locs})

        #print(dists)
        closest = shortest_dist(dists)

        
        close_with_fund = {}
        total_fund = {}
        for school in closest:
            name = school['School Name']
            total_fund[name] = {'2008':0, '2009':0,'2010':0, '2011':0,'2012':0, '2013':0,'2014':0, '2015':0, '2016':0}


        #print(total_fund)

        
        for school in closest:
            for c in school['Closest']:
                name = c['School Name']
                #print(name)
                fund = {}
                for i in fund_loc:
                    if i['Name'] == name:
                        fund = {name: i['Funding']}
                
                #print(fund)
                #print(school['School Name'])
                #print(name)
                
            
                for j in range(2008,2017):
                    year = str(j)
                    s = school['School Name']
                    #print(s)
                    #print(fund[name])
                    
                    fund[name][year] = int(fund[name][year].replace("$", "").replace(",", ""))

                    print(fund[name][year])
                    total_fund[s][year] += fund[name][year]

        print(total_fund)
                    
            

        prod_close = product(closest, PR)
        sel_close = (prod_close, lambda t: t[0]['School Name'] == t[1]['School Name'])
        proj_close = (sel_close, lambda t: {'School Name': t[0]['School Name'], 'SAT': t[0]['SAT'], 'Grad Rates': t[0]['Grad Rates'], 'Closest': t[1]['Closest']})

        #print(proj_close)
        

        '''
        sat_grad_loc = []
        for school in closest:
            for c in school['Closest']:
                sat_grad_loc.append({'School Name': school['School Name'], 
                
                
        
        print(shortest)'''


        repo.dropCollection('corr_location')
        repo.createCollection('corr_location')
        repo['hschurma_rcalleja.corr_location'].insert(closest)
        

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.'''
        

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hschurma_rcalleja', 'hschurma_rcalleja')

        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
    
        this_script = doc.agent('alg:hschurma_rcalleja#corr_location', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        funding_location = doc.entity('dat:hschurma_rcalleja#funding_location', {'prov:label':'Funding and School Location', \
            prov.model.PROV_TYPE:'ont:DataSet'})
        
        get_corr_loc = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_corr_loc, this_script)

        doc.used(get_corr_loc, funding_location, startTime)

        corr_loc = doc.entity('dat:hschurma_rcalleja#corr_location', {prov.model.PROV_LABEL:'High School Funding and Location Correlation', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(corr_loc, this_script)
        doc.wasGeneratedBy(corr_loc, get_corr_loc, endTime)
        
        doc.wasDerivedFrom(corr_loc, funding_location, get_corr_loc, get_corr_loc, get_corr_loc)
        
        #repo.record(doc.serialize())
        repo.logout()

        return doc
                  

corr_location.execute()
#doc = corr_location.provenance()
#print(json.dumps(json.loads(doc.serialize()), indent=4))
