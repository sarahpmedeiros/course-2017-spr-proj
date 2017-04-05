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
        newlist = sorted(i['Distances'], key=lambda k: k['Distance'])
        shortest.append({'School Name': i['School Name'], 'Closest': newlist[:3]})
    return shortest
    #return sorted(x, key=x.get, reverse=True)[:3]


#find distances then sort list and take the last 3 (top 3)

class corr_location(dml.Algorithm):
    contributor = 'hschurma_rcalleja'
    reads = ['hschurma_rcalleja.funding_location']
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
            
            dists.append({'School Name': c['School Name'], 'Distances': locs})

        #print(dists)
        print(shortest_dist(dists))
        

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

        doc.wasAssociatedWith(get_corr_fund, this_script)

        doc.used(get_corr_loc, funding_location, startTime)

        corr_loc = doc.entity('dat:hschurma_rcalleja#corr_location', {prov.model.PROV_LABEL:'High School Funding and Location Correlation', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(corr_loc, this_script)
        doc.wasGeneratedBy(corr_loc, get_corr_loc, endTime)
        
        doc.wasDerivedFrom(corr_loc, funding_location, get_corr_loc, get_corr_loc, get_corr_loc)
        
        repo.record(doc.serialize())
        repo.logout()

        return doc
                  

corr_location.execute()
#doc = corr_gradrates.provenance()
#print(json.dumps(json.loads(doc.serialize()), indent=4))
