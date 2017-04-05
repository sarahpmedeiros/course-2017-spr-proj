import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

from random import shuffle
from math import sqrt

def permute(x):
    shuffled = [xi for xi in x]
    shuffle(shuffled)
    return shuffled

def avg(x): # Average
    return sum(x)/len(x)

def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

def cov(x, y): # Covariance.
    return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

def corr(x, y): # Correlation coefficient.
    if stddev(x)*stddev(y) != 0:
        return cov(x, y)/(stddev(x)*stddev(y))

def p(x, y):
    c0 = corr(x, y)
    corrs = []
    for k in range(0, 2000):
        y_permuted = permute(y)
        corrs.append(corr(x, y_permuted))
    return len([c for c in corrs if abs(c) > c0])/len(corrs)


class corr_gradrates(dml.Algorithm):
    contributor = 'hschurma_rcalleja'
    reads = ['hschurma_rcalleja.funding_gradrates']
    writes = ['hschurma_rcalleja.corr_gradrates']
    
    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hschurma_rcalleja', 'hschurma_rcalleja')

        repo.dropPermanent("corr_gradrates")
        repo.createPermanent("corr_gradrates")

        
        trial = False

        fund_grad = []
        if trial == True:
            fund_grad.append(repo.hschurma_rcalleja.funding_gradrates.find_one({}))
        else:
            fund_grad = list(repo.hschurma_rcalleja.funding_gradrates.aggregate([{"$project":{"_id":0}}]))
        #print(fund_grad)

        correlation = []
        for i in range(len(fund_grad)):
            x = []
            y = []
            grad = fund_grad[i]['Graduation Rates']
            for g in grad:
                if grad[g] == '-' or grad[g] == '':
                    grad[g] = '0'
                x.append(float(grad[g]))

            fund = fund_grad[i]['Funding']
            for f in fund:
                y.append(int(fund[f].replace("$","").replace(",","")))
            
            correlation.append({'School Name': fund_grad[i]['Name'], 'SAT_Funding Correlation': corr(x,y)})

        #print(correlation)        

        repo.dropCollection('corr_gradrates')
        repo.createCollection('corr_gradrates')
        repo['hschurma_rcalleja.corr_gradrates'].insert(correlation)

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
    
        this_script = doc.agent('alg:hschurma_rcalleja#corr_gradrates', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        funding_gradrates = doc.entity('dat:hschurma_rcalleja#funding_gradrates', {'prov:label':'Funding and Gradrates', \
            prov.model.PROV_TYPE:'ont:DataSet'})
        
        get_corr_grad = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_corr_fund, this_script)

        doc.used(get_corr_grad, funding_gradrates, startTime)

        corr_grad = doc.entity('dat:hschurma_rcalleja#corr_gradrates', {prov.model.PROV_LABEL:'High School Funding and Graduation Data Correlation', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(corr_grad, this_script)
        doc.wasGeneratedBy(corr_grad, get_corr_grad, endTime)
        
        doc.wasDerivedFrom(corr_grad, funding_gradrates, get_corr_grad, get_corr_grad, get_corr_grad)
        
        repo.record(doc.serialize())
        repo.logout()

        return doc
                  

#corr_gradrates.execute()
doc = corr_gradrates.provenance()
#print(json.dumps(json.loads(doc.serialize()), indent=4))
        
        


