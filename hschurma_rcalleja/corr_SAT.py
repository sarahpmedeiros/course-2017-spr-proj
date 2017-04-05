import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

from random import shuffle
from math import sqrt

# Working correlation function called pearson_def
import math

def average(x):
    assert len(x) > 0
    return float(sum(x)) / len(x)

def pearson_def(x, y):
    assert len(x) == len(y)
    n = len(x)
    assert n > 0
    avg_x = average(x)
    avg_y = average(y)
    diffprod = 0
    xdiff2 = 0
    ydiff2 = 0
    for idx in range(n):
        xdiff = x[idx] - avg_x
        ydiff = y[idx] - avg_y
        diffprod += xdiff * ydiff
        xdiff2 += xdiff * xdiff
        ydiff2 += ydiff * ydiff
    if (xdiff2 * ydiff2) == 0:
        return 0
    else:
        return diffprod / math.sqrt(xdiff2 * ydiff2)

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

class corr_SAT(dml.Algorithm):
    contributor = 'hschurma_rcalleja'
    reads = ['hschurma_rcalleja.funding_SAT']
    writes = ['hschurma_rcalleja.corr_SAT']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hschurma_rcalleja', 'hschurma_rcalleja')

        repo.dropPermanent("corr_SAT")
        repo.createPermanent("corr_SAT")

        fund_SAT = list(repo.hschurma_rcalleja.funding_SAT.aggregate([{"$project": {"_id": 0}}]))
        print(fund_SAT, '\n')

        correlation = []
        for i in range(len(fund_SAT)):
            x_scores = []
            y_funds = []
            scores = fund_SAT[i]['SAT']
            funds = fund_SAT[i]['Funding']
            for j in range(2008, 2017):
                year = str(j)
                if (year in scores.keys() and year in funds.keys()):
                    fund = funds[year]
                    score = scores[year]['Total']
                    x_scores.append(score)
                    y_funds.append(int(fund.replace("$", "").replace(",", "")))
            print("Scores ", x_scores, '\n')
            print("Funds ", y_funds, '\n')
            correlation.append({'School Name': fund_SAT[i]['Name'], 'SAT_Funding Correlation': pearson_def(x_scores, y_funds)})

        print(correlation)

        repo.dropCollection('corr_SAT')
        repo.createCollection('corr_SAT')
        repo['hschurma_rcalleja.corr_SAT'].insert(correlation)

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        '''Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.'''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hschurma_rcalleja', 'hschurma_rcalleja')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:hschurma_rcalleja#corr_gradrates',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        funding_gradrates = doc.entity('dat:hschurma_rcalleja#funding_gradrates',
                                       {'prov:label': 'Funding and Gradrates', \
                                        prov.model.PROV_TYPE: 'ont:DataSet'})

        get_corr_grad = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_corr_fund, this_script)

        doc.used(get_corr_fund, funding_gradrates, startTime)

        corr_fund = doc.entity('dat:hschurma_rcalleja#corr_gradrates',
                               {prov.model.PROV_LABEL: 'High School Funding and Graduation Data Correlation',
                                prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(corr_fund, this_script)
        doc.wasGeneratedBy(corr_fund, get_corr_fund, endTime)

        doc.wasDerivedFrom(corr_fund, funding_gradrates, get_corr_fund, get_corr_fund, get_corr_fund)

        repo.record(doc.serialize())
        repo.logout()

        return doc


corr_SAT.execute()




