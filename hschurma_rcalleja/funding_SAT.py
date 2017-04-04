import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


# Things to Improve:
# write final database to repo
# go back and remove schools that don't have all 4 SAT scores
# edit school names so that the same school is spelled the same way in both datasets


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
    return [(t, u) for t in R for u in S]


def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k, v) in R if k == key])) for key in keys]


class funding_SAT(dml.Algorithm):
    contributor = 'hschurma_rcalleja'
    reads = ['hschurma_rcalleja.funding', 'hschurma_rcalleja.SAT']
    writes = ['hschurma_rcalleja.funding_SAT']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('hschurma_rcalleja', 'hschurma_rcalleja')

        repo.dropPermanent("funding_SAT")
        repo.createPermanent("funding_SAT")

        # size of SAT data set
        SAT = list(repo.hschurma_rcalleja.SAT.find())
        SATlen = len(SAT)

        # list of schools in form [{'Name': val, 'NumTesting': val, 'Reading': val, 'Math': val, 'Writing': val}]
        SATschools = []
        for i in range(2,SATlen):
            temp = SAT[i]
            SATschools.append({'Name': temp['FIELD1'].strip(), 'NumTesting': temp['FIELD2'], 'Reading': temp['FIELD3'], 'Math': temp['FIELD4'], 'Writing': temp['FIELD5']})

        #print(SATschools)
        # Dict of School name and Funding
        funding = list(repo.hschurma_rcalleja.funding.aggregate([{"$project": {"_id": 0, "FIELD2": 1, "FIELD13": 1}}]))

        # [{'Name': val, 'Funding': val}]
        nameFund = []
        for i in range(len(funding)):
            nameFund.append({'Name': funding[i]["FIELD2"].strip(), 'Funding': funding[i]["FIELD13"].strip()})

        #print(nameFund)
        # print(nameLoc)
        # print(nameFund)

        
        P = product(SATschools, nameFund)
        #print(P)
        S = select(P, lambda t: t[0]['Name'] == t[1]['Name'])
        #print(S)
        PR = project(S, lambda t: {'Name': t[0]['Name'], 'NumTesting': t[0]['NumTesting'], 'Reading': t[0]['Reading'], 'Math': t[0]['Math'], 'Writing': t[0]['Writing'], 'Funding': t[1]['Funding']})
        #print(PR)

        repo.dropCollection('funding_SAT')
        repo.createCollection('funding_SAT')
        repo['hschurma_rcalleja.funding_SAT'].insert(PR)

        # Trim white spaces

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

        this_script = doc.agent('alg:hschurma_rcalleja#funding_SAT',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        funding = doc.entity('dat:hschurma_rcalleja#funding', {prov.model.PROV_LABEL: 'BPS Funding', \
                                                               prov.model.PROV_TYPE: 'ont:DataSet'})
        SAT = doc.entity('dat:hschurma_rcalleja#SAT', {prov.model.PROV_LABEL: 'SAT Scores', \
                                                               prov.model.PROV_TYPE: 'ont:DataSet'})

        get_funding_SAT = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_funding_SAT, this_script)

        doc.used(get_funding_SAT, funding, startTime)
        doc.used(get_funding_SAT, SAT, startTime)

        funding_SAT = doc.entity('dat:hschurma_rcalleja#funding_SAT',
                               {prov.model.PROV_LABEL: 'High School Funding and SAT Score Data',
                                prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(funding_SAT, this_script)
        doc.wasGeneratedBy(get_funding_SAT, endTime)

        doc.wasDerivedFrom(funding_SAT, funding, get_funding_SAT, get_funding_SAT, get_funding_SAT)
        doc.wasDerivedFrom(funding_SAT, SAT, get_funding_SAT, get_funding_SAT, get_funding_SAT)

        repo.record(doc.serialize())
        repo.logout()

        return doc


'''funding_SAT.execute()
doc = funding_SAT.provenance()'''

