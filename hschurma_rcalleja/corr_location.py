import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

from math import sin, cos, sqrt, atan2, radians

from random import shuffle

'''methods used for calculating correlation'''
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

'''methods used to find closest schools'''
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


#find distances then sort list and take the last 3 (top 3)

'''methods used to project data sets'''
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

        #grab location, SAT, and graduation data
        #accomodate for trial mode
        #trial = False
        fund_loc = []
        if trial == True:
            fund_loc.append(repo.hschurma_rcalleja.funding_location.find_one({}))
        else:
            fund_loc = list(repo.hschurma_rcalleja.funding_location.aggregate([{"$project":{"_id":0}}]))

        fund_SAT = list(repo.hschurma_rcalleja.funding_SAT.aggregate([{"$project": {"_id": 0}}]))
        fund_grad = list(repo.hschurma_rcalleja.funding_gradrates.aggregate([{"$project":{"_id":0}}]))

        #project grad rates and SAT data together
        P = product(fund_SAT, fund_grad)
        S = select(P, lambda t: t[0]['Name'] == t[1]['Name'])
        PR = project(S, lambda t: {'School Name': t[0]['Name'], 'SAT': t[0]['SAT'], 'Grad Rates': t[1]['Graduation Rates']})
        
        #create lists of schools and coordinates
        coord = []
        for i in fund_loc:
            coord.append({'School Name': i['Name'], 'Location': i['location'][1]})

        #calculate distances between each school
        #accomodate for trial mode
        if trial == True:
            fund_loc = list(repo.hschurma_rcalleja.funding_location.aggregate([{"$project":{"_id":0}}]))
            
        dists = []
        for c in coord:
            locs = []
            for s in fund_loc:
                if c['School Name'] != s['Name']:
                    locs.append({'School Name': c['School Name'], 'Distance': distance(c['Location'],s['location'][1])})
            
            dists.append({'School Name': c['School Name'], 'Distance': locs})

        #find the closest 3 schools to each school
        closest = shortest_dist(dists)

        #include finding
        close_with_fund = {}
        total_fund = {}
        for school in closest:
            name = school['School Name']
            total_fund[name] = {'2008':0, '2009':0,'2010':0, '2011':0,'2012':0, '2013':0,'2014':0, '2015':0, '2016':0}



        #calculate total funding from all three closest schools for each year
        for school in closest:
            for c in school['Closest']:
                name = c['School Name']
                fund = {}
                for i in fund_loc:
                    if i['Name'] == name:
                        fund = {name: i['Funding']}
            
                for j in range(2008,2017):
                    year = str(j)
                    s = school['School Name']

                    if type(fund[name][year]) != int:
                        fund[name][year] = int(fund[name][year].replace("$", "").replace(",", ""))
                    else:
                        fund[name][year] = fund[name][year]

                    total_fund[s][year] += fund[name][year]

        #create list of dictionaries that we can use to project
        tot_fund_final = []
        for f in total_fund:
            tot_fund_final.append({'School Name': f, 'Neighbor Funding': total_fund[f]})

        #project SAT and Grad data with funding data for neighboring schools
        prod_close = product(PR, tot_fund_final)
        sel_close = select(prod_close, lambda t: t[0]['School Name'] == t[1]['School Name'])
        proj_close = project(sel_close, lambda t: {'School Name': t[0]['School Name'], 'SAT': t[0]['SAT'], 'Grad Rates': t[0]['Grad Rates'], 'Neighbor Funding': t[1]['Neighbor Funding']})

        #calculate SAT correlation with total funding from neighboring schools
        SAT_corr = []
        for i in range(len(proj_close)):
            x_scores = []
            y_funds = []
            scores = proj_close[i]['SAT']
            funds = proj_close[i]['Neighbor Funding']
            for j in range(2008, 2017):
                year = str(j)
                if (year in scores.keys() and year in funds.keys()):
                    fund = funds[year]
                    score = scores[year]['Total']
                    x_scores.append(score)
                    y_funds.append(fund)
            #print("Scores ", x_scores, '\n')
            #print("Funds ", y_funds, '\n')
            SAT_corr.append({'School Name': proj_close[i]['School Name'], 'SAT_Neighbor Funding Correlation': corr(x_scores, y_funds)})
        #print(SAT_corr)

        #Calculate grad rates correlation with total funding from neighboring schools
        gradr_corr = []
        for i in range(len(proj_close)):
            x_grad = []
            y_fund = []
            grads = proj_close[i]['Grad Rates']
            for g in grads:
                if grads[g] == '-' or grads[g] == '':
                    grads[g] = '0'
                x_grad.append(float(grads[g]))

            funds = proj_close[i]['Neighbor Funding']
            for j in range(2008, 2017):
                year = str(j)
                if (year in grads.keys() and year in funds.keys()):
                    fund = funds[year]
                    grad = grads[year]
                    x_grad.append(float(grad))
                    y_fund.append(fund)
            gradr_corr.append({'School Name': proj_close[i]['School Name'], 'Grad Rates Correlation': corr(x_grad, y_fund)})


        #project Grad correlation and SAT correlation
        prod_corr = product(SAT_corr, gradr_corr)
        sel_corr = select(prod_corr, lambda t: t[0]['School Name'] == t[1]['School Name'])
        proj_corr = project(sel_corr , lambda t: {'School Name': t[0]['School Name'], 'SAT_Neighbor Funding Correlation': t[0]['SAT_Neighbor Funding Correlation'],
                                                  'Grad Rates Correlation': t[1]['Grad Rates Correlation']})

        #print(proj_corr)

        repo.dropCollection('corr_location')
        repo.createCollection('corr_location')
        repo['hschurma_rcalleja.corr_location'].insert(proj_corr)
        

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
        funding_gradrates = doc.entity('dat:hschurma_rcalleja#funding_gradrates', {'prov:label':'School Funding and Graduation Rates', \
            prov.model.PROV_TYPE:'ont:DataSet'})
        funding_SAT = doc.entity('dat:hschurma_rcalleja#funding_SAT', {'prov:label':'School Funding and SAT data', \
            prov.model.PROV_TYPE:'ont:DataSet'})
        
        
        get_corr_loc = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_corr_loc, this_script)

        doc.used(get_corr_loc, funding_location, startTime)
        doc.used(get_corr_loc, funding_gradrates, startTime)
        doc.used(get_corr_loc, funding_SAT, startTime)

        corr_loc = doc.entity('dat:hschurma_rcalleja#corr_location', {prov.model.PROV_LABEL:'School Funding Correlation with SAT and Graduation Rates based on Location', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(corr_loc, this_script)
        doc.wasGeneratedBy(corr_loc, get_corr_loc, endTime)
        
        doc.wasDerivedFrom(corr_loc, funding_location, get_corr_loc, get_corr_loc, get_corr_loc)
        doc.wasDerivedFrom(corr_loc, funding_gradrates, get_corr_loc, get_corr_loc, get_corr_loc)
        doc.wasDerivedFrom(corr_loc, funding_SAT, get_corr_loc, get_corr_loc, get_corr_loc)

        
        #repo.record(doc.serialize())
        repo.logout()

        return doc
                  

corr_location.execute()
#doc = corr_location.provenance()
#print(json.dumps(json.loads(doc.serialize()), indent=4))
