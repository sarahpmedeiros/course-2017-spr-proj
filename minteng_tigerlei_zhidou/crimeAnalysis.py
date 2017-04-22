import dml, prov.model
import datetime, uuid
import numpy as np
from scipy import stats
import warnings

class crimeAnalysis(dml.Algorithm):
    contributor = 'minteng_tigerlei_zhidou'
    reads = ['minteng_tigerlei_zhidou.crime', 'minteng_tigerlei_zhidou.box_count']
    writes = ['minteng_tigerlei_zhidou.statsresult']
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets.'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('minteng_tigerlei_zhidou', 'minteng_tigerlei_zhidou')

        # every entry is one year, and inside the year, there will be 
        # the number of crime
        districtNum = repo['minteng_tigerlei_zhidou.box_count'].count()
        crimeVector = np.zeros([6, districtNum, 12])

        for date, dNum in zip(repo['minteng_tigerlei_zhidou.box_count'].find(), range(districtNum)):
            area = repo['minteng_tigerlei_zhidou.crime'].find({'location':{'$geoWithin':{ '$box': date['box']}}})
            for event in area:
                crimeVector[event['year'] - 2012][dNum][event['month'] - 1] += 1


        # compute the correlation coefficient and p-value
        # of same district but different year
        warnings.filterwarnings("ignore")
        corrfP = [ [stats.pearsonr(crimeVector[i][k], crimeVector[i + 1][k]) for k in range(districtNum)] for i in range(5) ]

        # print("                    corrf             p-value      ")
        # for i in range(5):
        #     for block, dnum in zip(corrfP[i], range(districtNum)):
        #         print(str(i + 12) + ' - block {}:  '.format(dnum) + str(block[0]) + "   " + str(block[1]))

        ret = []
        pattern = dict(year = 0, block = 0, corcof = 0, p = 0)
        for i in range(5):
            for block, dnum in zip(corrfP[i], range(districtNum)):
                tempDict = dict(pattern)
                tempDict['year'] = 2012 + i
                tempDict['block'] = dnum
                tempDict['corcof'] = block[0]
                tempDict['p'] = block[1]
                ret.append(tempDict)

        repo.dropCollection("statsresult")
        repo.createCollection("statsresult")
        repo['minteng_tigerlei_zhidou.statsresult'].insert_many(ret)
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
        repo.authenticate('minteng_tigerlei_zhidou', 'minteng_tigerlei_zhidou')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        
        this_script = doc.agent('alg:minteng_tigerlei_zhidou#statsresult', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        statsresult_resource = doc.entity('dat:minteng_tigerlei_zhidou#box_count', {'prov:label':'box count and grade', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        statsresult_resource2 = doc.entity('dat:minteng_tigerlei_zhidou#crime', {'prov:label':'crime dataset', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        statsresult_result = doc.entity('dat:minteng_tigerlei_zhidou#statsresult', {prov.model.PROV_LABEL:'statistics result', prov.model.PROV_TYPE:'ont:DataSet'})

        
        get_statsresult = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_statsresult, this_script)
        doc.usage(get_statsresult, statsresult_resource,startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_statsresult, statsresult_resource2,startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        doc.wasAttributedTo(statsresult_result, this_script)
        doc.wasGeneratedBy(statsresult_result, get_statsresult, endTime)
        doc.wasDerivedFrom(statsresult_result, statsresult_resource, get_statsresult, get_statsresult, get_statsresult)
        doc.wasDerivedFrom(statsresult_result, statsresult_resource2, get_statsresult, get_statsresult, get_statsresult)

        
        repo.logout()
        return doc

crimeAnalysis.execute()