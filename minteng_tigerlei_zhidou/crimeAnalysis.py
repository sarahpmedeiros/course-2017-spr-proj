import dml, prov.model
import datetime
import numpy as np
from scipy import stats


class crimeAnalysis(dml.Algorithm):
    contributor = 'minteng_tigerlei_zhidou'
    reads = []
    writes = ['minteng_tigerlei_zhidou.rent']

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

        corrfP = [ [stats.pearsonr(crimeVector[i][k], crimeVector[i + 1][k]) for k in range(districtNum)] for i in range(5) ]

        print("                    corrf             p-value      ")
        for i in range(5):
            for block, dnum in zip(corrfP[i], range(districtNum)):
                print(str(i + 12) + ' - block {}:  '.format(dnum) + str(block[0]) + "   " + str(block[1]))


        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):  
        return doc

crimeAnalysis.execute()