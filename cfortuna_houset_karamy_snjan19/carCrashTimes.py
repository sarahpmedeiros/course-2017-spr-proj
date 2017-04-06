import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import csv
import json
import requests 
from collections import *
import math

class carCrashTimes(dml.Algorithm):
    contributor = 'cfortuna_houset_karamy_snjan19'
    reads = ['cfortuna_houset_karamy_snjan19.CarCrashData']
    writes = ['cfortuna_houset_karamy_snjan19.CrashTimes']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cfortuna_houset_karamy_snjan19', 'cfortuna_houset_karamy_snjan19')

        # Trial Mode limits the data points on which to run execution if trial parameter set to True
        repoData = repo['cfortuna_houset_karamy_snjan19.CarCrashData'].find().limit(10) if trial else repo['cfortuna_houset_karamy_snjan19.CarCrashData'].find()

        # Categorize the times into the hour that the crash has occured
        times = []
        for element in repoData:
            time_string = element["Crash Time"]
            time_split = time_string.split(" ")

            if time_split[1] == "PM" and int(time_split[0].split(":")[0]) != 12:
                time_digit = int(time_split[0].split(":")[0]) + 12
            else:
                time_digit = int(time_split[0].split(":")[0])
            
            times.append(time_digit)

        # Count the amount of car crashes that appear in the hour ranges
        count = Counter(times)
        x = []
        y = []
        for key, value in count.items():
            x.append(key)
            y.append(value)

        # Calculate the linear regression on the data set
        meanX = sum(x) * 1.0 / len(x)
        meanY = sum(y) * 1.0 / len(y)

        varX = sum([(v - meanX)**2 for v in x])
        varY = sum([(v - meanY)**2 for v in y])

        minYHatCov = sum([(x[i] - meanX) * (y[i] - meanY) for i in range(len(y))])

        slope = minYHatCov / varX
        intercept = meanY - slope * meanX

        MSE = sum([(y[i] - (slope * x[i] + intercept))**2 for i in range(len(x))]) * 1.0 / len(x)
        RMSE = math.sqrt(MSE)
        lineOfBestFit = "y = " + str(slope) + "x + " + str(intercept)

        # Place the results of the linear regression into Mongo
        result = {"line of best fit": lineOfBestFit, "slope": slope , "intercept": intercept, "mean square error": MSE, "root mean square error": RMSE}
    
        repo.dropCollection("CrashTimes")
        repo.createCollection("CrashTimes")

        repo['cfortuna_houset_karamy_snjan19.CrashTimes'].insert(result)

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    """Provenance of this Document"""
    """Provenance of this Document"""
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
        repo.authenticate('cfortuna_houset_karamy_snjan19', 'cfortuna_houset_karamy_snjan19')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('mag', 'https://data.mass.gov/resource/')
        doc.add_namespace('car', 'http://datamechanics.io/data/cfortuna_houset_karamy_snjan19/')

        this_script = doc.agent('alg:cfortuna_houset_karamy_snjan19#retrieveData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        

        carCrashResource = doc.entity('car:CarCrashData', {'prov:label':'Car Crash Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        crashTimesResource = doc.entity('dat:cfortuna_houset_karamy_snjan19#CrashTimes', {'prov:label':'Crash Time Data', prov.model.PROV_TYPE:'ont:DataResource'})

        getCarCrashData = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)  
        getCrashTimes= doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime) 
        

        doc.wasAssociatedWith(getCarCrashData, this_script)
        doc.wasAssociatedWith(getCrashTimes, this_script)

        doc.usage(getCarCrashData, carCrashResource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(getCrashTimes, crashTimesResource, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        '''I NEED INPUT ON THE USAGE PART FOR getCrashTimes PLEASE'''

        CarCrashData = doc.entity('dat:cfortuna_houset_karamy_snjan19#CarCrashData', {prov.model.PROV_LABEL:'Car Crash Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(CarCrashData, this_script)
        doc.wasGeneratedBy(CarCrashData, getCarCrashData, endTime)
        doc.wasDerivedFrom(CarCrashData, carCrashResource, getCarCrashData, getCarCrashData, getCarCrashData)

        CrashTimes = doc.entity('dat:cfortuna_houset_karamy_snjan19#CrashTimes', {prov.model.PROV_LABEL:'Crash Time Data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(CrashTimes, this_script)
        doc.wasGeneratedBy(CrashTimes, getCrashTimes, endTime)            
        doc.wasDerivedFrom(CrashTimes, crashTimesResource, getCrashTimes, getCrashTimes, getCrashTimes)


        repo.logout()
                  
        return doc

carCrashTimes.execute()
# doc = retrieveData.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
