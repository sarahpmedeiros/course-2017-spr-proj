import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import requests
import numpy as np

class linearRegressionObesityTime(dml.Algorithm):
        contributor = 'asafer_asambors_maxzm_vivyee'
        reads = ['asafer_asambors_maxzm_vivyee.obesity_time']
        writes = ['asafer_asambors_maxzm_vivyee.obesity_time_linear_regression_data']

        @staticmethod
        def execute(trial=False):
                startTime = datetime.datetime.now()

                #set up the connection
                client = dml.pymongo.MongoClient()
                repo = client.repo
                repo.authenticate('asafer_asambors_maxzm_vivyee','asafer_asambors_maxzm_vivyee')

                #loads
                obesity_time = repo['asafer_asambors_maxzm_vivyee.obesity_time'].find()

                obesity_time_tuples = [[a['time'],a['data_value']] for a in obesity_time]

                X = np.array(obesity_time_tuples)[:,0] # x will be the time it takes to get to a healthy location
                Y = np.array(obesity_time_tuples)[:,1] # y is going to be the obesity percent

                # we are going to run a linear regression that predicts the current level of obesity given how long it takes to get somewhere healthy

                meanX = sum(X)*1.0/len(X)
                meanY = sum(Y)*1.0/len(Y)

                varX = sum([(v-meanX)**2 for v in X])
                varY = sum([(v-meanY)**2 for v in Y])

                minYHatCov = sum([(X[i]-meanX)*(Y[i]-meanY) for i in range(len(Y))])

                B1 = minYHatCov/varX
                B0 = meanY-B1*meanX

                #analysis on regression
                MSE = sum([(Y[i]-(B1*X[i]+B0)**2) for i in range(len(X))])*1.0/len(X)
                RMSE = math.sqrt(MSE)

                #send data up
                stats = {"B1":B1,"B0":B0, "MSE" : MSE, "RMSE":RMSE}
                repo.dropCollection('asafer_asambors_maxzm_vivyee.obesity_time_linear_regression_data')
                repo.createCollection('asafer_asambors_maxzm_vivyee.obesity_time_linear_regression_data')

                repo['asafer_asambors_maxzm_vivyee.obesity_time_linear_regression_data'].insert(stats)
                repo['asafer_asambors_maxzm_vivyee.obesity_time_linear_regression_data'].metadata({'complete': True})

                endTime = datetime.datetime.now()

                print('all uploaded: linearRegressionObesityTime')
                return {"start":startTime, "end":endTime}


        @staticmethod
        def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
                # Set up the database connection.
                client = dml.pymongo.MongoClient()
                repo = client.repo
                repo.authenticate('asafer_asambors_maxzm_vivyee', 'asafer_asambors_maxzm_vivyee')
                doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
                doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
                doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
                doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
                
                this_script = doc.agent('alg:asafer_asambors_maxzm_vivyee#linearRegressionObesityTime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

                get_linear_regression_obesity_time = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

                doc.wasAssociatedWith(get_linear_regression_obesity_time, this_script)

                obesity_time = doc.entity('dat:asafer_asambors_maxzm_vivyee#obesity_time', {prov.model.PROV_LABEL:'Time to get to a healthy location from an obese area (percentage)', prov.model.PROV_TYPE:'ont:DataSet'})
                obesity_time_linear_regression = doc.entity('dat:asafer_asambors_maxzm_vivyee#obesity_time_linear_regression', {prov.model.PROV_LABEL:'Linear regression on time to get to a healthy location from an obese area (percentage)', prov.model.PROV_TYPE:'ont:DataSet'})

                doc.usage(get_linear_regression_obesity_time, obesity_time, startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
                doc.wasAttributedTo(obesity_time_linear_regression, this_script)
                doc.wasGeneratedBy(obesity_time_linear_regression, get_linear_regression_obesity_time, endTime)
                doc.wasDerivedFrom(obesity_time_linear_regression, obesity_time, get_linear_regression_obesity_time, get_linear_regression_obesity_time, get_linear_regression_obesity_time)

                repo.logout()

                return doc


# linearRegressionObesityTime.execute()
