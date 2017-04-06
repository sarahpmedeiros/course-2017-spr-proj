#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import pandas as pd
from pandas import DataFrame as Df
import matplotlib.pyplot as plt
import numpy as np
from sklearn import datasets, linear_model
from sklearn.preprocessing import StandardScaler
from random import shuffle
from math import sqrt

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

def map(f, R):
    return [t for (k,v) in R for t in f(k,v)]
    
def reduce(f, R):
    keys = {k for (k,v) in R}
    return [f(k1, [v for (k2,v) in R if k1 == k2]) for k1 in keys]

def normalizeDict(X, keyName, valName):
    justVals = [int(i.get(valName)) for i in X]
    justKeys = [i.get(keyName) for i in X]
    avg = sum(justVals)/len(justVals)
    normalizedVals = project(justVals, lambda x: x/avg)
    Y = [{"Zip Code":justKeys[i],"Crime Risk Index":normalizedVals[i]} for i in range(len(justKeys))]
    return Y    

def createTiers(X, A, k):
    minMax =  [(min([int(i.get(j)) for i in X]),max([int(i.get(j)) for i in X])) for j in A]
    Y = [0]*(k+1)
    for i in range(len(minMax)):
        interval = ((minMax[i][1] - minMax[i][0])/k)
        Y[i] = [minMax[i][0]+(interval*j) for j in range(k+1)]
    Z = [{A[j]:Y[j]} for j in range(len(A))]
    return Z

def zipToRent(X, A):
    Y = [(i.get("Zip "),i.get(A[j])) for i in X for j in range(len(A))]
    return Y

def assignTier(X, Y, A):
    housingTiers = Y[0].get(A[0])
    Z = [0]*len(X)
    for i in range(len(X)):
        for j in range(len(housingTiers)-1):
            if int(X[i][1]) >= int(housingTiers[j]) and int(X[i][1]) <= int(housingTiers[j+1]):
                Z[i] = {X[i][0]:housingTiers[j]}
                break
    return Z

def pullGrad(X, keyName, valName):
    justVals = [float(i.get(valName)) for i in X]
    justKeys = [i.get(keyName) for i in X]
    Y = [{"Zip Code":justKeys[i],"College_Grad_Rate":justVals[i]} for i in range(len(justKeys))]
    
    return Y

def pullAges(X, keyName, valName):
    justVals = [float(i.get(valName)) for i in X]
    justKeys = [i.get(keyName) for i in X]
    Y = [(justKeys[i],justVals[i]) for i in range(len(justKeys))]
    
    return Y

def pullNeighborhood(X, keyName, valName):
    justVals = [i.get(valName) for i in X]
    justKeys = [i.get(keyName) for i in X]
    Y = [{justKeys[i]:justVals[i]} for i in range(len(justKeys))]
    
    return Y

def maxRate(X):
    max1 = 0
    max2 = 0
    max3 = 0
    max4 = 0
    max5 = 0
    for i in range(len(X)):
        if X[i][0] == 1998:
            max1 = max(max1,X[i][1])
        if X[i][0] == 1806:
            max2 = max(max2,X[i][1])
        if X[i][0] == 1614:
            max3 = max(max3,X[i][1])
        if X[i][0] == 1422:
            max4 = max(max4,X[i][1])
        if X[i][0] == 1230:
            max5 = max(max5,X[i][1])
            
    
    tierToMax = [(1998,max1),(1806,max2),(1614,max3),(1422,max4),(1230,max5)]
    
    return tierToMax

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



class statAnalysis(dml.Algorithm):
    contributor = 'babgi_hvdlaan_jspinell_mpinheir'
    reads = ['babgi_hvdlaan_jspinell_mpinheir.ageRanges',
              'babgi_hvdlaan_jspinell_mpinheir.crimeRate',
              'babgi_hvdlaan_jspinell_mpinheir.educationCosts', 
              'babgi_hvdlaan_jspinell_mpinheir.housingRates',
              'babgi_hvdlaan_jspinell_mpinheir.neighborhoods']
    writes = ['babgi_hvdlaan_jspinell_mpinheir.Predicted_Vs_Actual_Housing_Values',
              'babgi_hvdlaan_jspinell_mpinheir.Correlation_Coefficents']
    
    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('babgi_hvdlaan_jspinell_mpinheir', 'babgi_hvdlaan_jspinell_mpinheir')
    
        # Pull data from the database
        ageRanges = list(repo.babgi_hvdlaan_jspinell_mpinheir.ageRanges.find())
        educationCosts = list(repo.babgi_hvdlaan_jspinell_mpinheir.educationCosts.find())
        crimeRate = list(repo.babgi_hvdlaan_jspinell_mpinheir.crimeRate.find())
        housingRates = list(repo.babgi_hvdlaan_jspinell_mpinheir.housingRates.find())
        neighborhoods = list(repo.babgi_hvdlaan_jspinell_mpinheir.neighborhoods.find())
        
        newCrimeRate = normalizeDict(crimeRate, "Zip Code", "Crime Risk Index")
        gradRates = pullGrad(educationCosts, "Zip ", "College Grad Rate")
        typesOfHomes = ["Average Rent 2 Bedroom"]
        zipAndRent = zipToRent(housingRates, typesOfHomes)
        housingDict = [{"Zip Code": x,"Rent":int(y)} for (x,y) in zipAndRent]
        
        # Converts the list of dictionaries into a list of Tuples
        ageRangeList = [[(list(ageRanges[i].keys())[j],
                          float(list(ageRanges[i].values())[j])) for j in range(1,19)] 
                          for i in range(len(ageRanges))]
        
        # Returns an array of tuples where the 1st value is the most common age
        # range for a zip, and the 2nd value is the percentage of people in that age 
        maxAges = [max(ageRangeList[i][2:], key=lambda x: x[1])
                          for i in range(len(ageRangeList))]
        
        # Returns an array of dicts of the most common age, and the zip code for that age
        avgAgePerMax = [{'Zip Code':ageRanges[i]['Zip '],
            "Most Popular Age":(int(maxAges[i][0][:2])+int(maxAges[i][0][6:8]))/2}
            for i in range(len(maxAges))]
        
        crimeDf = Df(newCrimeRate)
        educationDf = Df(gradRates)
        housingDf = Df(housingDict)
        ageDf = Df(avgAgePerMax)
        mergedData = pd.merge(left=crimeDf,right=educationDf,on='Zip Code',how='inner')
        mergedData2 = pd.merge(left=mergedData,right=housingDf,on='Zip Code',how='inner')
        finalData = pd.merge(left=mergedData2,right=ageDf,on='Zip Code',how='inner')
        finalDataNoZip = finalData.drop("Zip Code",1)
        
        y = list(finalDataNoZip["Rent"])
        x = list(finalDataNoZip["Crime Risk Index"])
        f = list(finalDataNoZip["Most Popular Age"])
        h = list(finalDataNoZip["College_Grad_Rate"])
        
        x_train = finalDataNoZip.drop("Rent",1)
        y_train = finalDataNoZip["Rent"]
        
        
        # Create linear regression object
        regr = linear_model.LinearRegression()
        
        # Train the model using the training sets
        regr.fit(x_train, y_train)
        ypred=regr.predict(x_train)
        # The coefficients
        #print('Coefficients: \n', regr.coef_)
        # The mean square error
        
        c=regr.coef_
        #print("Price="+str(int(c[0]))+"*X1 + "+str(int(c[1]))+"*X2 + "+str(int(c[2]))+"*X3")
        #print("Where X1: Crime Rates, X2: Most Popular Age, X3: College Graduation" )
        
        #print("Residual sum of squares: %.2f"
        #      % np.mean((ypred - y_train) ** 2))
        # Explained variance score: 1 is perfect prediction
        #print('Variance score: %.2f' % regr.score(x_train, y_train))
        
        #print('Correlation value between crime rates and housing prices: ', corr(x,y))
        #print('Correlation value between most popular age and housing prices: ', corr(f,y))
        #print('Correlation value between college graduation rates and housing prices: ', corr(h,y))
        
        
        #print(y_train)
        #print(ypred)
        
        crimeToHousing = corr(x,y)
        ageToHousing = corr(f,y)
        gradToHousing = corr(h,y)
                
        toPush = [{"Crime To Housing Correlation Coefficient":str(crimeToHousing), 
                   "Age To Housing Correlation Coefficient": str(ageToHousing),
                   "Graduate Rate to Housing Correlation Coefficient": str(gradToHousing)}]
        repo.dropCollection('Correlation_Coefficients')
        repo.createCollection('Correlation_Coefficients')
        repo['babgi_hvdlaan_jspinell_mpinheir.Correlation_Coefficients'].insert_many(toPush)
        
        
        square=((ypred-y_train)*(ypred-y_train))
        mse=(np.sum(square))/len(y_train)
        
        #print("mse is : " ,mse)
        
        yavg=np.sum(y_train)/len(y_train)
        
        variance=(y_train-yavg)*(y_train-yavg)
        
        var=np.sum(variance)/len(y_train)
        
        r2=1-mse/var
        
        #print("r2 is: ", r2)
        #print(ypred)
        
        toPush = [{"Zip Code":finalData["Zip Code"][i], "Actual Rent":str(y[i]), "Predicted Rent":str(ypred[i])} 
            for i in range(len(y))]
        
        repo.dropCollection('Predicted_Vs_Actual_Housing_Values')
        repo.createCollection('Predicted_Vs_Actual_Housing_Values')
        repo['babgi_hvdlaan_jspinell_mpinheir.Predicted_Vs_Actual_Housing_Values'].insert_many(toPush)
        
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
        repo.authenticate('babgi_hvdlaan_jspinell_mpinheir', 'babgi_hvdlaan_jspinell_mpinheir')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/jspinell_mpinheir') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        

        this_script = doc.agent('alg:babgi_hvdlaan_jspinell_mpinheir#statAnalysis', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        crimeRate_ent = doc.entity('dat:crimeRates', {prov.model.PROV_LABEL:'Crime Risk Index Dataset', prov.model.PROV_TYPE:'ont:DataSet'})
        housingRate_ent = doc.entity('dat:housingRate', {prov.model.PROV_LABEL:'Average Housing Rates Dataset', prov.model.PROV_TYPE:'ont:DataSet'})
        educationCosts_ent = doc.entity('dat:educateCosts', {prov.model.PROV_LABEL:'Education Costs Dataset', prov.model.PROV_TYPE:'ont:DataSet'})
        ageRanges_ent = doc.entity('dat:ageRanges', {prov.model.PROV_LABEL:'Population By Age Ranges Dataset', prov.model.PROV_TYPE:'ont:DataSet'})
        resource1 = doc.entity('dat:babgi_hvdlaan_jspinell_mpinheir#finalData', {prov.model.PROV_LABEL:'Crime Rates, Rent, Most Popular Age and Graduation Rates per Zip Code', prov.model.PROV_TYPE:'ont:DataSet'})
        resource2 = doc.entity('dat:babgi_hvdlaan_jspinell_mpinheir#Correlation_Coefficients', {prov.model.PROV_LABEL:'Correlation Coefficients to Housing', prov.model.PROV_TYPE:'ont:DataSet'})
        resource3 = doc.entity('dat:babgi_hvdlaan_jspinell_mpinheir#Predicted_Vs_Actual_Housing_Values', {prov.model.PROV_LABEL:'Actual Vs Predicted Housing Rates', prov.model.PROV_TYPE:'ont:DataSet'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)
        
        doc.used(this_run, resource1, startTime)
        doc.used(this_run, resource2, startTime)
        doc.wasAttributedTo(resource1, this_script)
        doc.wasGeneratedBy(resource1, this_run, endTime)
        
        doc.wasDerivedFrom(crimeRate_ent,resource1,this_run,this_run,this_run)
        doc.wasDerivedFrom(housingRate_ent,resource1,this_run,this_run,this_run)
        doc.wasDerivedFrom(educationCosts_ent,resource1,this_run,this_run,this_run)
        doc.wasDerivedFrom(ageRanges_ent,resource1,this_run,this_run,this_run)
        
        #repo.record(doc.serialize())

        repo.logout()
                  
        return doc
    
"""
statAnalysis.execute()
doc = statAnalysis.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
"""

## eof