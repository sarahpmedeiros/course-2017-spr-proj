import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class transformation1(dml.Algorithm):
    contributor = 'tigerlei'
    reads = ['tigerlei.crime1','tigerlei.crime2', 'tigerlei.police', 'tigerlei.university', 'tigerlei.homicides']
    writes = ['tigerlei.crime', 'tigerlei.police', 'tigerlei.university', 'tigerlei.homicides']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('tigerlei', 'tigerlei')
        print("Transformation 1 starts!") 

        # Project useful part of crime incidents
        crime1 = []
        crime2 = []
        for item in repo['tigerlei.crime1'].find():
            temp = {}
            
            try:
                temp['district'] = item['reptdistrict']
                temp['coordinates'] = [float(item['location']['longitude']), float(item['location']['latitude'])]
                crime1.append(temp)
            # If part of data was missing, ignore this record
            except KeyError:
                pass
        for item in repo['tigerlei.crime2'].find():
            temp = {}
            try:
                temp['district'] = item['district']
                temp['coordinates'] = item['location']['coordinates']
                crime2.append(temp)
            except KeyError:
                pass
        # Union all crime incidents from 2012 to 2016
        crime = crime1 + crime2
        repo.dropCollection("crime")
        repo.createCollection("crime")
        repo['tigerlei.crime'].insert_many(crime)

        police = []
        # Project police dataset
        for item in repo['tigerlei.police'].find():
            temp = {}
            try:
                temp['zipcode'] = item['ZIP']
                temp['district'] = item['NAME'].split()[1].replace("-", "")
                temp['district'] = temp['district'].replace("Police", "Headquarters")
                temp['coordinates'] = item['coordinates']
                police.append(temp)
            except KeyError:
                pass
        repo.dropCollection("police")
        repo.createCollection("police")
        repo['tigerlei.police'].insert_many(police)

        university = []
        # Project university dataset
        for item in repo['tigerlei.university'].find():
            temp = {}
            try:
                temp['zipcode'] = item['Zipcode']
                temp['coordinates'] = [item['Longitude'], item['Latitude']]
                temp['name'] = item['Name']
                university.append(temp)
            except KeyError:
                pass  

        repo.dropCollection("university")
        repo.createCollection("university")
        repo['tigerlei.university'].insert_many(university)

        homicides = []
        # Project homicides dataset
        for item in repo['tigerlei.homicides'].find():
            temp = {}
            try:
                temp['district'] = item['DIST'].replace('-','')
                homicides.append(temp)
            except KeyError:
                pass

        repo.dropCollection("homicides")
        repo.createCollection("homicides")
        repo['tigerlei.homicides'].insert_many(homicides)

        repo.logout()
        print("Transformation 1 has been done!") 
        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('tigerlei', 'tigerlei')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:tigerlei#transformation1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_run, this_script)

        crime1 = doc.entity('dat:tigerlei#crime1', {'prov:label':'crime incidents 2012-2015', prov.model.PROV_TYPE:'ont:DataSet'})
        crime2 = doc.entity('dat:tigerlei#crime2', {'prov:label':'crime incidents 2015-2016', prov.model.PROV_TYPE:'ont:DataSet'})
        police = doc.entity('dat:tigerlei#police', {'prov:label':'police station in boston', prov.model.PROV_TYPE:'ont:DataSet'})
        university = doc.entity('dat:tigerlei#university', {'prov:label':'colleges and universities in boston', prov.model.PROV_TYPE:'ont:DataSet'})
        homicides = doc.entity('dat:tigerlei#homicides', {'prov:label':'homicides in boston', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.usage(this_run, crime1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(this_run, crime2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(this_run, police, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(this_run, university, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(this_run, homicides, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'})

        crime = doc.entity('dat:tigerlei#crime', {prov.model.PROV_LABEL:'crime incidents 2012-2016', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, this_run, endTime)
        doc.wasDerivedFrom(crime, crime1, this_run, this_run, this_run)
        doc.wasDerivedFrom(crime, crime2, this_run, this_run, this_run)

        doc.wasAttributedTo(police, this_script)
        doc.wasGeneratedBy(police, this_run, endTime)
        doc.wasDerivedFrom(police, police, this_run, this_run, this_run)

        doc.wasAttributedTo(university, this_script)
        doc.wasGeneratedBy(university, this_run, endTime)
        doc.wasDerivedFrom(university, university, this_run, this_run, this_run)

        doc.wasAttributedTo(homicides, this_script)
        doc.wasGeneratedBy(homicides, this_run, endTime)
        doc.wasDerivedFrom(homicides, homicides, this_run, this_run, this_run)        

        repo.logout()
                  
        return doc

# transformation1.execute()
# doc = transformation1.provenance()
# print(doc.get_provn())

# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
