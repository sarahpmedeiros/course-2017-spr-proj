import urllib.request
import json
import dml
import prov.model
import datetime 
import uuid
import sys

TRIAL_LIMIT = 5000

class crime(dml.Algorithm):
    contributor = 'minteng_tigerlei_zhidou'
    reads = []
    writes = ['minteng_tigerlei_zhidou.crime']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets.'''
        startTime = datetime.datetime.now()

        print("crime start!")

        if trial:
            print(" Now you are running trial mode")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('minteng_tigerlei_zhidou', 'minteng_tigerlei_zhidou')

        crime = []  
        record = 175000
        limit = 50000

        if trial:
            limit = TRIAL_LIMIT
            record = 5000

        url='https://data.cityofboston.gov/resource/29yf-ye7n.json?$limit='+ str(limit)+ '&$offset='
        for i in range(1, record, limit):
            response = urllib.request.urlopen(url+str(i)).read().decode("utf-8")
            crime_info=json.loads(response)
            for c in crime_info:
                try:
                    temp={}
                    temp['location']=c['location']['coordinates'][::-1]
                    temp['type'] = 'crime'
                    temp['year'] = int(c['year'])
                    temp['month'] = int(c['month'])
                    temp['date'] = c['occurred_on_date'].split('T')[0]
                    crime.append(temp)
                except KeyError:
                    continue

        record = 268000

        if trial:
            limit = TRIAL_LIMIT
            record = 5000

        url='https://data.cityofboston.gov/resource/crime.json?$limit='+ str(limit)+ '&$offset='
        for i in range(1, record, limit):
            response = urllib.request.urlopen(url+str(i)).read().decode("utf-8")
            crime_info=json.loads(response)
            for c in crime_info:
                try:
                    temp={}
                    temp['location']=[float(c['location']['latitude']), float(c['location']['longitude'])]
                    temp['type'] = 'crime'
                    temp['year'] = int(c['year'])
                    temp['month'] = int(c['month'])
                    temp['date'] = c['fromdate'].split('T')[0]
                    crime.append(temp)
                except KeyError:
                    continue

        repo.dropCollection("crime")
        repo.createCollection("crime")
        repo['minteng_tigerlei_zhidou.crime'].insert_many(crime)
        repo.logout()

        print("End!")
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
        doc.add_namespace('bd', 'https://data.cityofboston.gov/resource/')

        
        this_script = doc.agent('alg:minteng_tigerlei_zhidou#crime', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        crime_resource1 = doc.entity('bd:crime', {'prov:label':'Crime Incident 2012-2015', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        crime_resource2 = doc.entity('bd:29yf-ye7n', {'prov:label':'Crime Incident Reports 2015-2017', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(get_crime, crime_resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_crime, crime_resource2, startTime, None,
          {prov.model.PROV_TYPE:'ont:Retrieval'})

        crime = doc.entity('dat:minteng_tigerlei_zhidou#crime', {prov.model.PROV_LABEL:'crime incidents 2012-2017', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime, this_script)
        doc.wasGeneratedBy(crime, get_crime, endTime)
        doc.wasDerivedFrom(crime, crime_resource1, get_crime, get_crime, get_crime)
        doc.wasDerivedFrom(crime, crime_resource2, get_crime, get_crime, get_crime)
        repo.logout()
                  
        return doc

# if 'trial' in sys.argv:
#     crime.execute(True)
# else:
#     crime.execute()
# doc = location.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
