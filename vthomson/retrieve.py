import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

class retrieve(dml.Algorithm):
    contributor = 'vthomson'
    reads = []
    writes = ['vthomson.cambridge_crime_reports', #cambridge_crime
              'vthomson.commonwealth_connect', #comm_connect
              'vthomson.daily_traffic', #cambridge_traffic
              'vthomson.waze_jams', #boston_traffic
              'vthomson.boston_crime_reports'] #boston_crime


    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('vthomson', 'vthomson')

        cred = dml.auth

        city_of_boston_datasets = {

            "waze_jam": "dih6-az4h",
            "boston_crime_reports": "crime"

        }

        cambridge_datasets = {

            "cambridge_crime_reports": "dypy-nwuz",
            "daily_traffic": "aeey-t2nm"
        }

        massdata = {

            "commonwealth_connect": "cb3u-xiib"
        }


        #1. DO THIS FOR EVERY DATABASE #WAZE JAM DATA
        url = 'https://data.cityofboston.gov/resource/dih6-az4h.json?$limit=1000000'
        response = urllib.request.urlopen(url).read().decode("utf-8") #maybe change
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("boston_traffic")
        repo.createCollection("boston_traffic")
        repo['vthomson.boston_traffic'].insert_many(r)
        repo['vthomson.boston_traffic'].metadata({'complete':True})
        print(repo['vthomson.boston_traffic'].metadata())


        #2. DO THIS FOR EVERY DATABASE #BOSTON CRIME INCIDENT REPORTS (JULY 2012 - AUGUST 2015)
        url = 'https://data.cityofboston.gov/resource/crime.json?$limit=50000'
        response = urllib.request.urlopen(url).read().decode("utf-8") #maybe change
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("boston_crime") 
        repo.createCollection("boston_crime")
        repo['vthomson.boston_crime'].insert_many(r)
        repo['vthomson.boston_crime'].metadata({'complete':True})
        print(repo['vthomson.boston_crime'].metadata())


        #3. DO THIS FOR EVERY DATABASE #COMMONWEALTH CONNECT REPORTS WITHIN MASSACHUSETTS STATE
        url = 'https://data.mass.gov/resource/cb3u-xiib.json?$limit=1000000'
        response = urllib.request.urlopen(url).read().decode("utf-8") #maybe change
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("comm_connect")
        repo.createCollection("comm_connect")
        repo['vthomson.comm_connect'].insert_many(r)
        repo['vthomson.comm_connect'].metadata({'complete':True})
        print(repo['vthomson.comm_connect'].metadata())

        #4. DO THIS FOR EVERY DATABASE #CAMBRIDGE CRIME REPORTS
        url = 'https://data.cambridgema.gov/resource/dypy-nwuz.json?$limit=1000000'
        response = urllib.request.urlopen(url).read().decode("utf-8") #maybe change
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("cambridge_crime")
        repo.createCollection("cambridge_crime")
        repo['vthomson.cambridge_crime'].insert_many(r)
        repo['vthomson.cambridge_crime'].metadata({'complete':True})
        print(repo['vthomson.cambridge_crime'].metadata())

        #5. DO THIS FOR EVERY DATABASE #DAILY TRAFFIC
        url = 'https://data.cambridgema.gov/resource/aeey-t2nm.json?$limit=1000000'
        response = urllib.request.urlopen(url).read().decode("utf-8") #maybe change
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("cambridge_traffic")
        repo.createCollection("cambridge_traffic")
        repo['vthomson.cambridge_traffic'].insert_many(r)
        repo['vthomson.cambridge_traffic'].metadata({'complete':True})
        print(repo['vthomson.cambridge_traffic'].metadata())
    

        repo.logout()

        endTime = datetime.datetime.now()

        return{"start":startTime, "end":endTime}

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
        repo.authenticate('vthomson', 'vthomson')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/vthomson') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/vthomson') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/resource/')
        doc.add_namespace('madata', 'https://data.mass.gov/resource/')

        this_script = doc.agent('alg:vthomson#retrieve', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        resource1 = doc.entity('bdp:dih6-az4h', {'prov:label':'Waze Jam Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_waze_jams = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_waze_jams, this_script)
        doc.usage(get_waze_jams, resource1, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval',
                #'ont:Query':''
                }
                )
        
        resource2 = doc.entity('cdp:crime', {'prov:label':'Boston Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_boston_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_boston_crime, this_script)
        doc.usage(get_boston_crime, resource2, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                #'ont:Query':''
                }
                )

        resource3 = doc.entity('madata:cb3u-xiib', {'prov:label':'Commonwealth Connect reports within Massachusetts State', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_comm_connect = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_comm_connect, this_script)
        doc.usage(get_comm_connect, resource3, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                #'ont:Query':''
                }
                )
        
        resource4 = doc.entity('cdp:dypy-nwuz', {'prov:label': 'Cambridge Crime Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_cambridge_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_cambridge_crime, this_script)
        doc.usage(get_cambridge_crime, resource4, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                #'ont:Query':''
                }
                )

        resource5 = doc.entity('cdp:aeey-t2nm', {'prov:label':'Average Daily Traffic Counts', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_cambridge_traffic = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_cambridge_traffic, this_script)
        doc.usage(get_cambridge_traffic, resource5, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'
                #'ont:Query':''
                }
                )

        waze_jams = doc.entity('dat:vthomson#waze_jams', {prov.model.PROV_LABEL:'Waze Jams', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        doc.wasAttributedTo(waze_jams, this_script)
        doc.wasGeneratedBy(waze_jams, get_waze_jams, endTime)
        doc.wasDerivedFrom(waze_jams, resource1, get_waze_jams, get_waze_jams, get_waze_jams)

        boston_crime = doc.entity('dat:vthomson#boston_crime', {prov.model.PROV_LABEL:'Boston Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        doc.wasAttributedTo(boston_crime, this_script)
        doc.wasGeneratedBy(boston_crime, get_boston_crime, endTime)
        doc.wasDerivedFrom(boston_crime, resource2, get_boston_crime, get_boston_crime, get_boston_crime)

        comm_connect = doc.entity('dat:vthomson#comm_connect', {prov.model.PROV_LABEL:'Commonwealth Connect reports within Massachusetts State', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        doc.wasAttributedTo(comm_connect, this_script)
        doc.wasGeneratedBy(comm_connect, get_comm_connect, endTime)
        doc.wasDerivedFrom(comm_connect, resource3, get_comm_connect, get_comm_connect, get_comm_connect)

        cambridge_crime = doc.entity('dat:vthomson#cambridge_crime', {prov.model.PROV_LABEL:'Cambridge Crime Reports', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        doc.wasAttributedTo(cambridge_crime, this_script)
        doc.wasGeneratedBy(cambridge_crime, get_cambridge_crime, endTime)
        doc.wasDerivedFrom(cambridge_crime, resource4, get_cambridge_crime, get_cambridge_crime, get_cambridge_crime)

        cambridge_traffic = doc.entity('dat:vthomson#cambridge_traffic', {prov.model.PROV_LABEL:'Average Daily Traffic Counts', prov.model.PROV_TYPE:'ont:DataSet', 'ont:Extension':'json'})
        doc.wasAttributedTo(cambridge_traffic, this_script)
        doc.wasGeneratedBy(cambridge_traffic, get_cambridge_traffic, endTime)
        doc.wasDerivedFrom(cambridge_traffic, resource5, get_cambridge_traffic, get_cambridge_traffic, get_cambridge_traffic)

        repo.logout()

        return doc

retrieve.execute()
doc = retrieve.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
