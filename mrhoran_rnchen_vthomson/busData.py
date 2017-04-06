import json
import dml
import prov.model
import datetime
import uuid
import ast
import sodapy
import urllib.request

class getData(dml.Algorithm):

    contributor = 'mrhoran_rnchen_vthomson'
    reads = []
    writes = ['mrhoran_rnchen_vthomson.buses',
              'mrhoran_rnchen_vthomson.schools',
              'mrhoran_rnchen_vthomson.students']

    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()

        repo = client.repo
        
        repo.authenticate('mrhoran_rnchen_vthomson', 'mrhoran_rnchen_vthomson')
    
        #with open('../auth.json') as json_data:
            #credentials = json.load(json_data)

        cred = dml.auth
        
        bus_datasets = {

            "buses": "buses.json",
            "schools":"schools.json",     
            "students": "students.json"

	}

	### DATASETS UPLOADS #################################
        
        url = 'http://datamechanics.io/data/_bps_transportation_challenge/buses.json'
        response = urllib.request.urlopen(url).read().decode("utf-8") #maybe change
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("buses")
        repo.createCollection("buses")
        repo['mrhoran_rnchen_vthomson.buses'].insert_many(r)
        repo['mrhoran_rnchen_vthomson.buses'].metadata({'complete':True})
        
        url = 'http://datamechanics.io/data/_bps_transportation_challenge/schools.json'
        response = urllib.request.urlopen(url).read().decode("utf-8") #maybe change
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("schools")
        repo.createCollection("schools")
        repo['mrhoran_rnchen_vthomson.schools'].insert_many(r)
        repo['mrhoran_rnchen_vthomson.schools'].metadata({'complete':True})

        
        url = 'http://datamechanics.io/data/_bps_transportation_challenge/students.json'
        response = urllib.request.urlopen(url).read().decode("utf-8") #maybe change
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("students")
        repo.createCollection("students")
        repo['mrhoran_rnchen_vthomson.students'].insert_many(r)
        repo['mrhoran_rnchen_vthomson.students'].metadata({'complete':True})



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
            # return doc

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('mrhoran_rnchen_vthomson', 'mrhoran_rnchen_vthomson')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('tpc', 'http://datamechanics.io/?prefix=_bps_transportation_challenge/')

        this_script = doc.agent('alg:mrhoran_rnchen_vthomson#busData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # label section might be wrong
        resource1 = doc.entity('tpc:buses', {'prov:label':'Bus Information', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_buses = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_buses, this_script)

        doc.usage(get_buses, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  #'ont:Query':'location, area, coordinates, zip_code' #?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        resource2 = doc.entity('tpc:schools', {'prov:label':'School Information', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_schools = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_schools, this_script)

        doc.usage(get_schools, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  #'ont:Query':'location, area, coordinates, zip_code' #?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        resource3 = doc.entity('tpc:students', {'prov:label':'Student Information', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_students = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_students, this_script)

        doc.usage(get_students, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  #'ont:Query':'location, area, coordinates, zip_code' #?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
    
        buses = doc.entity('dat:mrhoran_rnchen_vthomson#buses', {prov.model.PROV_LABEL:'Bus Information', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        doc.wasAttributedTo(buses, this_script)
        doc.wasGeneratedBy(buses, get_buses, endTime)
        doc.wasDerivedFrom(buses, resource1, get_buses, get_buses, get_buses)

        schools = doc.entity('dat:mrhoran_rnchen_vthomson#schools', {prov.model.PROV_LABEL:'School Information', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        doc.wasAttributedTo(schools, this_script)
        doc.wasGeneratedBy(schools, get_schools, endTime)
        doc.wasDerivedFrom(schools, resource2, get_schools, get_schools, get_schools)

        students = doc.entity('dat:mrhoran_rnchen_vthomson#students', {prov.model.PROV_LABEL:'Student Information', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        doc.wasAttributedTo(students, this_script)
        doc.wasGeneratedBy(students, get_students, endTime)
        doc.wasDerivedFrom(students, resource3, get_students, get_students, get_students)

        repo.logout()

        return doc

                

getData.execute()
doc = getData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
