
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class get(dml.Algorithm):
    contributor = 'houset_karamy'
    reads = []
    writes = ['houset_karamy.policeStations','houset_karamy.crimeReportsBoston', 'houset_karamy.crimeReportsCambridge', 'houset_karamy.policeCarRoutesCambridge', 'houset_karamy.policeWalkingRoutesCambridge','houset_karamy.realTimeTravelMassdot']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('houset_karamy', 'houset_karamy')
        
        # Data for Police Stations in Boston
        url = 'https://data.cityofboston.gov/resource/pyxn-r3i2.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("policeStations")
        repo.createCollection("policeStations")
        repo['houset_karamy'].insert_many(r)
        repo['houset_karamy'].metadata({'complete':True})
        print(repo['houset_karamy.policeStations'].metadata())
        
        # Data for Crime Reports in Boston
        url = 'https://data.cityofboston.gov/resource/crime.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crimeReportsBoston")
        repo.createCollection("crimeReportsBoston")
        repo['houset_karamy.crimeReportsBoston'].insert_many(r)
        
        # Data for Crime Reports in Cambridge
        url = 'https://data.cambridgema.gov/resource/dypy-nwuz.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crimeReportsCambridge")
        repo.createCollection("crimeReportsCambridge")
        repo['houset_karamy.crimeReportsCambridge'].insert_many(r)
        
        # Data for Police Car Routes in Cambridge
        url = 'https://data.cambridgema.gov/api/views/svjm-6war.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("policeCarRoutesCambridge")
        repo.createCollection("policeCarRoutesCambridge")
        repo['houset_karamy.policeCarRoutesCambridge'].insert_many(r)
        
        # Data for Police Walking Routes in Cambridge
        url = 'https://data.cambridgema.gov/api/views/aaqv-qhr2.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("policeWalkingRoutesCambridge")
        repo.createCollection("policeWalkingRoutesCambridge")
        repo['houset_karamy.policeWalkingRoutesCambridge'].insert_many(r)
       #******* I PUT IN THE LINKS FOR A FEW DATA SETS I FOUND RELEVANT*******
       #******* JUST TO KEEP IT SIMPLER FOR US I DIDNT USE ALL THE ONES WE TALKED ABOUT********
       #******* NEED HELP INSERTING MBTA ONE THOUGH. NOT SURE IF IT WORKS. NEED TO INCORPORATE PROVENANCE. IDEK ANYMORE*******
    
       # url = 'http://cs-people.bu.edu/lapets/591/examples/found.json'
        #response = urllib.request.urlopen(url).read().decode("utf-8")
        #r = json.loads(response)
        #s = json.dumps(r, sort_keys=True, indent=2)
        #repo.dropCollection("found")
        #repo.createCollection("found")
        #repo['houset_karamy.found'].insert_many(r)

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
        repo.authenticate('houset_karamy', 'houset_karamy')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        doc.add_namespace('mbta', 'http://realtime.mbta.com/developer/api/v2/r') # MBTA API
        doc.add_namspace('cma', 'https://data.cambridgema.gov/browse')
        
      #writes = ['houset_karamy.policeStations','houset_karamy.crimeReportsBoston', 'houset_karamy.crimeReportsCambridge', 'houset_karamy.policeCarRoutesCambridge', 'houset_karamy.policeWalkingRoutesCambridge','houset_karamy.realTimeTravelMassdot']

            
        this_script = doc.agent('alg:houset_karamy#get', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource1 = doc.entity('bdp:pyxn-r3i2', {'prov:label':'Police Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource2 = doc.entity('bdp:crime', {'prov:label':'Crime Reports Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource3 = doc.entity('cma:dypy-nwuz', {'prov:label':'Crime Reports Cambridge', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource4 = doc.entity('cma:svjm-6war', {'prov:label':'Police Car Routes Cambridge', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource5 = doc.entity('cma:aaqv-qhr2', {'prov:label':'Police Walking Routes Cambridge', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
                        
        get_policeStations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_crimeReportsBoston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_crimeReportsCambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_policeCarRoutesCambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_policeWalkingRoutesCambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
#         get_realTimeTravelMassDot = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_policeStations, this_script)
        doc.wasAssociatedWith(get_crimeReportsBoston, this_script)
        doc.wasAssociatedWith(get_crimeReportsCambridge, this_script)
        doc.wasAssociatedWith(get_policeCarRoutesCambridge, this_script)
        doc.wasAssociatedWith(get_policeWalkingRoutesCambridge, this_script)
#         doc.wasAssociatedWith(get_realTimeTravelMassDot, this_script)
        
        doc.usage(get_policeStations, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'#,
#                   'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_crimeReportsBoston, resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'#,
#                   'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_crimeReportsCambridge, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'#,
#                   'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_policeCarRoutesCambridge, resource4, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'#,
#                   'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_policeWalkingRoutesCambridge, resource5, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'#,
#                   'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
#         doc.usage(get_realTimeTravelMassDot, resource, startTime, None,
#                   {prov.model.PROV_TYPE:'ont:Retrieval'#,
# #                   'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
#                   }
#                   )
#         doc.usage(get_found, resource, startTime, None,
#                   {prov.model.PROV_TYPE:'ont:Retrieval',
#                   'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
#                   }
#                   )
#         doc.usage(get_lost, resource, startTime, None,
#                   {prov.model.PROV_TYPE:'ont:Retrieval',
#                   'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
#                   }
#                   )

           
        policeStations = doc.entity('dat:houset_karamy#policeStations', {prov.model.PROV_LABEL:'Police Station Locations', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(policeStations, this_script)
        doc.wasGeneratedBy(policeStations, get_policeStations, endTime)
        doc.wasDerivedFrom(policeStations, resource1, get_policeStations, get_policeStations, get_policeStations) 
        
        crimeReportsBoston = doc.entity('dat:houset_karamy#crimeReportsBoston', {prov.model.PROV_LABEL:'Boston Crime Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimeReportsBoston, this_script)
        doc.wasGeneratedBy(crimeReportsBoston, get_crimeReportsBoston, endTime)
        doc.wasDerivedFrom(crimeReportsBoston, resource2, get_crimeReportsBoston, get_crimeReportsBoston, get_crimeReportsBoston) 
        
        crimeReportsCambridge = doc.entity('dat:houset_karamy#crimeReportsCambridge', {prov.model.PROV_LABEL:'Cambridge Crime Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimeReportsCambridge, this_script)
        doc.wasGeneratedBy(crimeReportsCambridge, get_crimeReportsCambridge, endTime)
        doc.wasDerivedFrom(crimeReportsCambridge, resource3, get_crimeReportsCambridge, get_crimeReportsCambridge, get_crimeReportsCambridge)
        
        policeCarRoutesCambridge = doc.entity('dat:houset_karamy#policeCarRoutesCambridge', {prov.model.PROV_LABEL:'Cambridge Police Car Routes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(policeCarRoutesCambridge, this_script)
        doc.wasGeneratedBy(policeCarRoutesCambridge, get_policeCarRoutesCambridge, endTime)
        doc.wasDerivedFrom(policeCarRoutesCambridge, resource4, get_policeCarRoutesCambridge, get_policeCarRoutesCambridge, get_policeCarRoutesCambridge)
        
        policeWalkingRoutesCambridge = doc.entity('dat:houset_karamy#policeWalkingRoutesCambridge', {prov.model.PROV_LABEL:'Cambridge Police Walking Routes', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(policeWalkingRoutesCambridge, this_script)
        doc.wasGeneratedBy(policeWalkingRoutesCambridge, get_policeWalkingRoutesCambridge, endTime)
        doc.wasDerivedFrom(policeWalkingRoutesCambridge, resource5, get_policeWalkingRoutesCambridge, get_policeWalkingRoutesCambridge, get_policeWalkingRoutesCambridge)
        
#         realTimeTravelMassDot = doc.entity('dat:houset_karamy#realTimeTravelMassDot', {prov.model.PROV_LABEL:'MBTA Travel Times', prov.model.PROV_TYPE:'ont:DataSet'})
#         doc.wasAttributedTo(realTimeTravelMassDot, this_script)
#         doc.wasGeneratedBy(realTimeTravelMassDot, get_realTimeTravelMassDot, endTime)
#         doc.wasDerivedFrom(realTimeTravelMassDot, resource, get_realTimeTravelMassDot, get_realTimeTravelMassDot, get_realTimeTravelMassDot)
        
        
#         lost = doc.entity('dat:houset_karamy#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
#         doc.wasAttributedTo(lost, this_script)
#         doc.wasGeneratedBy(lost, get_lost, endTime)
#         doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

#         found = doc.entity('dat:houset_karamy#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
#         doc.wasAttributedTo(found, this_script)
#         doc.wasGeneratedBy(found, get_found, endTime)
#         doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

get.execute()
doc = get.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
