
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class get(dml.Algorithm):
    contributor = 'houset_karamy'
    reads = []
    writes = ['houset_karamy.policeStations','houset_karamy.crimeReportsBoston', 'houset_karamy.crimeReportsCambridge', 'houset_karamy.policeCarRoutesCambridge', 'houset_karamy.policeWalkingRoutesCambridge','houset_karamy.streetsBoston']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('houset_karamy', 'houset_karamy')
        
            dataSets = {'policeStations': 'https://data.cityofboston.gov/resource/pyxn-r3i2.json', 'crimeReportsBoston': 'https://data.cityofboston.gov/resource/crime.json', 'crimeReportsCambridge':'https://data.cambridgema.gov/resource/dypy-nwuz.json','policeCarRoutesCambridge': 'https://data.cambridgema.gov/resource/svjm-6war.json', 'policeWalkingRoutesCambridge' :' https://data.cambridgema.gov/resource/aaqv-qhr2.json', 'streetsBoston' : 'http://data.mass.gov/resource/ms23-5ubn.json'}
            url = dataSets[ds]
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            s = json.dumps(r, sort_keys=True, indent=2)
            repo.dropPermanent(ds)
            repo.createPermanent(ds)
            repo['houset_karamy.' + ds].insert_many(r)
        
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
        doc.add_namspace('cma', 'https://data.cambridgema.gov/resource/')
        doc.add_namespace('mag', 'https://data.mass.gov/resource/')

            
        this_script = doc.agent('alg:houset_karamy#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource1 = doc.entity('bdp:pyxn-r3i2', {'prov:label':'Police Stations', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource2 = doc.entity('bdp:crime', {'prov:label':'Crime Reports Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource3 = doc.entity('cma:dypy-nwuz', {'prov:label':'Crime Reports Cambridge', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource4 = doc.entity('cma:svjm-6war', {'prov:label':'Police Car Routes Cambridge', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource5 = doc.entity('cma:aaqv-qhr2', {'prov:label':'Police Walking Routes Cambridge', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource6 = doc.entity('mag:ms23-5ubn', {'prov:label':'Streets Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})                
        
        get_policeStations = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_crimeReportsBoston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_crimeReportsCambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_policeCarRoutesCambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_policeWalkingRoutesCambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_streetsBoston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
#         get_realTimeTravelMassDot = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_policeStations, this_script)
        doc.wasAssociatedWith(get_crimeReportsBoston, this_script)
        doc.wasAssociatedWith(get_crimeReportsCambridge, this_script)
        doc.wasAssociatedWith(get_policeCarRoutesCambridge, this_script)
        doc.wasAssociatedWith(get_policeWalkingRoutesCambridge, this_script)
        doc.wasAssociatedWith(get_streetsBoston, this_script)
        
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
        doc.usage(get_streetsBoston, resource6, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'#,
#                   'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )


           
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
        
        streetsBoston = doc.entity('dat:houset_karamy#streetsBoston', {prov.model.PROV_LABEL:'Streets Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(streetsBoston, this_script)
        doc.wasGeneratedBy(streetsBoston, get_streetsBoston, endTime)
        doc.wasDerivedFrom(streetsBoston, resource6, get_streetsBoston, get_streetsBoston, get_streetsBoston)         


        repo.logout()
                  
        return doc


get.execute()
doc = get.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
