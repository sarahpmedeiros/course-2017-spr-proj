import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class get(dml.Algorithm):
    contributor = 'houset_karamy'
    reads = []
    writes = ['houset_karamy.policeCarRoutesCambridge']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('houset_karamy', 'houset_karamy')
        
        dataSets = {'policeCarRoutesCambridge':'https://data.cambridgema.gov/resource/svjm-6war.json'}  
        for ds in dataSets:
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
        doc.add_namspace('cma', 'https://data.cambridgema.gov/browse')
        
      #writes = ['houset_karamy.policeStations','houset_karamy.crimeReportsBoston', 'houset_karamy.crimeReportsCambridge', 'houset_karamy.policeCarRoutesCambridge', 'houset_karamy.policeWalkingRoutesCambridge','houset_karamy.realTimeTravelMassdot']

            
        this_script = doc.agent('alg:houset_karamy#getPoliceCarRoutesCambridge', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource3 = doc.entity('cma:svjm-6war', {'prov:label':'Police Car Routes Cambridge', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_crimeReportsCambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_policeCarRoutesCambridge, this_script)
        
       
        doc.usage(get_policeCarRoutesCambridge, resource3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
       
        policeCarRoutesCambridge = doc.entity('dat:houset_karamy#policeCarRoutesCambridge', {prov.model.PROV_LABEL:'Cambridge Crime Reports', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(policeCarRoutesCambridge, this_script)
        doc.wasGeneratedBy(policeCarRoutesCambridge, get_policeCarRoutesCambridge, endTime)
        doc.wasDerivedFrom(policeCarRoutesCambridge, resource3, get_policeCarRoutesCambridge, get_policeCarRoutesCambridge, get_policeCarRoutesCambridge)
        
        repo.logout()
                  
        return doc


get.execute()
doc = get.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
