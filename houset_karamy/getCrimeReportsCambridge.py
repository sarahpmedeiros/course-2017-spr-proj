
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class get(dml.Algorithm):
    contributor = 'houset_karamy'
    reads = []
    writes = ['houset_karamy.crimeReportsCambridge']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('houset_karamy', 'houset_karamy')
        
        dataSets = {'crimeReportsCambridge': 'https://data.cambridgema.gov/resource/dypy-nwuz.json'}  
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
        doc.add_namspace('cma', 'https://data.cambridgema.gov/resource/')       
            
        this_script = doc.agent('alg:houset_karamy#getCrimeReportsCambridge', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource1 = doc.entity('cma:dypy-nwuz', {'prov:label':'Crime Reports Cambridge', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
                        
        get_CrimeReportsCambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        
        doc.wasAssociatedWith(get_CrimeReportsCambridge, this_script)

        
        doc.usage(get_CrimeReportsCambridge, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        

           
        crimeReportsCambridge = doc.entity('dat:houset_karamy#crimeReportsCambridge', {prov.model.PROV_LABEL:'Crime Reports Cambridge', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crimeReportsCambridge, this_script)
        doc.wasGeneratedBy(crimeReportsCambridge, get_CrimeReportsCambridge, endTime)
        doc.wasDerivedFrom(crimeReportsCambridge, resource1, get_CrimeReportsCambridge, get_CrimeReportsCambridge, get_CrimeReportsCambridge) 
        

        repo.logout()
                  
        return doc


get.execute()
doc = get.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
