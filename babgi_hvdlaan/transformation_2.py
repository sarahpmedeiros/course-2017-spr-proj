import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code

class transformation2(dml.Algorithm):
    contributor = 'babgi_hvdlaan'
    reads = ['babgi_hvdlaan.boston_waze_data']
    writes = ['babgi_hvdlaan.waze']


    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('babgi_hvdlaan', 'babgi_hvdlaan')


#w = repo.babgi_hvdlaan.boston_waze_data

############# Waze Data
#Find number of jams per street and put in jamMR
        map_function = Code('''function() {
            emit(this.street, {jams:1});
            }''')
    
        reduce_function = Code('''function(k, vs) {
            var total = 0;
            for (var i = 0; i < vs.length; i++)
            total += vs[i].jams;
            return {jams:total};
            }''')

        repo.logout()
        
        endTime = datetime.datetime.now()
        
        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

    # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('babgi_hvdlaan', 'babgi_hvdlaan')
    
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/')
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/resource/')
        
        
        this_script = doc.agent('alg:babgi_hvdlaan#transformation2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        waze = doc.entity('bdp:babgi_hvdlaan#waze', {prov.model.PROV_LABEL:'Parking & Traffic Jams', prov.model.PROV_TYPE:'ont:DataSet'})
        getwaze = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime, {'prov:label':'Parking & Traffic Jams with MR'})
        doc.wasAssociatedWith(getwaze, this_script)
        doc.used(getwaze, waze, startTime)
        doc.wasAttributedTo(waze, this_script)
        doc.wasGeneratedBy(waze, getwaze, endTime)
        repo.logout()
        
        return doc

transformation2.execute()
doc = transformation2.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

