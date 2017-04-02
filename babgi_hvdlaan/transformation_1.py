import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code

class transformation1(dml.Algorithm):
    contributor = 'babgi_hvdlaan'
    reads = ['babgi_hvdlaan.boston_parking_tickets', 'babgi_hvdlaan.boston_parking_spaces']
    writes = ['babgi_hvdlaan.boston_parking_data']


    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('babgi_hvdlaan', 'babgi_hvdlaan')


        bps = repo.babgi_hvdlaan.boston_parking_spaces
        bpt = repo.babgi_hvdlaan.boston_parking_tickets


############# Boston Parking Spaces
        spaces=bps.find()
    
        select_spaces=[]
        for s in spaces:
            if (s['STRUCTURE']=="Garage"):
                select_spaces.append(s)


############# Boston Parking Tickets
        tickets = bpt.find()
    
        select_tickets = []
        for t in tickets:
            if (t['violation1'] == "HYDRANT" or t['violation1'] == " NO PARKING" or t['violation1'] == " NO PARKING-ZONE B" or t['violation1'] == " NO STOP OR STAND"):
               select_tickets.append(t)

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
        
        
        this_script = doc.agent('alg:babgi_hvdlaan#transformation1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        this_script = doc.agent('alg:babgi_hvdlaan#transformation1', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        
        boston_parking_data = doc.entity('dat:babgi_hvdlaan#boston_parking_data', {prov.model.PROV_LABEL:'Boston Parking Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_tickets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_tickets, this_script)
        doc.used(get_tickets, boston_parking_data, startTime)
        doc.wasAttributedTo(boston_parking_data, this_script)
        doc.wasGeneratedBy(boston_parking_data, get_tickets, endTime)
        repo.logout()
        
        return doc

transformation1.execute()
doc = transformation1.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
