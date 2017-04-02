import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from bson.code import Code

class transformation_0(dml.Algorithm):
    contributor = 'babgi_hvdlaan'
    reads = ['babgi_hvdlaan.cambridge_parking_spaces', 'babgi_hvdlaan.cambridge_parking_tickets','babgi_hvdlaan.cambridge_metered_parking']
    writes = ['babgi_hvdlaan.cambridge_parking_data']

    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()
        
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('babgi_hvdlaan', 'babgi_hvdlaan')


        cps = repo.babgi_hvdlaan.cambridge_parking_spaces
        cpt = repo.babgi_hvdlaan.cambridge_parking_tickets
        cmp = repo.babgi_hvdlaan.cambridge_metered_parking


############# Cambridge Parking Spaces
        spaces=cps.find()
    
        select_spaces=[]
        for s in spaces:
            if (s['structure']=="Garage" and s['owntype']=="Private"): #
               select_spaces.append(s)


############# Cambridge Parking Tickets
        tickets = cpt.find()

        select_tickets = []
        for t in tickets:
            if 'location' in t and (t['violation_description'] == "RESIDENT PERMIT ONLY" or t['violation_description'] == " NO PARKING" or t['violation_description'] == " NO STOPPING"):
                select_tickets.append(t)


############# Cambridge Metered Parking
        
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
        
        
        this_script = doc.agent('alg:babgi_hvdlaan#transformation0', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        this_script = doc.agent('alg:babgi_hvdlaan#transformation0', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        cambridge_parking_data = doc.entity('cdp:babgi_hvdlaan#cambridge_parking_tickets', {prov.model.PROV_LABEL:'Cambridge Parking Data', prov.model.PROV_TYPE:'ont:DataSet'})
        get_tickets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_tickets, this_script)
        doc.used(get_tickets, cambridge_parking_data, startTime)
        doc.wasAttributedTo(cambridge_parking_data, this_script)
        doc.wasGeneratedBy(cambridge_parking_data, get_tickets, endTime)
        repo.logout()
        
        return doc

transformation_0.execute()
doc = transformation_0.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
