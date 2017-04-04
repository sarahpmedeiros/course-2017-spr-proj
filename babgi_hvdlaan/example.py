import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
#import geopy.distance import vincenty

class example(dml.Algorithm):
    contributor = 'babgi_hvdlaan'
    reads = []
    writes = ['babgi_hvdlaan.cambridge_parking_spaces', 'babgi_hvdlaan.cambridge_parking_tickets','babgi_hvdlaan.cambridge_metered_parking','babgi_hvdlaan.boston_parking_tickets','babgi_hvdlaan.boston_parking_spaces','babgi_hvdlaan.waze_data']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('babgi_hvdlaan', 'babgi_hvdlaan')
        
# DATA TRANSFORMATION


    #Cambridge Parking Spaces

        url = 'https://data.cambridgema.gov/resource/vr3p-e9ke.json' #municipal
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("cambridge_parking_spaces")
        repo.createCollection("cambridge_parking_spaces")
        repo['babgi_hvdlaan.cambridge_parking_spaces'].insert_many(r)
        
        #repo['babgi_hvdlaan.cambridge_parking_spaces'].metadata({'complete':True})
        #print(repo['babgi_hvdlaan.cambridge_parking_spaces'].metadata())

    #Cambridge Parking Tickets
    
        url = 'https://data.cambridgema.gov/resource/m4i2-83v6.json' #done except commercial
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("cambridge_parking_tickets")
        repo.createCollection("cambridge_parking_tickets")
        repo['babgi_hvdlaan.cambridge_parking_tickets'].insert_many(r)

    #Cambridge Metered Parking

        url = 'https://data.cambridgema.gov/resource/up94-ihbw.json' #done
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("cambridge_metered_parking")
        repo.createCollection("cambridge_metered_parking")
        repo['babgi_hvdlaan.cambridge_metered_parking'].insert_many(r)
        #repo['babgi_hvdlaan.cambridge_metered_parking'].metadata({'complete':True})
        #print(repo['babgi_hvdlaan.cambridge_metered_parking'].metadata())
    
    #Boston Parking Tickets

        url = 'https://data.cityofboston.gov/resource/cpdb-ie6e.json' # done!
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("boston_parking_tickets")
        repo.createCollection("boston_parking_tickets")
        repo['babgi_hvdlaan.boston_parking_tickets'].insert_many(r)

    #Boston Parking Spaces
    
        url = 'http://datamechanics.io/data/boston_commerical_parking.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("boston_parking_spaces")
        repo.createCollection("boston_parking_spaces")
        repo['babgi_hvdlaan.boston_parking_spaces'].insert_many(r)
        
    #Boston & Cambridge Waze Data

        url ='https://data.cityofboston.gov/resource/dih6-az4h.json' #done
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("waze_data")
        repo.createCollection("waze_data")
        repo['babgi_hvdlaan.waze_data'].insert_many(r)
        
        
        repo.logout()
        endTime = datetime.datetime.now()
        return {"start":startTime, "end":endTime}
        
    
# PROVENANCE
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('babgi_hvdlaan', 'babgi_hvdlaan')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/') # boston data portal
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/resource/') #cambridge data portal


        this_script = doc.agent('alg:babgi_hvdlaan#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        #entity
        Cam_park_space_resource = doc.entity('cdp:vr3p-e9ke', {'prov:label':'Cambridge parking space', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        Cam_metered_parking_resource = doc.entity('cdp:p94-ihbw', {'prov:label':'Cambridge metered parking space', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        Cam_parking_tickets_resource = doc.entity('cdp:m4i2-83v6', {'prov:label':'Cambridge parking tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        Bos_park_tickets_resource = doc.entity('bdp:cpdb-ie6e', {'prov:label':'Boston Parking tickets', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        Bos_parking_spaces_resource = doc.entity('dat:boston_commercial_parking', {'prov:label':'Boston parking spaces', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        Wazedata_resource = doc.entity('bdp:dih6-az4h', {'prov:label':'Boston & Cambridge waze data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        #activity
        get_cps = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_cmp = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_cpt = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_bpt = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_bps = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_w = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        #associated
        doc.wasAssociatedWith(get_cps, this_script)
        doc.wasAssociatedWith(get_cmp, this_script)
        doc.wasAssociatedWith(get_cpt, this_script)
        doc.wasAssociatedWith(get_bpt, this_script)
        doc.wasAssociatedWith(get_bps, this_script)
        doc.wasAssociatedWith(get_w, this_script)

        #used
        doc.usage(get_cps, Cam_park_space_resource,startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_cmp, Cam_metered_parking_resource,startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_cpt, Cam_parking_tickets_resource,startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_bpt, Bos_park_tickets_resource,startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_bps, Bos_parking_spaces_resource,startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})
        doc.usage(get_w, Wazedata_resource,startTime, None, {prov.model.PROV_TYPE:'ont:Retrieval'})

        #attributed, generated, derived
        cps = doc.entity('dat:babgi_hvdlaan#cps', {prov.model.PROV_LABEL:'Cambridge parking space', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(cps, this_script)
        doc.wasGeneratedBy(cps, get_cps, endTime)
        doc.wasDerivedFrom(cps, Cam_park_space_resource, get_cps,get_cps, get_cps)

        cmp = doc.entity('dat:babgi_hvdlaan#cmp', {prov.model.PROV_LABEL:'Cambridge metered parking space', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(cps, this_script)
        doc.wasGeneratedBy(cps, get_cmp, endTime)
        doc.wasDerivedFrom(cps, Cam_metered_parking_resource, get_cmp,get_cmp, get_cmp)

        cpt = doc.entity('dat:babgi_hvdlaan#cpt', {prov.model.PROV_LABEL:'Cambridge parking tickets', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(cpt, this_script)
        doc.wasGeneratedBy(cpt, get_cpt, endTime)
        doc.wasDerivedFrom(cpt, Cam_parking_tickets_resource, get_cpt,get_cpt, get_cpt)

        bpt = doc.entity('dat:babgi_hvdlaan#bpt', {prov.model.PROV_LABEL:'Boston parking tickets', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bpt, this_script)
        doc.wasGeneratedBy(bpt, get_bpt, endTime)
        doc.wasDerivedFrom(bpt, Bos_park_tickets_resource, get_bpt,get_bpt, get_bpt)

        bps = doc.entity('dat:babgi_hvdlaan#bps', {prov.model.PROV_LABEL:'Boston parking space', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bps, this_script)
        doc.wasGeneratedBy(bps, get_bps, endTime)
        doc.wasDerivedFrom(bps, Bos_parking_spaces_resource, get_bps,get_bps, get_bps)

        w = doc.entity('dat:babgi_hvdlaan#w', {prov.model.PROV_LABEL:'Boston & Cambridge waze data', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(w, this_script)
        doc.wasGeneratedBy(w, get_w, endTime)
        doc.wasDerivedFrom(w, Wazedata_resource, get_w, get_w, get_w)
        repo.logout()
        return doc

example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


##eof        
        

