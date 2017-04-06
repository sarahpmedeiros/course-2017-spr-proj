import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

class transformation2(dml.Algorithm):
    contributor = 'johnt3_rsromero'
    reads = ['johnt3_rsromero.newbpdcrimefio', 'johnt3_rsromero.bpdlocations']
    writes = ['johnt3_rsromero.bpdcrimefiolocation']

    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()

        ## Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('johnt3_rsromero', 'johnt3_rsromero')

        bpdcrimefiorepo = repo.johnt3_rsromero.newbpdcrimefio
        bpdlocations = repo.johnt3_rsromero.bpdlocations

        ##Run Projection on Arrays
        crimefiolocation = []
        locations = []
        
        for file in bpdlocations.find():
            temp = file['name']
            temp2 = ''
            if temp != 'Boston Police Headquarters':
                temp2 = temp[9] + temp[11]
                locations.append({"district":temp2, 'streetaddress':file['location_location']})
        
        for file in bpdcrimefiorepo.find():
            temp_police_station = ''
            matcher = file['district']
            for i in range(len(locations)):
                if locations[i]["district"] == matcher:
                    temp_police_station = locations[i]["streetaddress"]
                    crimefiolocation.append({"district":file['district'], 'date':file['date'], 'location':file['location'], 'description':file['description'], 'police_station':temp_police_station})


        ##This is the Union Step

            
        repo.dropPermanent("bpdcrimefiolocation")
        repo.createPermanent("bpdcrimefiolocation")
        repo['johnt3_rsromero.bpdcrimefiolocation'].insert_many(crimefiolocation)
        ##repo['johnt3_rsromero.bpdcrimefiolocations'].insert_many(fio)


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
        repo.authenticate('johnt3_rsromero', 'johnt3_rsromero')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        this_script = doc.agent('alg:johnt3_rsromero#transformation2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
 ##       resource = doc.entity('bdp:ufcx-3fdn', {'prov:label':'BPD Merged Data with District', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_bpdcrimefiodistrict = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_bpdcrimefiodistrict, this_script)
        doc.usage(get_bpdcrimefiodistrict, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        bpdcrimefiolocations = doc.entity('dat:johnt3_rsromero#transformation2', {prov.model.PROV_LABEL:'Tranformation2', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(bpdcrimefiolocations, this_script)
        doc.wasGeneratedBy(bpdcrimefiolocations, get_bpdcrimefiodistrict, endTime)
        doc.wasDerivedFrom(bpdcrimefiolocations, get_bpdcrimefiodistrict, get_bpdcrimefiodistrict, get_bpdcrimefiodistrict)

        repo.logout()
                  
        return doc
    

##transformation2.execute()
##doc = bpdcrimefio.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof
