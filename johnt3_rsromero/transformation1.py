import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

class transformation1(dml.Algorithm):
    contributor = 'johnt3_rsromero'
    reads = ['johnt3_rsromero.cambridgecrime', 'johnt3_rsromero.cambridgecitations']
    writes = ['johnt3_rsromero.camcrimecit']

    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()

        ## Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('johnt3_rsromero', 'johnt3_rsromero')

        camcrimerepo = repo.johnt3_rsromero.cambridgecrime
        camcitrepo = repo.johnt3_rsromero.cambridgecitations

        ##Run Projection on Arrays
        crime = []
        cit = []
        

        for file in camcrimerepo.find():
            if "location" in file:
                crime.append({"location":file['location'], 'date':file['crime_date_time'], 'description':file['crime']})
    
        for file in camcitrepo.find():
            if "charge_description" in file:
                cit.append({'location':file['street_name'], 'date':file['date_time_issued'], 'description':file['charge_description']})

        ##This is the Union Step

            
        repo.dropPermanent("camcrimecit")
        repo.createPermanent("camcrimecit")
        repo['johnt3_rsromero.camcrimecit'].insert_many(crime)
        repo['johnt3_rsromero.camcrimecit'].insert_many(cit)


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

        this_script = doc.agent('alg:johnt3_rsromero#transformation1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
 ##       resource = doc.entity('bdp:ufcx-3fdn', {'prov:label':'CPD Merged Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_cpdcrime_cit = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_cpdcrime_cit, this_script)
        doc.usage(get_cpdcrime_cit, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  }
                  )

        cpdcrimecit = doc.entity('dat:johnt3_rsromero#Transformation1', {prov.model.PROV_LABEL:'Transformation 1', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(cpdcrimecit, this_script)
        doc.wasGeneratedBy(cpdcrimecit, get_cpdcrime_cit, endTime)
        doc.wasDerivedFrom(cpdcrimecit, get_cpdcrime_cit, get_cpdcrime_cit, get_cpdcrime_cit)

        repo.logout()
                  
        return doc
    

##transformation1.execute()
##doc = bpdcrimefio.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof
