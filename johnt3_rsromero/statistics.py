import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

class statistics(dml.Algorithm):
    contributor = 'johnt3_rsromero'
    reads = ['johnt3_rsromero.newbpdcrimefio']
    writes = ['johnt3_rsromero.statistics']

    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()

        ## Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('johnt3_rsromero', 'johnt3_rsromero')

        newbpdcrimefiorepo = repo.johnt3_rsromero.newbpdcrimefio


        ##Creates Array for each District
        Final_Array = []
        District_B3=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_C6=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_A7=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_D14=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_A1=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_E13=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_D4=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_E18=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_B2=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_E5=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_C11=[0,0,0,0,0,0,0,0,0,0,0,0]
        
        for file in newbpdcrimefiorepo.find():
            current_district = file['district']
            current_month = int(file['month']) - 1
            
            if current_district == "B3":
                District_B3[current_month] += 1
                
            if current_district == "C6":
                District_C6[current_month] += 1
                
            if current_district== "A7":
                District_A7[current_month] += 1
                
            if current_district== "D14":
                District_D14[current_month] += 1
                
            if current_district== "A1":
                District_A1[current_month] += 1
                
            if current_district == "E13":
                District_E13[current_month] += 1
                
            if current_district == "D4":
                District_D4[current_month] += 1
                
            if current_district== "E18":
                District_E18[current_month] += 1
                
            if current_district == "B2":
                District_B2[current_month] += 1

            if current_district == "E5":
                District_E5[current_month] += 1
                
            if current_district== "C11":
                District_C11[current_month] += 1

        Final_Array.append({"District_B3":District_B3, "District_C6":District_C6, "District_A7":District_A7,"District_D14":District_D14,"District_A1":District_A1, "District_E13":District_E13,"District_D4":District_D4, "District_E18":District_E18, "District_B2":District_B2, "District_E5":District_E5, "District_C11":District_C11}) 
        ##print(Final_Array)                   

        ##This is the Union Step

            
        repo.dropPermanent("statistics")
        repo.createPermanent("statistics")
        repo['johnt3_rsromero.statistics'].insert_many(Final_Array)


        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
        
    @staticmethod
    def execute(trial = True):
        
        startTime = datetime.datetime.now()

        ## Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('johnt3_rsromero', 'johnt3_rsromero')

        newbpdcrimefiorepo = repo.johnt3_rsromero.newbpdcrimefio


        ##Creates Array for each District
        Final_Array = []
        District_B3=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_C6=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_A7=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_D14=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_A1=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_E13=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_D4=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_E18=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_B2=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_E5=[0,0,0,0,0,0,0,0,0,0,0,0]
        District_C11=[0,0,0,0,0,0,0,0,0,0,0,0]
        
        for file in newbpdcrimefiorepo.find()[:100]:
            current_district = file['district']
            current_month = int(file['month']) - 1
            
            if current_district == "B3":
                District_B3[current_month] += 1
                
            if current_district == "C6":
                District_C6[current_month] += 1
                
            if current_district== "A7":
                District_A7[current_month] += 1
                
            if current_district== "D14":
                District_D14[current_month] += 1
                
            if current_district== "A1":
                District_A1[current_month] += 1
                
            if current_district == "E13":
                District_E13[current_month] += 1
                
            if current_district == "D4":
                District_D4[current_month] += 1
                
            if current_district== "E18":
                District_E18[current_month] += 1
                
            if current_district == "B2":
                District_B2[current_month] += 1

            if current_district == "E5":
                District_E5[current_month] += 1
                
            if current_district== "C11":
                District_C11[current_month] += 1

        Final_Array.append({"District_B3":District_B3, "District_C6":District_C6, "District_A7":District_A7,"District_D14":District_D14,"District_A1":District_A1, "District_E13":District_E13,"District_D4":District_D4, "District_E18":District_E18, "District_B2":District_B2, "District_E5":District_E5, "District_C11":District_C11}) 
        ##print(Final_Array)                   

        ##This is the Union Step

            
        repo.dropPermanent("statistics")
        repo.createPermanent("statistics")
        repo['johnt3_rsromero.statistics'].insert_many(Final_Array)


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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/') #city of boston data portal

        this_script = doc.agent('alg:johnt3_rsromero#statistics', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        newbpdcrimefio_resource = doc.entity('bdp:2pem-965w+ufcx-3fdn', {'prov:label':'Combined BPD FIO + Crime Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_bpdcrimefio = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_bpdcrimefio, this_script)
        
        doc.usage(get_bpdcrimefio, newbpdcrimefio_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Run+Statistics'
                  }
                  )

        statistics = doc.entity('dat:johnt3_rsromero#statistics', {prov.model.PROV_LABEL:'Crime + FIO Statistics', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(statistics, this_script)
        doc.wasGeneratedBy(statistics, get_bpdcrimefio, endTime)
        
        doc.wasDerivedFrom(statistics, newbpdcrimefio_resource, get_bpdcrimefio, get_bpdcrimefio, get_bpdcrimefio)


        repo.logout()
                  
        return doc
    

##statistics.execute()
##doc = statistics.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof
