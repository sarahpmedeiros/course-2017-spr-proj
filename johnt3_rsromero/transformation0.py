import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy


class transformation0(dml.Algorithm):
    contributor = 'johnt3_rsromero'
    reads = ['johnt3_rsromero.bpdcrime', 'johnt3_rsromero.newbpdfio']
    writes = ['johnt3_rsromero.newbpdcrimefio']

    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()

        ## Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('johnt3_rsromero', 'johnt3_rsromero')

        bpdcrimerepo = repo.johnt3_rsromero.bpdcrime
        newbpdfiorepo = repo.johnt3_rsromero.newbpdfio

        ##Run Projection on Arrays
        crime = []
        fio = []
        
        for file in bpdcrimerepo.find():
        	invertedcoordinates = file['location']['coordinates']
        	##print(invertedcoordinates)
        	coordinates = [float(invertedcoordinates[1]), float(invertedcoordinates[0])]
        	##print(coordinates)
        	crime.append({"district":file['reptdistrict'], 'date':file['fromdate'], 'description':file['incident_type_description'], 'coordinates':coordinates, 'month':file['month']})
    
        for file in newbpdfiorepo.find():
        	rawcoordinates = file['coordinates']
        	##print(rawcoordinates)
        	trimcoordinates = rawcoordinates[1:-1]
        	##print(trimcoordinates)
        	finalcoordinates = trimcoordinates.split(",")
        	##print(finalcoordinates)
        	coordinates = [float(finalcoordinates[0]), float(finalcoordinates[1])]
        	##print(coordinates)
        	fio.append({"district":file['DIST'], 'datetime':file['FIO_TIME'], 'description':file['FIOFS_REASONS'], 'coordinates':coordinates, 'month':file['month']})

        ##This is the Union Step

            
        repo.dropPermanent("newbpdcrimefio")
        repo.createPermanent("newbpdcrimefio")
        repo['johnt3_rsromero.newbpdcrimefio'].insert_many(crime)
        repo['johnt3_rsromero.newbpdcrimefio'].insert_many(fio)


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

        this_script = doc.agent('alg:johnt3_rsromero#transformation0', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        bpdcrime_resource = doc.entity('bdp:ufcx-3fdn', {'prov:label':'Boston Police Department Crime Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        bpdfio_resource = doc.entity('bdp:2pem-965w', {'prov:label':'Boston Police Department FIO Data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_bpdcrime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_bpdfio = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_bpdcrime, this_script)
        doc.wasAssociatedWith(get_bpdfio, this_script)
        
        doc.usage(get_bpdcrime, bpdcrime_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Transform+Datasets'
                  }
                  )
                  
        doc.usage(get_bpdfio, bpdfio_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Transform+Datasets'
                  }
                  )

        transformation0 = doc.entity('dat:johnt3_rsromero#transformation0', {prov.model.PROV_LABEL:'Combined BPD Crime + FIO Transformation0', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(transformation0, this_script)
        doc.wasGeneratedBy(transformation0, get_bpdcrime, endTime)
        doc.wasGeneratedBy(transformation0, get_bpdfio, endTime)
        doc.wasDerivedFrom(transformation0, bpdcrime_resource, get_bpdcrime, get_bpdcrime, get_bpdcrime)
        doc.wasDerivedFrom(transformation0, bpdfio_resource, get_bpdfio, get_bpdfio, get_bpdfio)

        repo.logout()
                  
        return doc
    

##transformation0.execute()
##doc = transformation0.provenance()
##print(doc.get_provn())
##print(json.dumps(json.loads(doc.serialize()), indent=4))

##eof
