import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import methods

class processschool(dml.Algorithm):
    contributor = 'cici_fyl'
    reads = ['school', 'property']
    writes = ['property_school']
    


    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cici_fyl', 'cici_fyl')

        propertydata = repo['cici_fyl.property'].find()
        schooldata= repo['cici_fyl.school'].find()

        x= methods.appendattribute(propertydata,"edu_score")
        y= methods.schoolinfo(schooldata)
        x= methods.edu_score(x,y,0.05)

        repo.dropCollection("property_school")
        repo.createCollection("property_school")
        repo['cici_fyl.property_school'].insert_many(x)


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
        repo.authenticate('cici_fyl', 'cici_fyl')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('irs', 'https://www.irs.gov/pub/irs-soi/')

        this_script = doc.agent('alg:cici_fyl#processschool', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        school_dataset = doc.entity('dat:cici_fyl/school', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'geojson'})
        property_dataset = doc.entity('dat:cici_fyl/property', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'json'})

    
        get_property_school = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_property_school, this_script)

        #Query might need to be changed
        doc.usage(get_property_school,school_dataset, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                 
                  }
                  )
        doc.usage(get_property_school, property_dataset, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  
                  }
                  )

        pro_school_w = doc.entity('dat:cici_fyl/property_school', {prov.model.PROV_LABEL:'average income of MA cities', prov.model.PROV_TYPE:'ont:DataSet'})
        
        doc.wasAttributedTo(pro_school_w, this_script)
        doc.wasGeneratedBy(pro_school_w, school_dataset, endTime)
        doc.wasDerivedFrom(pro_school_w, school_dataset, get_property_school, get_property_school, get_property_school)
        doc.wasDerivedFrom(pro_school_w, property_dataset, get_property_school, get_property_school, get_property_school)

        repo.logout()
                  
        return doc



















