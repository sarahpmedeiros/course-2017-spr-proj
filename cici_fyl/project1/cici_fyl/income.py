import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import methods

class income(dml.Algorithm):
    contributor = 'cici_fyl'
    reads = []
    writes = ['income','property','avg_income','income_property']
    
    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()
        j = json.loads(open("income.json").read())

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cici_fyl', 'cici_fyl')

        #Read income data
        repo.dropCollection("income")
        repo.createCollection("income")
        repo['cici_fyl.income'].insert_many(j)
        repo['cici_fyl.income'].metadata({'complete':True})

        #Read property data
        url = 'https://data.cityofboston.gov/resource/n7za-nsjh.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("property")
        repo.createCollection("property")
        repo['cici_fyl.property'].insert_many(r)
        repo['cici_fyl.property'].metadata({'complete':True})

        #Read property data
        url = 'https://data.cityofboston.gov/resource/g5b5-xrwi.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        a = json.loads(response)
        repo.dropCollection("property_2016")
        repo.createCollection("property_2016")
        repo['cici_fyl.property_2016'].insert_many(a)
        repo['cici_fyl.property_2016'].metadata({'complete':True})

        temp= methods.merge1(r,a)
        temp1=[]

        for i in temp:
            for l in i:
                temp1.append(l)

        repo.dropCollection("property")
        repo.createCollection("property")
        repo['cici_fyl.property'].insert_many(temp1)
        repo['cici_fyl.property'].metadata({'complete':True})
        repo.dropCollection("property_2016")

        #Write avg_income and property_income
        incomedata = repo['cici_fyl.income'].find()
        propertydata = repo['cici_fyl.property'].find()

        x=methods.select(incomedata,"MA")
        
        incomedata= methods.averageincome(x)

        repo.dropCollection("avg_income")
        repo.createCollection("avg_income")
        repo['cici_fyl.avg_income'].insert_many(incomedata)


        m= methods.merge(propertydata,incomedata)

        repo.dropCollection("property_income")
        repo.createCollection("property_income")
        repo['cici_fyl.property_income'].insert_many(m)

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

        this_script = doc.agent('alg:cici_fyl#income', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        dataset1 = doc.entity('bdp:n7za-nsjh', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'json'})
        dataset2 = doc.entity('irs:14zpallagi', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'csv'})

        income_data = doc.entity('dat:cici_fyl#income', {'prov:label':'income IRS report', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        property_data = doc.entity('dat:cici_fyl#property', {prov.model.PROV_LABEL:'Boston property', prov.model.PROV_TYPE:'ont:DataSet'})
        get_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_property = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_income, this_script)
        doc.wasAssociatedWith(get_property, this_script)
        #Query might need to be changed
        doc.usage(get_income,dataset2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_property, dataset1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        avg_income_data = doc.entity('dat:cici_fyl#avg_income', {prov.model.PROV_LABEL:'average income of MA cities', prov.model.PROV_TYPE:'ont:DataSet'})
        get_avg_income = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAttributedTo(avg_income_data, this_script)
        doc.wasGeneratedBy(avg_income_data, get_avg_income, endTime)
        doc.wasDerivedFrom(avg_income_data, income_data, get_avg_income, get_avg_income, get_avg_income)


        income_property_data = doc.entity('dat:cici_fyl#income_property', {prov.model.PROV_LABEL:'property and average income', prov.model.PROV_TYPE:'ont:DataSet'})
        get_income_property = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAttributedTo(income_property_data, this_script)
        doc.wasGeneratedBy(income_property_data, get_income_property, endTime)
        doc.wasDerivedFrom(income_property_data, avg_income_data, get_property, get_avg_income)
        

        repo.logout()
                  
        return doc

doc = income.provenance()

