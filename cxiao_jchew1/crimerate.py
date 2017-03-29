import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class example(dml.Algorithm):
    contributor = 'cxiao_jchew1'
    reads = []
    writes = ['cxiao_jchew1.crime_reports']

    @staticmethod
    def execute(trial = False):
        'Acquire data'
        startTime = datetime.datetime.now()

        # Set up connection for database.
        client = dml.pymongo.MongoClient() #starts the client
        repo = client.repo
        repo.authenticate('cxiao_jchew1', 'cxiao_jchew1') #authenticates data directory

        url = 'https://data.cityofboston.gov/resource/29yf-ye7n.json' # API key for data
        response = urllib.request.urlopen(url).read().decode("utf-8")

        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)

        repo.dropCollection("crime_reports")

        repo.createCollection("crime_reports")

        repo['cxiao_jchew1.crime_reports'].insert_many(r)

        repo['cxiao_jchew1.crime_reports'].metadata({'complete':True})

        print(repo['cxiao_jchew1.crime_reports'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        'Creating provenance'

        # Set up database connection.
        client = dml.pymongo.MongoClient() # some thingys that format the data

        repo = client.repo

        repo.authenticate('cxiao_jchew1', 'cxiao_jchew1')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.

        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.

        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.

        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:cxiao_jchew1#crimerate', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'}) # name of script that needs to be changed depending on data set

        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime) # change variable based on topic pertaining to data (in this case: crime)

        doc.wasAssociatedWith(get_crime, this_script) #get_crime

        doc.usage(get_crime, resource, startTime, None,

                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'$select=INCIDENT,OFFENSE CODE,OFFENSE CODE GROUP,OFFENSE DESCRIPTION,DISTRICT,REPORTING AREA,SHOOTING,OCCURRED ON DATE,Hour,YEAR,MONTH,DAY OF WEEK,UCR PART,STREET,LAT,LONG,Location'
                  } # ^the column names for the tables (be specific and accurate!)
                  )


        crime = doc.entity('dat:cxiao_jchew1#crimerate', {prov.model.PROV_LABEL:'Crime Rate', prov.model.PROV_TYPE:'ont:DataSet'})

        doc.wasAttributedTo(crime, this_script)

        doc.wasGeneratedBy(crime, get_crime, endTime)

        doc.wasDerivedFrom(crime, resource, get_crime, get_crime, get_crime)

        repo.logout()
                  
        return doc
#################
example.execute()
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
