import urllib.request
import json
import dml
import prov.model
import datetime
import uuid



class healthIncome(dml.Algorithm):
    contributor = 'jw0208'
    reads = ['jw0208.income', 'jw0208.health']
    writes = ['jw0208.healthIncome']



    @staticmethod



    def execute(trial=False):
        startTime = datetime.datetime.now()

        def project(R, p):
            return [p(t) for t in R]

        def select(R, s):
            return [t for t in R if s(t)]

        def product(R, S):
            return [(t, u) for t in R for u in S]

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jw0208', 'jw0208')



        income = repo['jw0208.income']
        health = repo['jw0208.health']

        health_array = []

        for document in health.find():
            health_array.append((document['locationabbr'], document['data_value']))

        #print (health_array);


        income_array = []

        for document in income.find():
            income_array.append((document['state'], document['HHincome']))


        x = project(select(product(income_array, health_array), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1]))
        #print(x)

        y = []
        for i in range(0,51):
            y.append({'state':x[i][0], 'income': x[i][1], 'unhealthydays':x[i][2]})

        #print (y)



        repo.dropPermanent('jw0208.healthIncome')
        repo.createPermanent('jw0208.healthIncome')
        repo['jw0208.healthIncome'].insert_many(y)



        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}



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
        repo.authenticate('jw0208', 'jw0208')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jw0208#healthEducation', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:t85d-b449', {'prov:label':'State physically and mentally unhealthy days vs. state education level', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_healthIncome = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_healthIncome, this_script)
        doc.usage(this_healthIncome, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        healthEducation = doc.entity('dat:jw0208#healthEducation', {prov.model.PROV_LABEL:'State physically and mentally unhealthy days vs. state education level', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(healthEducation, this_script)
        doc.wasGeneratedBy(healthEducation, this_healthIncome, endTime)
        doc.wasDerivedFrom(healthEducation, resource, this_healthIncome, this_healthIncome, this_healthIncome)

        repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc

healthIncome.execute()
#doc = healthEducation.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
