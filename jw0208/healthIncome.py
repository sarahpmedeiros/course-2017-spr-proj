import urllib.request
import json
import dml
import prov.model
import datetime
import uuid



class healthIncome(dml.Algorithm):
    contributor = 'jw0208'
    reads = ['jw0208.income', 'jw0208.health', 'jw0208.poverty']
    writes = ['jw0208.healthIncome']



    @staticmethod



    def execute(trial=False):
        startTime = datetime.datetime.now()

        def project(R, p):
            return [p(t) for t in R]

        def select(R, s):
            return [t for t in R if s(t)]

        def product(R, S, X):
            return [(t, u, q) for t in R for u in S for q in X]

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jw0208', 'jw0208')


        poverty = repo['jw0208.poverty']
        income = repo['jw0208.income']
        health = repo['jw0208.health']

        health_array = []

        for document in health.find():
            health_array.append((document['locationabbr'], document['data_value']))

        #print (health_array);


        income_array = []

        for document in income.find():
            income_array.append((document['state'], document['HHincome']))


        poverty_array = []

        for document in poverty.find():
            poverty_array.append((document['Name'], document['Percent']))

        #print(poverty_array)

        x = project(select(product(health_array, income_array, poverty_array), lambda t: t[0][0] == t[1][0] == t[2][0]), lambda t: (t[0][0], t[0][1], t[1][1], t[2][1]))


        #x = project(select(product(income_array, health_array), lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1]))
        #print(x)

        y = []
        for i in range(0,50):
            y.append({'state':x[i][0], 'annual physically&mentally unhealthydays': x[i][1], 'annual HHincome': x[i][2], 'povertyrate':x[i][3]})

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
        doc.add_namespace('cdg', 'https://chronicdata.cdc.gov/resource/')

        this_script = doc.agent('alg:jw0208#healthIncome', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('cdg:fq5d-abxc', {'prov:label':'State physically and mentally unhealthy days vs. state income&poverty level', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        this_healthIncome = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(this_healthIncome, this_script)
        doc.usage(this_healthIncome, resource, startTime, None,
                {prov.model.PROV_TYPE:'ont:Retrieval'}
            )

        healthIncome = doc.entity('dat:jw0208#healthIncome', {prov.model.PROV_LABEL:'State physically and mentally unhealthy days vs. state education level', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(healthIncome, this_script)
        doc.wasGeneratedBy(healthIncome, this_healthIncome, endTime)
        doc.wasDerivedFrom(healthIncome, resource, this_healthIncome, this_healthIncome, this_healthIncome)

        #repo.record(doc.serialize()) # Record the provenance document.
        repo.logout()

        return doc
#
# healthIncome.execute()
# doc = healthIncome.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
