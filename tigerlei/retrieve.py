import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class retrieve(dml.Algorithm):
    contributor = 'tigerlei'
    reads = []
    writes = ['tigerlei.crime1', 'tigerlei.crime2', 'tigerlei.police', 'tigerlei.university', 'tigerlei.homicides']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('tigerlei', 'tigerlei')

        # Load credentials
        with open('../auth.json') as auth_data:
            credentials = json.load(auth_data)

        token = "?$$app_token="+credentials['services']['cityofbostondataportal']['token']
        limit = 50000

        boston_data_portal_datasets = {
            'crime1': 'https://data.cityofboston.gov/resource/crime.json',
            'crime2': 'https://data.cityofboston.gov/resource/29yf-ye7n.json'
            }

        # Retrieve huge datasets with app token
        for dataset in boston_data_portal_datasets:
            repo.dropCollection(dataset)
            repo.createCollection(dataset)

            # Get total number of records
            url = boston_data_portal_datasets[dataset] + token + "&$select=count(*)"
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            count = int(r[0]["count"])
            print("Retreiving dataset:", dataset, ", total number of records:", count)
            pages = count//limit + 1

            for i in range(pages):
                print('page', i, 'of', pages)
                offset = limit*i
                paging = '&$limit=' + str(limit) + '&$offset=' + str(offset)
                url = boston_data_portal_datasets[dataset] + token + paging
                response = urllib.request.urlopen(url).read().decode("utf-8")
                r = json.loads(response)

                repo['tigerlei.'+ dataset].insert_many(r)

        # Retrieve small datasets without app token
        boston_open_data_datasets = {
            'police': 'http://bostonopendata-boston.opendata.arcgis.com/datasets/e5a0066d38ac4e2abbc7918197a4f6af_6.geojson',
            'university': 'http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.geojson'
            }

        for dataset in boston_open_data_datasets:
            repo.dropCollection(dataset)
            repo.createCollection(dataset)
            url = boston_open_data_datasets[dataset]
            response = urllib.request.urlopen(url).read().decode("utf-8")
            r = json.loads(response)
            
            # flatten Geojson file 
            for i in range(len(r['features'])):
                repo['tigerlei.' + dataset].insert(r['features'][i]['properties'])

        # Manually download csv files and upload json format file as data resources
        url = 'http://datamechanics.io/data/tigerlei/Boston%20Homicides%202012-2016.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("homicides")
        repo.createCollection("homicides")
        repo['tigerlei.homicides'].insert_many(r)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('tigerlei', 'tigerlei')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
        doc.add_namespace('rid', 'https://dataverse.harvard.edu/dataset.xhtml?persistentId=')

        this_script = doc.agent('alg:tigerlei#retrieve', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource_crime1 = doc.entity('bdp:crime', {'prov:label':'police, crime, incidents, safety', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_crime2 = doc.entity('bdp:29yf-ye7n', {'prov:label':'police, crime, incidents, safety', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        resource_police = doc.entity('bod:e5a0066d38ac4e2abbc7918197a4f6af_6', {'prov:label':'boston, police, departments', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_university = doc.entity('bod:cbf14bb032ef4bd38e20429f71acb61a_2', {'prov:label':'boston, colleges, universities', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
        resource_homicides = doc.entity('rid:doi:10.7910/DVN/1J0IBN', {'prov:label':'homicide, crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'csv'})
        this_run = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(this_run, this_script)

        doc.usage(this_run, resource_crime1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})
        doc.usage(this_run, resource_crime2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})
        doc.usage(this_run, resource_police, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})
        doc.usage(this_run, resource_university, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})
        doc.usage(this_run, resource_homicides, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',})

        crime1 = doc.entity('dat:tigerlei#crime1', {prov.model.PROV_LABEL:'crime incidents 2012-2015', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime1, this_script)
        doc.wasGeneratedBy(crime1, this_run, endTime)
        doc.wasDerivedFrom(crime1, resource_crime1, this_run, this_run, this_run)

        crime2 = doc.entity('dat:tigerlei#crime2', {prov.model.PROV_LABEL:'crime incidents 2015-2016', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(crime2, this_script)
        doc.wasGeneratedBy(crime2, this_run, endTime)
        doc.wasDerivedFrom(crime2, resource_crime2, this_run)

        police = doc.entity('dat:tigerlei#police', {prov.model.PROV_LABEL:'police station in boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(police, this_script)
        doc.wasGeneratedBy(police, this_run, endTime)
        doc.wasDerivedFrom(police, resource_police, this_run)

        university = doc.entity('dat:tigerlei#university', {prov.model.PROV_LABEL:'colleges and universities in boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(university, this_script)
        doc.wasGeneratedBy(university, this_run, endTime)
        doc.wasDerivedFrom(university, resource_university, this_run)

        homicides = doc.entity('dat:tigerlei#homicides', {prov.model.PROV_LABEL:'homicides 2012-2016', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(homicides, this_script)
        doc.wasGeneratedBy(homicides, this_run, endTime)
        doc.wasDerivedFrom(homicides, resource_homicides, this_run)

        repo.logout()
                  
        return doc

# retrieve.execute()
# doc = retrieve.provenance()
# print(doc.get_provn())

# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
