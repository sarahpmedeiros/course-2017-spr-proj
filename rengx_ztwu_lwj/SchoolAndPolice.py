import urllib
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
from bson.code import Code
from bson.json_util import dumps



class SchoolAndPolice(dml.Algorithm):

    contributor = 'rengx_ztwu_lwj'
    reads = ['rengx_ztwu_lwj.publicschool','rengx_ztwu_lwj.policestation']
    writes = ['rengx_ztwu_lwj.SchoolAndPolice']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rengx_ztwu_lwj', 'rengx_ztwu_lwj')

        # Get the collections
        publicschool = repo['rengx_ztwu_lwj.publicschool']
        policestation = repo['rengx_ztwu_lwj.policestation']

        # Get names and zipcode of all schools and put them into (key, value) form
        SchoolAndPolice = []
        for entry in policestation.find({"Location": {"$exists": True}}):
            SchoolAndPolice.append(
                {"name": entry['NAME'], "value": {'street': entry['Location']}
                 })

        for entry in crimereports.find({"STREETNAME": {"$exists": True}}):
            SchoolAndPolice.append(
                {"name": entry['SCH_NAME'], "value": {'street': entry['Location']}
                })


    # Create a new collection and insert the result data set
        repo.dropCollection('SchoolAndPoliceDB')
        repo.createCollection('SchoolAndPoliceDB')
        repo['rengx_ztwu_lwj.SchoolAndPoliceDB'].insert_many(SchoolAndPolice)

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rengx_ztwu_lwj', 'rengx_ztwu_lwj')

        doc.add_namespace('alg',
                          'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat',
                          'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/')
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        # Agent
        this_script = doc.agent('alg:rengx_ztwu_lwj#SchoolAndPolice',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resources
        resource_publicschool = doc.entity('dat:rengx_ztwu_lwj#publicschool',
                                                {'prov:label': 'public school in Boston',
                                                 prov.model.PROV_TYPE: 'ont:DataResource',
                                                 'ont:Extension': 'json'})

        resource_policestation = doc.entity('dat:rengx_ztwu_lwj#policestation',
                                              {'prov:label': 'police station in Boston',
                                               prov.model.PROV_TYPE: 'ont:DataResource',
                                               'ont:Extension': 'json'})

        # Activities
        combine_SchoolAndPolice = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                {
                                                    prov.model.PROV_LABEL: "Combine all School And Police in Boston",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})

        # Activities' Associations with Agent
        doc.wasAssociatedWith(combine_SchoolAndPolice, this_script)

        # Record which activity used which resource
        doc.usage(combine_SchoolAndPolice, resource_publicschool, startTime)
        doc.usage(combine_SchoolAndPolice, resource_policestation, startTime)

        # Result dataset entity
        SchoolAndPolice = doc.entity('dat:rengx_ztwu_lwj#SchoolAndPolice',
                                      {prov.model.PROV_LABEL: 'All School And Police in Boston',
                                       prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(SchoolAndPolice, this_script)
        doc.wasGeneratedBy(SchoolAndPolice, combine_SchoolAndPolice, endTime)
        doc.wasDerivedFrom(SchoolAndPolice, resource_publicschool, combine_SchoolAndPolice,
                           combine_SchoolAndPolice,
                           combine_SchoolAndPolice)
        doc.wasDerivedFrom(SchoolAndPolice, resource_policestation, combine_SchoolAndPolice,
                           combine_SchoolAndPolice,
                           combine_SchoolAndPolice)

        repo.logout()

        return doc

combineSchoolAndPolice.execute()
doc = combineSchoolAndPolice.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))




