import urllib
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
from bson.code import Code
from bson.json_util import dumps



class combineschoolandhosptial(dml.Algorithm):

    contributor = 'rengx_ztwu'
    reads = ['rengx_ztwu.publicschool','rengx_ztwu.hosptial']
    writes = ['rengx_ztwu.schoolandhosptial']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rengx_ztwu', 'rengx_ztwu')

        # Get the collections
        publicschool = repo['rengx_ztwu.publicschool']
        hosptial = repo['rengx_ztwu.hosptial']

        # Get names and zipcode of all schools and put them into (key, value) form
        SchoolAndHospital = []
        for entry in publicschool.find({"ZIPCODE": {"$exists": True}}):
            SchoolAndHospital.append(
                {"name": entry['SCH_NAME'], "value": {'zip': entry['ZIPCODE']}
                 })

        for entry in hosptial.find({"ZIPCODE": {"$exists": True}}):
            SchoolAndHospital.append(
                {"name": entry['NAME'], "value": {'zip': entry['ZIPCODE']}
                })


    # Create a new collection and insert the result data set
        repo.dropCollection('SchoolAndHospitalDB')
        repo.createCollection('SchoolAndHospitalDB')
        repo['rengx_ztwu.SchoolAndHospitalDB'].insert_many(SchoolAndHospital)

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rengx_ztwu', 'rengx_ztwu')

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
        this_script = doc.agent('alg:rengx_ztwu#schoolandhosptial',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resources
        resource_publicschool = doc.entity('dat:rengx_ztwu#publicschool',
                                                {'prov:label': 'public school in Boston',
                                                 prov.model.PROV_TYPE: 'ont:DataResource',
                                                 'ont:Extension': 'json'})

        resource_hosptial = doc.entity('dat:rengx_ztwu#hosptial',
                                              {'prov:label': 'hosptials in Boston',
                                               prov.model.PROV_TYPE: 'ont:DataResource',
                                               'ont:Extension': 'json'})

        # Activities
        combine_schoolandhosptial = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                {
                                                    prov.model.PROV_LABEL: "Combine all schools and hostials in Boston",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})

        # Activities' Associations with Agent
        doc.wasAssociatedWith(combine_schoolandhosptial, this_script)

        # Record which activity used which resource
        doc.usage(combine_schoolandhosptial, resource_publicschool, startTime)
        doc.usage(combine_schoolandhosptial, resource_hosptial, startTime)

        # Result dataset entity
        schoolandhosptial = doc.entity('dat:rengx_ztwu#schoolandhosptial',
                                      {prov.model.PROV_LABEL: 'All school and hosptial in Boston',
                                       prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(schoolandhosptial, this_script)
        doc.wasGeneratedBy(schoolandhosptial, combine_schoolandhosptial, endTime)
        doc.wasDerivedFrom(schoolandhosptial, resource_publicschool, combine_schoolandhosptial,
                           combine_schoolandhosptial,
                           combine_schoolandhosptial)
        doc.wasDerivedFrom(schoolandhosptial, resource_hosptial, combine_schoolandhosptial,
                           combine_schoolandhosptial,
                           combine_schoolandhosptial)

        repo.logout()

        return doc

combinehosptial.execute()
doc = combinehosptial.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))




