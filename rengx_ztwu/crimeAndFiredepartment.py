import urllib
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
from bson.code import Code
from bson.json_util import dumps



class crimeAndFiredpartment(dml.Algorithm):

    contributor = 'rengx_ztwu'
    reads = ['rengx_ztwu.firestation','rengx_ztwu.crimereports']
    writes = ['rengx_ztwu.CrimeAndFire']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rengx_ztwu', 'rengx_ztwu')

        # Get the collections
        firestation = repo['rengx_ztwu.firestation']
        crimereports = repo['rengx_ztwu.crimereports']

        # Get names and zipcode of all schools and put them into (key, value) form
        CrimeAndFire = []
        for entry in firestation.find({"LOCADDR": {"$exists": True}}):
            CrimeAndFire.append(
                {"name": entry['LOCNAME'], "value": {'street': entry['LOCADDR']}
                 })

        for entry in crimereports.find({"STREETNAME": {"$exists": True}}):
            CrimeAndFire.append(
                {"name": entry['COMPNOS'], "value": {'street': entry['STREETNAME']}
                })


    # Create a new collection and insert the result data set
        repo.dropCollection('CrimeAndFireDB')
        repo.createCollection('CrimeAndFireDB')
        repo['rengx_ztwu.CrimeAndFireDB'].insert_many(CrimeAndFire)

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
        this_script = doc.agent('alg:rengx_ztwu#CrimeAndFire',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resources
        resource_firestation = doc.entity('dat:rengx_ztwu#firestation',
                                                {'prov:label': 'firestation in Boston',
                                                 prov.model.PROV_TYPE: 'ont:DataResource',
                                                 'ont:Extension': 'json'})

        resource_crimereports = doc.entity('dat:rengx_ztwu#crimereports',
                                              {'prov:label': 'crimereports in Boston',
                                               prov.model.PROV_TYPE: 'ont:DataResource',
                                               'ont:Extension': 'json'})

        # Activities
        combine_CrimeAndFire = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                {
                                                    prov.model.PROV_LABEL: "Combine all Crime And Fire in Boston",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})

        # Activities' Associations with Agent
        doc.wasAssociatedWith(combine_CrimeAndFire, this_script)

        # Record which activity used which resource
        doc.usage(combine_CrimeAndFire, resource_firestation, startTime)
        doc.usage(combine_CrimeAndFire, resource_crimereports, startTime)

        # Result dataset entity
        CrimeAndFire = doc.entity('dat:rengx_ztwu#schoolandhosptial',
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




