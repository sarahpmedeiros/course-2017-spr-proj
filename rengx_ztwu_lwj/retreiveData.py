import datetime
import json
import urllib
import uuid
from urllib.request import urlopen
import dml
import prov.model
import sodapy


class retrieveData(dml.Algorithm):
    contributor = 'rengx_ztwu_lwj'
    reads = []
    writes = ['rengx_ztwu_lwj.publicschool', 'rengx_ztwu_lwj.policestation',
              'rengx_ztwu_lwj.firestation','rengx_ztwu_lwj.hosptial',
              'rengx_ztwu_lwj.crimereports']

    @staticmethod
    def execute(trial=False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rengx_ztwu_lwj', 'rengx_ztwu_lwj')

        # get public school data in Boston
        client = sodapy.Socrata("data.cityofboston.gov", None)
        response = client.get("492y-i77g")
        # print(json.dumps(response, sort_keys=True, indent=2))
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("publicschool")
        repo.createCollection("publicschool")
        repo['rengx_ztwu_lwj.publicschool'].insert_many(r)
        repo['rengx_ztwu_lwj.publicschool'].metadata({'complete': True})
        print(repo['rengx_ztwu_lwj.publicschool'].metadata())

        # get Police Station data in Boston
        client = sodapy.Socrata("data.cityofboston.gov", None)
        response = client.get("pyxn-r3i2")
        # print(json.dumps(response, sort_keys=True, indent=2))
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("policestation")
        repo.createCollection("policestation")
        repo['rengx_ztwu_lwj.policestation'].insert_many(r)
        repo['rengx_ztwu_lwj.policestation'].metadata({'complete': True})
        print(repo['rengx_ztwu_lwj.policestation'].metadata())

        # get Crime Reports in Boston
        client = sodapy.Socrata("data.cityofboston.gov", None)
        response = client.get("crime")
        # print(json.dumps(response, sort_keys=True, indent=2))
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crimereports")
        repo.createCollection("crimereports")
        repo['rengx_ztwu_lwj.crimereports'].insert_many(r)
        repo['rengx_ztwu_lwj.crimereports'].metadata({'complete': True})
        print(repo['rengx_ztwu_lwj.crimereports'].metadata())


        # get hosptial in Boston
        client = sodapy.Socrata("data.cityofboston.gov", None)
        response = client.get("u6fv-m8v4")
        # print(json.dumps(response, sort_keys=True, indent=2))
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("hosptial")
        repo.createCollection("hosptial")
        repo['rengx_ztwu_lwj.hosptial'].insert_many(r)
        repo['rengx_ztwu_lwj.hosptial'].metadata({'complete': True})
        print(repo['rengx_ztwu_lwj.hosptial'].metadata())


        # Get data of Fire Stations in Boston
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/092857c15cbb49e8b214ca5e228317a1_2.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        # print(json.dumps(json.loads(response), sort_keys=True, indent=2))
        r = json.loads(response)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("firestation")
        repo.createCollection("firestation")
        repo['rengx_ztwu_lwj.firestation'].insert_many(r['features'])
        repo['rengx_ztwu_lwj.firestation'].metadata({'complete': True})
        print(repo['rengx_ztwu_lwj.firestation'].metadata())



        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('rengx_ztwu_lwj', 'rengx_ztwu_lwj')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')


        #agent
        this_script = doc.agent('alg:rengx_ztwu_lwj#retreiveData',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        publicschool_resource = doc.entity('bdp:492y-i77g', {'prov:label': 'Public School',
                                                                   prov.model.PROV_TYPE: 'ont:DataResource',
                                                                   'ont:Extension': 'json'})
        policestation_resource = doc.entity('bdp:pyxn-r3i2', {'prov:label': 'Police Station',
                                                                  prov.model.PROV_TYPE: 'ont:DataResource',
                                                                  'ont:Extension': 'json'})

        crimereports_resource = doc.entity('bdp:crime', {'prov:label': 'Crime Report',
                                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                                'ont:Extension': 'json'})

        hosptial_resource = doc.entity('bdp:u6fv-m8v4', {'prov:label': 'hosptial',
                                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                                'ont:Extension': 'json'})


        firestation_resource = doc.entity('bod:092857c15cbb49e8b214ca5e228317a1_2', {'prov:label': 'Fire Station',
                                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                                'ont:Extension': 'geojson'})






        #activities
        get_publicschool = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_policestation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_crimereports = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_firestation = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_hosptial = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_publicschool, this_script)
        doc.wasAssociatedWith(get_policestation, this_script)
        doc.wasAssociatedWith(get_crimereports, this_script)
        doc.wasAssociatedWith(get_firestation, this_script)
        doc.wasAssociatedWith(get_hosptial, this_script)

        doc.usage(get_publicschool, publicschool_resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_policestation, policestation_resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_crimereports, crimereports_resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_hosptial, hosptial_resource, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_firestation, firestation_resource, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})


        publicschool = doc.entity('dat:rengx_ztwu_lwj#publicschool',
                               {prov.model.PROV_LABEL: 'Public school Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(publicschool, this_script)
        doc.wasGeneratedBy(publicschool, get_publicschool, endTime)
        doc.wasDerivedFrom(publicschool, publicschool_resource)

        policestation = doc.entity('dat:rengx_ztwu_lwj#policestation',
                             {prov.model.PROV_LABEL: 'policestation Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(policestation, this_script)
        doc.wasGeneratedBy(policestation, get_policestation, endTime)
        doc.wasDerivedFrom(policestation, policestation_resource)

        firestation = doc.entity('dat:rengx_ztwu_lwj#firestation',
                            {prov.model.PROV_LABEL: 'firestation Locations', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(firestation, this_script)
        doc.wasGeneratedBy(firestation, get_firestation, endTime)
        doc.wasDerivedFrom(firestation, firestation_resource)

        crimereports = doc.entity('dat:rengx_ztwu_lwj#crimereports',
                                     {prov.model.PROV_LABEL: 'crimereports',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(crimereports, this_script)
        doc.wasGeneratedBy(crimereports, get_crimereports, endTime)
        doc.wasDerivedFrom(crimereports, crimereports_resource)

        hosptial = doc.entity('dat:rengx_ztwu_lwj#hosptial',
                                     {prov.model.PROV_LABEL: 'hosptial',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(hosptial, this_script)
        doc.wasGeneratedBy(hosptial, get_hosptial, endTime)
        doc.wasDerivedFrom(hosptial, hosptial_resource)

        repo.record(doc.serialize())
        repo.logout()

        return doc


retrieveData.execute()
# doc = retrieveData.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof

