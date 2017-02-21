from urllib.request import urlopen, Request
import urllib
import json
import dml
import prov.model
import datetime
import uuid
import sodapy


class retrieveData(dml.Algorithm):
    contributor = 'billy108_zhou13'
    reads = []
    writes = ['billy108_zhou13.seasonalSwimPools', 'billy108_zhou13.communityGardens',
              'billy108_zhou13.openSpaceCambridge', 'billy108_zhou13.waterplayCambridge',
              'billy108_zhou13.openSpaceBoston', 'billy108_zhou13.commCenterPools']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('billy108_zhou13', 'billy108_zhou13')

        # get Seasonal Swimming pools data in Boston
        client = sodapy.Socrata("data.cityofboston.gov", None)
        response = client.get("xw3e-c7pz")
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        repo.dropCollection("seasonalSwimPools")
        repo.createCollection("seasonalSwimPools")
        repo['billy108_zhou13.seasonalSwimPools'].insert_many(r)
        repo['billy108_zhou13.seasonalSwimPools'].metadata({'complete': True})
        print(repo['billy108_zhou13.seasonalSwimPools'].metadata())

        # get Community Gardens data in Boston
        client = sodapy.Socrata("data.cityofboston.gov", None)
        response = client.get("rdqf-ter7")
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        repo.dropCollection("communityGardens")
        repo.createCollection("communityGardens")
        repo['billy108_zhou13.communityGardens'].insert_many(r)
        repo['billy108_zhou13.communityGardens'].metadata({'complete': True})
        print(repo['billy108_zhou13.communityGardens'].metadata())

        # get recreational open space data in Cambridge
        client = sodapy.Socrata("data.cambridgema.gov", None)
        response = client.get("5ctr-ccas")
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        repo.dropCollection("openSpaceCambridge")
        repo.createCollection("openSpaceCambridge")
        repo['billy108_zhou13.openSpaceCambridge'].insert_many(r)
        repo['billy108_zhou13.openSpaceCambridge'].metadata({'complete': True})
        print(repo['billy108_zhou13.openSpaceCambridge'].metadata())

        # get recreational waterplay parks data in Cambridge
        client = sodapy.Socrata("data.cambridgema.gov", None)
        response = client.get("hv2t-vv6d")
        r = json.loads(json.dumps(response, sort_keys=True, indent=2))
        repo.dropCollection("waterplayCambridge")
        repo.createCollection("waterplayCambridge")
        repo['billy108_zhou13.waterplayCambridge'].insert_many(r)
        repo['billy108_zhou13.waterplayCambridge'].metadata({'complete': True})
        print(repo['billy108_zhou13.waterplayCambridge'].metadata())

        # Get data of Open spaces of conservation and recreation interest in Boston
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/2868d370c55d4d458d4ae2224ef8cddd_7.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("openSpaceBoston")
        repo.createCollection("openSpaceBoston")
        repo['billy108_zhou13.openSpaceBoston'].insert_many(r['features'])
        repo['billy108_zhou13.openSpaceBoston'].metadata({'complete': True})
        print(repo['billy108_zhou13.openSpaceBoston'].metadata())

        # Get data of Community Center Pools in Boston
        url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/5575f763dbb64effa36acd67085ef3a8_0.geojson'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("commCenterPools")
        repo.createCollection("commCenterPools")
        repo['billy108_zhou13.commCenterPools'].insert_many(r['features'])
        repo['billy108_zhou13.commCenterPools'].metadata({'complete': True})
        print(repo['billy108_zhou13.commCenterPools'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('billy108_zhou13', 'billy108_zhou13')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/')
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        # Agent
        this_script = doc.agent('alg:billy108_zhou13#retrieveData',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resources
        resource_seasonalPools = doc.entity('bdp:xw3e-c7pz',
                                            {'prov:label': 'Seasonal Swimming Pools in Boston',
                                             prov.model.PROV_TYPE: 'ont:DataResource',
                                             'ont:Extension': 'json'})
        resource_communityGardens = doc.entity('bdp:rdqf-ter7',
                                               {'prov:label': 'Community Gardens in Boston',
                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                'ont:Extension': 'json'})
        resource_openSpaceCambridge = doc.entity('cdp:5ctr-ccas',
                                                 {'prov:label': 'Open Spaces in Cambridge',
                                                  prov.model.PROV_TYPE: 'ont:DataResource',
                                                  'ont:Extension': 'json'})
        resource_waterplayCambridge = doc.entity('cdp:hv2t-vv6d',
                                                 {'prov:label': 'Waterplay Park Locations in Cambridge',
                                                  prov.model.PROV_TYPE: 'ont:DataResource',
                                                  'ont:Extension': 'json'})
        resource_openSpaceBoston = doc.entity('bod:2868d370c55d4d458d4ae2224ef8cddd_7',
                                              {'prov:label': 'Open Spaces in Boston',
                                               prov.model.PROV_TYPE: 'ont:DataResource',
                                               'ont:Extension': 'geojson'})
        resource_commCenterPools = doc.entity('bod:5575f763dbb64effa36acd67085ef3a8_0',
                                              {'prov:label': 'Community Center Pools in Boston',
                                               prov.model.PROV_TYPE: 'ont:DataResource',
                                               'ont:Extension': 'geojson'})

        # Activities
        get_seasonalPools = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                         {prov.model.PROV_TYPE: 'ont:Retrieval'})
        get_communityGardens = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                            {prov.model.PROV_TYPE: 'ont:Retrieval'})
        get_openSpaceCambridge = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                              {prov.model.PROV_TYPE: 'ont:Retrieval'})
        get_waterplayCambridge = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                              {prov.model.PROV_TYPE: 'ont:Retrieval'})
        get_openSpaceBoston = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                           {prov.model.PROV_TYPE: 'ont:Retrieval'})
        get_commCenterPools = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                           {prov.model.PROV_TYPE: 'ont:Retrieval'})

        # Activities' Associations with Agent
        doc.wasAssociatedWith(get_seasonalPools, this_script)
        doc.wasAssociatedWith(get_communityGardens, this_script)
        doc.wasAssociatedWith(get_openSpaceCambridge, this_script)
        doc.wasAssociatedWith(get_waterplayCambridge, this_script)
        doc.wasAssociatedWith(get_openSpaceBoston, this_script)
        doc.wasAssociatedWith(get_commCenterPools, this_script)

        # Record which activity used which resource
        doc.usage(get_seasonalPools, resource_seasonalPools, startTime)
        doc.usage(get_communityGardens, resource_communityGardens, startTime)
        doc.usage(get_openSpaceCambridge, resource_openSpaceCambridge, startTime)
        doc.usage(get_waterplayCambridge, resource_waterplayCambridge, startTime)
        doc.usage(get_openSpaceBoston, resource_openSpaceBoston, startTime)
        doc.usage(get_commCenterPools, resource_commCenterPools, startTime)

        seasonalSwimPools = doc.entity('dat:billy108_zhou13#seasonalSwimPools',
                                       {prov.model.PROV_LABEL: 'Seasonal Swimming Pools',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(seasonalSwimPools, this_script)
        doc.wasGeneratedBy(seasonalSwimPools, get_seasonalPools, endTime)
        doc.wasDerivedFrom(seasonalSwimPools, resource_seasonalPools, get_seasonalPools, get_seasonalPools,
                           get_seasonalPools)

        communityGardens = doc.entity('dat:billy108_zhou13#communityGardens',
                                      {prov.model.PROV_LABEL: 'Community Gardens in Boston',
                                       prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(communityGardens, this_script)
        doc.wasGeneratedBy(communityGardens, get_communityGardens, endTime)
        doc.wasDerivedFrom(communityGardens, resource_communityGardens, get_communityGardens, get_communityGardens,
                           get_communityGardens)

        openSpaceCambridge = doc.entity('dat:billy108_zhou13#openSpaceCambridge',
                                        {prov.model.PROV_LABEL: 'Open Spaces in Cambridge',
                                         prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(openSpaceCambridge, this_script)
        doc.wasGeneratedBy(openSpaceCambridge, get_openSpaceCambridge, endTime)
        doc.wasDerivedFrom(openSpaceCambridge, resource_openSpaceCambridge, get_openSpaceCambridge,
                           get_openSpaceCambridge,
                           get_openSpaceCambridge)

        waterplayCambridge = doc.entity('dat:billy108_zhou13#waterplayCambridge',
                                        {prov.model.PROV_LABEL: 'Waterplay Parks in Cambridge',
                                         prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(waterplayCambridge, this_script)
        doc.wasGeneratedBy(waterplayCambridge, get_waterplayCambridge, endTime)
        doc.wasDerivedFrom(waterplayCambridge, resource_waterplayCambridge, get_waterplayCambridge,
                           get_waterplayCambridge,
                           get_waterplayCambridge)

        openSpaceBoston = doc.entity('dat:billy108_zhou13#openSpaceBoston',
                                     {prov.model.PROV_LABEL: 'Open Spaces in Boston',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(openSpaceBoston, this_script)
        doc.wasGeneratedBy(openSpaceBoston, get_openSpaceBoston, endTime)
        doc.wasDerivedFrom(openSpaceBoston, resource_openSpaceBoston, get_openSpaceBoston,
                           get_openSpaceBoston,
                           get_openSpaceBoston)

        commCenterPools = doc.entity('dat:billy108_zhou13#commCenterPools',
                                     {prov.model.PROV_LABEL: 'Community Center Pools in Boston',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(commCenterPools, this_script)
        doc.wasGeneratedBy(commCenterPools, get_commCenterPools, endTime)
        doc.wasDerivedFrom(commCenterPools, resource_commCenterPools, get_commCenterPools,
                           get_commCenterPools,
                           get_commCenterPools)

        repo.logout()

        return doc


# retrieveData.execute()
# doc = retrieveData.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

# eof
