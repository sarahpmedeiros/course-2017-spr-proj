import urllib
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
from bson.code import Code
from bson.json_util import dumps

class combineAllOpenSpaces(dml.Algorithm):

    contributor = 'billy108_zhouy13_jw0208'
    reads = ['billy108_zhouy13_jw0208.communityGardens','billy108_zhouy13_jw0208.openSpaceCambridge','billy108_zhouy13_jw0208.openSpaceBoston']
    writes = ['billy108_zhouy13_jw0208.allOpenSpacesInBoston']

    @staticmethod
    def execute(trial = False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('billy108_zhouy13_jw0208', 'billy108_zhouy13_jw0208')

        # Get the collections
        openSpaceCambridge = repo['billy108_zhouy13_jw0208.openSpaceCambridge']
        openSpaceBoston = repo['billy108_zhouy13_jw0208.openSpaceBoston']
        communityGardens = repo['billy108_zhouy13_jw0208.communityGardens']

        #Get names, neighborhood of all open spaces in Cambridge
        allOpenSpaces_list = []
        for entry in openSpaceCambridge.find():
            allOpenSpaces_list.append(
                {"name": entry['name'], 'neighborhood': 'cambridge'}
            )

        #get names, neighborhood of all open spaces in Boston excluding Cambrdige and Brookline
        for entry in openSpaceBoston.find():

            if entry['properties'].get('DISTRICT').lower() == 'allston-brighton':
                allOpenSpaces_list.append({"name": entry['properties'].get('SITE_NAME'),
                                           'neighborhood': 'allston/brighton'})
            elif entry['properties'].get('DISTRICT').lower() == 'north dorchester':
                allOpenSpaces_list.append({"name": entry['properties'].get('SITE_NAME'),
                                           'neighborhood': 'dorchester'})
            elif entry['properties'].get('DISTRICT').lower() == 'central boston':
                allOpenSpaces_list.append({"name": entry['properties'].get('SITE_NAME'),
                                           'neighborhood': 'boston'})
            else:
                allOpenSpaces_list.append(
                    {"name": entry['properties'].get('SITE_NAME'),
                     'neighborhood': entry['properties'].get('DISTRICT').lower()}
                )

        #get names, neighborhood of all community gardens in Boston
        for entry in communityGardens.find():
            if entry['site'] != 'Site name' and entry['area'] != 'Area':
                if entry['area'].lower() == 'allston' or entry['area'].lower() == 'brighton':
                    allOpenSpaces_list.append(
                        {"name": entry['site'], 'neighborhood': 'allston/brighton'}
                    )
                elif entry['area'].lower() == 'back bay' or entry['area'].lower() == 'beacon hill' :
                    allOpenSpaces_list.append(
                        {"name": entry['site'], 'neighborhood': 'back bay/beacon hill'}
                    )
                elif entry['area'].lower() == 'mattapan or dorchester':
                    allOpenSpaces_list.append(
                        {"name": entry['site'], 'neighborhood': 'mattapan'}
                    )
                    allOpenSpaces_list.append(
                        {"name": entry['site'], 'neighborhood': 'dorchester'}
                    )
                else:
                    allOpenSpaces_list.append(
                        {"name": entry['site'], 'neighborhood': entry['area'].lower()}
                    )


        # Create a new collection and insert the result data set
        repo.dropCollection('allOpenSpacesInBoston')
        repo.createCollection('allOpenSpacesInBoston')
        repo['billy108_zhouy13_jw0208.allOpenSpacesInBoston'].insert_many(allOpenSpaces_list)


        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('billy108_zhouy13_jw0208', 'billy108_zhouy13_jw0208')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/')
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        # Agent
        this_script = doc.agent('alg:billy108_zhouy13_jw0208#combineAllOpenSpaces',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resources
        resource_communityGardens = doc.entity('dat:billy108_zhouy13_jw0208#communityGardens',
                                                {'prov:label': 'Community Gardens in Boston',
                                                 prov.model.PROV_TYPE: 'ont:DataResource',
                                                 'ont:Extension': 'json'})

        resource_openSpaceCambridge = doc.entity('dat:billy108_zhouy13_jw0208#openSpaceCambridge',
                                              {'prov:label': 'Open Spaces in Cambridge',
                                               prov.model.PROV_TYPE: 'ont:DataResource',
                                               'ont:Extension': 'json'})

        resource_openSpaceBoston = doc.entity('dat:billy108_zhouy13_jw0208#openSpaceBoston',
                                              {'prov:label': 'Open Spaces in Boston',
                                               prov.model.PROV_TYPE: 'ont:DataResource',
                                               'ont:Extension': 'json'})
        # Activities
        combine_AllOpenSpaces = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                {
                                                    prov.model.PROV_LABEL: "Combine all open spaces in Boston",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})

        # Activities' Associations with Agent
        doc.wasAssociatedWith(combine_AllOpenSpaces, this_script)

        # Record which activity used which resource
        doc.usage(combine_AllOpenSpaces, resource_communityGardens, startTime)
        doc.usage(combine_AllOpenSpaces, resource_openSpaceCambridge, startTime)
        doc.usage(combine_AllOpenSpaces, resource_openSpaceBoston, startTime)

        # Result dataset entity
        allOpenSpacesInBoston = doc.entity('dat:billy108_zhouy13_jw0208#allOpenSpacesInBoston',
                                      {prov.model.PROV_LABEL: 'All open spaces in Boston',
                                       prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(allOpenSpacesInBoston, this_script)
        doc.wasGeneratedBy(allOpenSpacesInBoston, combine_AllOpenSpaces, endTime)
        doc.wasDerivedFrom(allOpenSpacesInBoston, resource_communityGardens, combine_AllOpenSpaces,
                           combine_AllOpenSpaces,
                           combine_AllOpenSpaces)
        doc.wasDerivedFrom(allOpenSpacesInBoston, resource_openSpaceCambridge, combine_AllOpenSpaces,
                           combine_AllOpenSpaces,
                           combine_AllOpenSpaces)
        doc.wasDerivedFrom(allOpenSpacesInBoston, resource_openSpaceBoston, combine_AllOpenSpaces,
                           combine_AllOpenSpaces,
                           combine_AllOpenSpaces)

        repo.logout()

        return doc

# combineAllOpenSpaces.execute()
# doc = combineAllOpenSpaces.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))