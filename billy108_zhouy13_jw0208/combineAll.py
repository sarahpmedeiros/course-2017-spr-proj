
import json
import dml
import prov.model
import datetime
import uuid

class combineAll(dml.Algorithm):

    contributor = 'billy108_zhouy13_jw0208'
    reads = ['billy108_zhouy13_jw0208.waterplayCambridge','billy108_zhouy13_jw0208.allOpenSpacesInBoston','billy108_zhouy13_jw0208.allPoolsInBoston']
    writes = ['billy108_zhouy13_jw0208.allRecreationalPlaces']

    @staticmethod
    def execute(trial = True):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('billy108_zhouy13_jw0208', 'billy108_zhouy13_jw0208')

        # Get the collections
        waterplayCambridge = repo['billy108_zhouy13_jw0208.waterplayCambridge']
        allOpenSpacesBoston = repo['billy108_zhouy13_jw0208.allOpenSpacesInBoston']
        allPoolsInBoston = repo['billy108_zhouy13_jw0208.allPoolsInBoston']

        #Get names, neighborhood of all water play parks in Cambridge
        allRecreationalPlaces_list = []
        for entry in waterplayCambridge.find():
            allRecreationalPlaces_list.append(
                {"name": entry['park'], 'neighborhood': 'cambridge'}
            )

        #get names, neighborhood of all open spaces in Boston
        for entry in allOpenSpacesBoston.find():
            allRecreationalPlaces_list.append(
                {"name": entry['name'], 'neighborhood': entry['neighborhood']}
            )

        #get names, neighborhood of all swimming pools in Boston
        for entry in allPoolsInBoston.find():
            allRecreationalPlaces_list.append(
                {"name": entry['name'], 'neighborhood': entry['value'].get('neighberhood')}
            )


        # Create a new collection and insert the result data set
        repo.dropCollection('allRecreationalPlaces')
        repo.createCollection('allRecreationalPlaces')
        repo['billy108_zhouy13_jw0208.allRecreationalPlaces'].insert_many(allRecreationalPlaces_list)


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
        this_script = doc.agent('alg:billy108_zhouy13_jw0208#combineAll',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resources
        resource_waterplayCambridge = doc.entity('dat:billy108_zhouy13_jw0208#waterplayCambridge',
                                               {'prov:label': 'Waterplay Parks in Cambridge',
                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                'ont:Extension': 'json'})

        resource_allOpenSpacesInBoston = doc.entity('dat:billy108_zhouy13_jw0208#allOpenSpacesInBoston',
                                                 {'prov:label': 'All Open Spaces in Boston',
                                                  prov.model.PROV_TYPE: 'ont:DataResource',
                                                  'ont:Extension': 'json'})

        resource_allPoolsInBoston = doc.entity('dat:billy108_zhouy13_jw0208#allPoolsInBoston',
                                              {'prov:label': 'All Swimming Pools in Boston',
                                               prov.model.PROV_TYPE: 'ont:DataResource',
                                               'ont:Extension': 'json'})
        # Activities
        combine_allRecreationalPlaces = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                             {
                                                 prov.model.PROV_LABEL: "Combine all recreational places in Boston",
                                                 prov.model.PROV_TYPE: 'ont:Computation'})

        # Activities' Associations with Agent
        doc.wasAssociatedWith(combine_allRecreationalPlaces, this_script)

        # Record which activity used which resource
        doc.usage(combine_allRecreationalPlaces, resource_waterplayCambridge, startTime)
        doc.usage(combine_allRecreationalPlaces, resource_allOpenSpacesInBoston, startTime)
        doc.usage(combine_allRecreationalPlaces, resource_allPoolsInBoston, startTime)

        # Result dataset entity
        allRecreationalPlaces = doc.entity('dat:billy108_zhouy13_jw0208#allRecreationalPlaces',
                                           {prov.model.PROV_LABEL: 'All recreational places in Boston',
                                            prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(allRecreationalPlaces, this_script)
        doc.wasGeneratedBy(allRecreationalPlaces, combine_allRecreationalPlaces, endTime)
        doc.wasDerivedFrom(allRecreationalPlaces, resource_waterplayCambridge, combine_allRecreationalPlaces,
                           combine_allRecreationalPlaces,
                           combine_allRecreationalPlaces)
        doc.wasDerivedFrom(allRecreationalPlaces, resource_allOpenSpacesInBoston, combine_allRecreationalPlaces,
                           combine_allRecreationalPlaces,
                           combine_allRecreationalPlaces)
        doc.wasDerivedFrom(allRecreationalPlaces, resource_allPoolsInBoston, combine_allRecreationalPlaces,
                           combine_allRecreationalPlaces,
                           combine_allRecreationalPlaces)

        repo.logout()

        return doc


# combineAll.execute()
# doc = combineAll.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
