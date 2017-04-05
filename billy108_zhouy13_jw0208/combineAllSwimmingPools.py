
import json
import dml
import prov.model
import datetime
import uuid



class combineAllSwimmingPools(dml.Algorithm):
    contributor = 'billy108_zhouy13_jw0208'
    reads = ['billy108_zhouy13_jw0208.seasonalSwimPools', 'billy108_zhouy13_jw0208.commCenterPools']
    writes = ['billy108_zhouy13_jw0208.allPoolsInBoston']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('billy108_zhouy13_jw0208', 'billy108_zhouy13_jw0208')

        # Get the collections
        seasonalPools = repo['billy108_zhouy13_jw0208.seasonalSwimPools']
        commCenterPools = repo['billy108_zhouy13_jw0208.commCenterPools']

        # Get names, neighborhood and zipcode of all seasonal pools and put them into (key, value) form
        allPools_list = []
        for entry in seasonalPools.find({"location_1_city": {"$exists": True}}):
            if entry['location_1_city'].lower() == 'allston' or entry['location_1_city'].lower() == 'brighton' \
                    or entry['location_1_city'].lower() == 'allston / brighton':
                allPools_list.append(
                    {"name": entry['business_name'], "value": {'neighborhood': 'allston/brighton',
                                                               'zip': entry['location_1_zip']}}
                )
            else:
                allPools_list.append(
                    {"name": entry['business_name'], "value": {'neighborhood': entry['location_1_city'].lower(),
                                                               'zip': entry['location_1_zip']}}
                )

        # Get names, neighborhood and zipcode of community center pools and put them into (key, value) form
        for entry in commCenterPools.find():
            if entry['properties'].get('NEIGH').lower() == 'jamaica plai':
                allPools_list.append(
                    {"name": entry['properties'].get('SITE'),
                     "value": {'neighborhood': 'jamaica plain',
                               "zip": (str(0) + str(entry['properties'].get('ZIP')))}}
                )
            else:
                allPools_list.append(
                    {"name": entry['properties'].get('SITE'),
                     "value": {'neighborhood': entry['properties'].get('NEIGH').lower(),
                               "zip": (str(0) + str(entry['properties'].get('ZIP')))}}
                )

        # Create a new collection and insert the result data set
        repo.dropCollection('allPoolsInBoston')
        repo.createCollection('allPoolsInBoston')
        repo['billy108_zhouy13_jw0208.allPoolsInBoston'].insert_many(allPools_list)

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
        this_script = doc.agent('alg:billy108_zhouy13_jw0208#combineAllSwimmingPools',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resources
        resource_seasonalSwimPools = doc.entity('dat:billy108_zhouy13_jw0208#seasonalSwimPools',
                                                {'prov:label': 'Seasonal Swimming Pools in Boston',
                                                 prov.model.PROV_TYPE: 'ont:DataResource',
                                                 'ont:Extension': 'json'})

        resource_commCenterPools = doc.entity('dat:billy108_zhouy13_jw0208#commCenterPools',
                                              {'prov:label': 'Community Center Pools in Boston',
                                               prov.model.PROV_TYPE: 'ont:DataResource',
                                               'ont:Extension': 'json'})

        # Activities
        combine_allPoolsInBoston = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                {
                                                    prov.model.PROV_LABEL: "Combine all swimming pools in Boston",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})

        # Activities' Associations with Agent
        doc.wasAssociatedWith(combine_allPoolsInBoston, this_script)


        # Record which activity used which resource
        doc.usage(combine_allPoolsInBoston, resource_seasonalSwimPools, startTime)
        doc.usage(combine_allPoolsInBoston, resource_commCenterPools, startTime)

        # Result dataset entity
        allPoolsInBoston = doc.entity('dat:billy108_zhouy13_jw0208#allPoolsInBoston',
                                       {prov.model.PROV_LABEL: 'All swimming pools in Boston',
                                        prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(allPoolsInBoston, this_script)
        doc.wasGeneratedBy(allPoolsInBoston, combine_allPoolsInBoston, endTime)
        doc.wasDerivedFrom(allPoolsInBoston, resource_seasonalSwimPools, combine_allPoolsInBoston, combine_allPoolsInBoston,
                           combine_allPoolsInBoston)
        doc.wasDerivedFrom(allPoolsInBoston, resource_commCenterPools, combine_allPoolsInBoston, combine_allPoolsInBoston,
                           combine_allPoolsInBoston)

        repo.logout()

        return doc


# combineAllSwimmingPools.execute()
# doc = combineAllSwimmingPools.provenance()
# # print(doc.get_provn())
# # print(json.dumps(json.loads(doc.serialize()), indent=4))