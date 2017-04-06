import json
import dml
import prov.model
import datetime
import uuid
from bson.json_util import dumps

'''
Map Hubway Stations with their coordinates
to the closest neighborhood.
'''


class mapHubwaysToClosestNeigh(dml.Algorithm):
    contributor = 'billy108_zhouy13_jw0208'
    reads = ['billy108_zhouy13_jw0208.neighborhoodCentroid', 'billy108_zhouy13_jw0208.hubwayStations']
    writes = ['billy108_zhouy13_jw0208.hubwaysInNeigh']

    @staticmethod
    def execute(trial=True):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('billy108_zhouy13_jw0208', 'billy108_zhouy13_jw0208')
        hubwayStations = repo['billy108_zhouy13_jw0208.hubwayStations']
        neighborhoodCentroid = repo['billy108_zhouy13_jw0208.neighborhoodCentroid']

        hubwayStationsList = []

        # 3b. If trial is true, run algorithm in trial mode
        if (trial == True):
            count = 0
            # sample data
            for entry in hubwayStations.find():
                if count >= 40:
                    break
                else:
                    hubwayStationsList.append(entry)
                    count += 1
        else:
            for entry in hubwayStations.find():
                hubwayStationsList.append(entry)


        # print(len(hubwayStationsList))


        # Helper Method
        def dist(p, q):
            (x1, y1) = p
            (x2, y2) = q
            return (x1 - x2) ** 2 + (y1 - y2) ** 2

        hubwayNeighMapping = []

        # For each hubway station with its coordinate, map it with the nearest neighborhood centroid coordinates
        for entry in hubwayStationsList:
            longitude, latitude = entry['geometry']['coordinates'][0], entry['geometry']['coordinates'][1]
            hubwayCoords = tuple((longitude, latitude))

            if longitude != 0 and latitude != 0:
                smallestDist = 100 * 100
                hubwayCoordWithNeigh = {'longitude': hubwayCoords[0], 'latitude': hubwayCoords[1]}

                for doc in neighborhoodCentroid.find():
                    neighLong, neighLat = doc['longitude'], doc['latitude']
                    neighCoords = tuple((neighLong, neighLat))

                    if dist(hubwayCoords, neighCoords) <= smallestDist:
                        smallestDist = dist(hubwayCoords, neighCoords)
                        hubwayCoordWithNeigh['neighborhood'] = doc['neighborhood']

                hubwayNeighMapping.append(hubwayCoordWithNeigh)

        # print(hubwayNeighMapping)
        # print(len(hubwayNeighMapping))
        repo.dropPermanent('hubwaysInNeigh')
        repo.createPermanent('hubwaysInNeigh')
        repo['billy108_zhouy13_jw0208.hubwaysInNeigh'].insert_many(hubwayNeighMapping)

        repo.logout()
        endTime = datetime.datetime.now()

        return {'start': startTime, 'end': endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        # Set up the database connection
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
        this_script = doc.agent('alg:billy108_zhouy13_jw0208#mapHubwaysToClosestNeigh',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resources
        resource_neighborhoodCentroid = doc.entity('dat:billy108_zhouy13_jw0208#neighborhoodCentroid',
                                                   {'prov:label': 'Neighborhood centroid coordinates',
                                                    prov.model.PROV_TYPE: 'ont:DataResource',
                                                    'ont:Extension': 'json'})

        resource_hubwayStations = doc.entity('dat:billy108_zhouy13_jw0208#hubwayStations',
                                             {'prov:label': 'Hubway stations coordinates in Boston',
                                              prov.model.PROV_TYPE: 'ont:DataResource',
                                              'ont:Extension': 'json'})

        # Activities
        mapHubwaysToClosestNeigh = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                {
                                                    prov.model.PROV_LABEL: "Map hubway stations to their nearest neighborhood by using coordinates",
                                                    prov.model.PROV_TYPE: 'ont:Computation'})

        # Activities' Associations with Agent
        doc.wasAssociatedWith(mapHubwaysToClosestNeigh, this_script)

        # Record which activity used which resource
        doc.usage(mapHubwaysToClosestNeigh, resource_neighborhoodCentroid, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(mapHubwaysToClosestNeigh, resource_hubwayStations, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        # Result dataset entity
        hubwaysInNeigh = doc.entity('dat:billy108_zhouy13_jw0208#hubwaysInNeigh',
                                    {prov.model.PROV_LABEL: 'Hubway stations with their associated neighborhoods',
                                     prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(hubwaysInNeigh, this_script)
        doc.wasGeneratedBy(hubwaysInNeigh, mapHubwaysToClosestNeigh, endTime)
        doc.wasDerivedFrom(hubwaysInNeigh, resource_neighborhoodCentroid, mapHubwaysToClosestNeigh,
                           mapHubwaysToClosestNeigh,
                           mapHubwaysToClosestNeigh)
        doc.wasDerivedFrom(hubwaysInNeigh, resource_hubwayStations, mapHubwaysToClosestNeigh,
                           mapHubwaysToClosestNeigh,
                           mapHubwaysToClosestNeigh)

        repo.logout()

        return doc

# mapHubwaysToClosestNeigh.execute()
# doc = mapHubwaysToClosestNeigh.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
