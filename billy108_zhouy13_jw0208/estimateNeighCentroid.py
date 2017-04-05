import json
import dml
import prov.model
import datetime
import uuid


'''
By looking at all the building permits data with its coordinates and neighborhood in Boston,
we use K-means trying to estimate the centroid coordinates of a neighborhood.
'''

class estimateNeighCentroid(dml.Algorithm):

    contributor = 'billy108_zhouy13_jw0208'
    reads = ['billy108_zhouy13_jw0208.buildingPermits']
    writes = ['billy108_zhouy13_jw0208.neighborhoodCentroid']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('billy108_zhouy13_jw0208', 'billy108_zhouy13_jw0208')
        buildingPermits = repo['billy108_zhouy13_jw0208.buildingPermits']

        neighCoordinates = []
        # for each entry, project its neighborhood and the neiborhood's coordinates
        for entry in buildingPermits.find():
            if 'location' in entry and 'city' in entry:
                if ('coordinates' in entry['location'] and len(entry['location']['coordinates']) > 0):
                    list = [entry['location']['coordinates'][0], entry['location']['coordinates'][1]]
                    neighCoordinates.append(
                        {'neighborhood':entry['city'],
                         'location':{'coordinates':list}
                         }
                    )

        # print(len(neighCoordinates))
        #standardize neighborhood names
        neighCoordinatesUpdated = []
        for entry in neighCoordinates:
                if entry['neighborhood'].lower() == 'allston' or entry['neighborhood'].lower() == 'brighton':
                    neighCoordinatesUpdated.append(
                        {"neighborhood": 'allston/brighton', 'location': entry['location']}
                    )
                elif entry['neighborhood'].lower() == 'back bay' or entry['neighborhood'].lower() == 'beacon hill' :
                    neighCoordinatesUpdated.append(
                        {"neighborhood": 'back bay/beacon hill', 'location': entry['location']}
                    )
                elif entry['neighborhood'].lower() == 'e boston':
                    neighCoordinatesUpdated.append(
                        {"neighborhood": 'east boston', 'location': entry['location']}
                    )
                elif entry['neighborhood'].lower() == 'financial district':
                    neighCoordinatesUpdated.append(
                        {"neighborhood": 'boston', 'location': entry['location']}
                    )
                elif entry['neighborhood'].lower() == 'chestnut hill':
                    neighCoordinatesUpdated.append(
                        {"neighborhood": 'brookline', 'location': entry['location']}
                    )
                else:
                    neighCoordinatesUpdated.append(
                        {"neighborhood": entry['neighborhood'].lower(), 'location': entry['location']}
                    )

        # print(neighCoordinatesUpdated)
        # print(len(neighCoordinatesUpdated))

        # # Helper functions
        def aggregate(R, f):
            keys = {r['neighborhood'] for r in R}
            return [{'neighborhood': key,
                     'latitude': f([r for r in R if r['neighborhood'] == key])[0],
                     'longitude': f([r for r in R if r['neighborhood'] == key])[1]} for key in keys]

        def average(args):
            coordinates = [0, 0]
            length = len(args)
            for i in args:
                coordinates[0] += i['latitude']
                coordinates[1] += i['longitude']
            coordinates[0] /= length
            coordinates[1] /= length
            return tuple(coordinates)


        neighCentroid = []

        for doc in neighCoordinatesUpdated:
                        entry = {
                            'neighborhood': doc['neighborhood'],
                            'longitude': doc['location']['coordinates'][0],
                            'latitude': doc['location']['coordinates'][1]
                        }
                        neighCentroid.append(entry)

        neighCentroid = aggregate(neighCentroid, average)

        # print(neighCentroid)
        # print(len(neighCentroid))

        repo.dropPermanent("neighborhoodCentroid")
        repo.createPermanent("neighborhoodCentroid")
        repo['billy108_zhouy13_jw0208.neighborhoodCentroid'].insert_many(neighCentroid)

        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}


    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        # Set up the database connection
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('billy108_zhouy13_jw0208', 'billy108_zhouy13_jw0208')
        buildingPermits = repo['billy108_zhouy13_jw0208.buildingPermits']

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont',
                          'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('cdp', 'https://data.cambridgema.gov/')
        doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')

        this_script = doc.agent('alg:billy108_zhouy13_jw0208#estimateNeighCentroid',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_buildingPermits = doc.entity('dat:billy108_zhouy13_jw0208#buildingPermits',
                              {'prov:label': 'Building Permits data in Boston', prov.model.PROV_TYPE: 'ont:DataResource',
                               'ont:Extension': 'json'})

        findNeighCentroid = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                                         {
                                                             prov.model.PROV_LABEL:
                                                                 "Use K-means algorithm to find neighborhood's centroid coordinates"})

        doc.wasAssociatedWith(findNeighCentroid, this_script)

        doc.usage(findNeighCentroid, resource_buildingPermits, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        # Result dataset entities
        neighborhoodCentroid = doc.entity('dat:billy108_zhouy13_jw0208#neighborhoodCentroid',
                                                   {prov.model.PROV_LABEL: 'Centroid coordinates of neighborhoods',
                                                    prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(neighborhoodCentroid, this_script)
        doc.wasGeneratedBy(neighborhoodCentroid, findNeighCentroid, endTime)
        doc.wasDerivedFrom(neighborhoodCentroid, resource_buildingPermits, findNeighCentroid,
                           findNeighCentroid,
                           findNeighCentroid)

        repo.logout()

        return doc

# estimateNeighCentroid.execute()
# doc = estimateNeighCentroid.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
