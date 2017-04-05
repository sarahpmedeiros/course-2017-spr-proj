import json
import dml
import prov.model
import datetime
import uuid
from random import shuffle
from math import sqrt


class statsAnalysisForRecreatPlaces(dml.Algorithm):
    contributor = 'billy108_zhouy13_jw0208'
    reads = ['billy108_zhouy13_jw0208.numOfOpenSpaces', 'billy108_zhouy13_jw0208.numOfPools',
             'billy108_zhouy13_jw0208.numOfHubwayPerNeigh']
    writes = ['billy108_zhouy13_jw0208.RecreatPlacesStats']

    @staticmethod
    def execute(trial=False):

        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('billy108_zhouy13_jw0208', 'billy108_zhouy13_jw0208')

        # Get the collections
        numOfOpenSpacesBoston = repo['billy108_zhouy13_jw0208.numOfOpenSpaces']
        numOfPoolsBoston = repo['billy108_zhouy13_jw0208.numOfPools']
        numOfHubwayPerNeigh = repo['billy108_zhouy13_jw0208.numOfHubwayPerNeigh']

        x = []
        y = []
        z = []
        area = []

        #
        for entry in numOfOpenSpacesBoston.find():
            x.append(entry['value'].get('numOfOpenSpace'))
            area.append(entry['_id'])

        #
        for entry in numOfPoolsBoston.find():
            y.append(entry['value'].get('numOfPool'))

        for entry in numOfHubwayPerNeigh.find():
            z.append(entry['value'].get('numOfHubway'))

        def permute(x):
            shuffled = [xi for xi in x]
            shuffle(shuffled)
            return shuffled

        def avg(x):  # Average
            return sum(x) / len(x)

        def stddev(x):  # Standard deviation.
            m = avg(x)
            return sqrt(sum([(xi - m) ** 2 for xi in x]) / len(x))

        def cov(x, y):  # Covariance.
            return sum([(xi - avg(x)) * (yi - avg(y)) for (xi, yi) in zip(x, y)]) / len(x)

        def corr(x, y):  # Correlation coefficient.
            if stddev(x) * stddev(y) != 0:
                return cov(x, y) / (stddev(x) * stddev(y))

        def p(x, y):
            c0 = corr(x, y)
            corrs = []
            for k in range(0, 2000):
                y_permuted = permute(y)
                corrs.append(corr(x, y_permuted))
            return len([c for c in corrs if abs(c) > c0]) / len(corrs)

        stats = []
        stats.append({'_id': 'openSpaceAndpool', 'corr': corr(x, y), 'p value': p(x, y)})
        stats.append({'_id': 'openSpaceAndHubway', 'corr': corr(x, z), 'p value': p(x, z)})
        stats.append({'_id': 'poolAndHubway', 'corr': corr(y, z), 'p value': p(y, z)})


        repo.dropCollection('RecreatPlacesStats')
        repo.createCollection('RecreatPlacesStats')
        repo['billy108_zhouy13_jw0208.RecreatPlacesStats'].insert_many(stats)

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
        this_script = doc.agent('alg:billy108_zhouy13_jw0208#statsAnalysisForRecreatPlaces',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        # Resources
        resource_numOfOpenSpaces = doc.entity('dat:billy108_zhouy13_jw0208#numOfOpenSpaces',
                                              {'prov:label': 'Number of open spaces per neighborhood',
                                               prov.model.PROV_TYPE: 'ont:DataResource',
                                               'ont:Extension': 'json'})

        resource_numOfPools = doc.entity('dat:billy108_zhouy13_jw0208#numOfPools',
                                         {'prov:label': 'Number of pools per neighborhood',
                                          prov.model.PROV_TYPE: 'ont:DataResource',
                                          'ont:Extension': 'json'})

        resource_numOfHubwayPerNeigh = doc.entity('dat:billy108_zhouy13_jw0208#numOfHubwayPerNeigh',
                                                  {'prov:label': 'Number of hubway stations per neighborhood',
                                                   prov.model.PROV_TYPE: 'ont:DataResource',
                                                   'ont:Extension': 'json'})
        # Activities
        doStatsAnalysis = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                                       {
                                           prov.model.PROV_LABEL:
                                               "Compute coorrelation and p-value between the "
                                               "quantity of open spaces, swimming pools and hubway stations",
                                           prov.model.PROV_TYPE: 'ont:Computation'})

        # Activities' Associations with Agent
        doc.wasAssociatedWith(doStatsAnalysis, this_script)

        # Record which activity used which resource
        doc.usage(doStatsAnalysis, resource_numOfOpenSpaces, startTime)
        doc.usage(doStatsAnalysis, resource_numOfPools, startTime)
        doc.usage(doStatsAnalysis, resource_numOfHubwayPerNeigh, startTime)

        # Result dataset entity
        RecreatPlacesStats = doc.entity('dat:billy108_zhouy13_jw0208#RecreatPlacesStats',
                                        {prov.model.PROV_LABEL: 'Statistics between recreational places',
                                         prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(RecreatPlacesStats, this_script)
        doc.wasGeneratedBy(RecreatPlacesStats, doStatsAnalysis, endTime)
        doc.wasDerivedFrom(RecreatPlacesStats, resource_numOfOpenSpaces, doStatsAnalysis,
                           doStatsAnalysis,
                           doStatsAnalysis)
        doc.wasDerivedFrom(RecreatPlacesStats, resource_numOfPools, doStatsAnalysis,
                           doStatsAnalysis,
                           doStatsAnalysis)
        doc.wasDerivedFrom(RecreatPlacesStats, resource_numOfHubwayPerNeigh, doStatsAnalysis,
                           doStatsAnalysis,
                           doStatsAnalysis)

        repo.logout()

        return doc

# statsAnalysisForRecreatPlaces.execute()
# doc = statsAnalysisForRecreatPlaces.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
