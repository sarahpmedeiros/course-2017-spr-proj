import dml
import prov.model
import datetime
import uuid
from bson.code import Code
import json


class count(dml.Algorithm):
    contributor = 'billy108_zhouy13_jw0208'
    reads = ['billy108_zhouy13_jw0208.allOpenSpacesInBoston', 'billy108_zhouy13_jw0208.allPoolsInBoston',
             'billy108_zhouy13_jw0208.hubwaysInNeigh']
    writes = ['billy108_zhouy13_jw0208.numOfOpenSpaces', 'billy108_zhouy13_jw0208.numOfPools',
              'billy108_zhouy13_jw0208.numOfHubwayPerNeigh']

    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('billy108_zhouy13_jw0208', 'billy108_zhouy13_jw0208')

        # Get the collections
        allOpenSpacesBoston = repo['billy108_zhouy13_jw0208.allOpenSpacesInBoston']
        allPoolsInBoston = repo['billy108_zhouy13_jw0208.allPoolsInBoston']

        # MapReduce functions used to do aggregation with sum
        mapper = Code("""
                       function() {
                            emit(this.neighborhood, {numOfOpenSpace:1});
                        }
                      """)

        reducer = Code("""
                        function(k, vs) {
                            var total = 0;
                            for (var i = 0; i < vs.length; i++) {
                                total += vs[i].numOfOpenSpace;
                            }
                            return {numOfOpenSpace: total};
                        }
                        """)
        repo.dropPermanent("numOfOpenSpaces")
        repo.createCollection('numOfOpenSpaces')
        allOpenSpacesBoston.map_reduce(mapper, reducer, "billy108_zhouy13_jw0208.numOfOpenSpaces")

        mapper = Code("""
                               function() {
                                    emit(this.value.neighborhood, {numOfPool:1});
                                }
                              """)

        reducer = Code("""
                                function(k, vs) {
                                    var total = 0;
                                    for (var i = 0; i < vs.length; i++) {
                                        total += vs[i].numOfPool;
                                    }
                                    return {numOfPool: total};
                                }
                                """)
        repo.dropPermanent("numOfPools")
        repo.createCollection('numOfPools')
        allPoolsInBoston.map_reduce(mapper, reducer, "billy108_zhouy13_jw0208.numOfPools")

        hubwaysInNeigh = repo['billy108_zhouy13_jw0208.hubwaysInNeigh']

        mapper = Code("""
                                       function() {
                                            emit(this.neighborhood, {numOfHubway:1});
                                        }
                                      """)

        reducer = Code("""
                                        function(k, vs) {
                                            var total = 0;
                                            for (var i = 0; i < vs.length; i++) {
                                                total += vs[i].numOfHubway;
                                            }
                                            return {numOfHubway: total};
                                        }
                                        """)

        repo.dropPermanent("numOfHubwayPerNeigh")
        repo.createCollection('numOfHubwayPerNeigh')
        hubwaysInNeigh.map_reduce(mapper, reducer, "billy108_zhouy13_jw0208.numOfHubwayPerNeigh")

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

        this_script = doc.agent('alg:billy108_zhouy13_jw0208#count',
                                {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})

        resource_allOpenSpacesInBoston = doc.entity('dat:billy108_zhouy13_jw0208#allOpenSpacesInBoston',
                                                    {'prov:label': 'All Open Spaces in Boston',
                                                     prov.model.PROV_TYPE: 'ont:DataResource',
                                                     'ont:Extension': 'json'})

        resource_allPoolsInBoston = doc.entity('dat:billy108_zhouy13_jw0208#allPoolsInBoston',
                                               {'prov:label': 'All public swimming pools in Boston',
                                                prov.model.PROV_TYPE: 'ont:DataResource',
                                                'ont:Extension': 'json'})

        resource_hubwaysInNeigh = doc.entity('dat:billy108_zhouy13_jw0208#hubwaysInNeigh',
                                             {'prov:label': 'All hubway stations in Boston',
                                              prov.model.PROV_TYPE: 'ont:DataResource',
                                              'ont:Extension': 'json'})
        # Activities
        doCount = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime,
                               {
                                   prov.model.PROV_LABEL: "Count the number of open spaces, pools and hubway stations for each neighborhood"})
        # Associations
        doc.wasAssociatedWith(doCount, this_script)

        # Record which activity used which resource
        doc.usage(doCount, resource_allOpenSpacesInBoston, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(doCount, resource_allPoolsInBoston, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})
        doc.usage(doCount, resource_hubwaysInNeigh, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        # Result dataset entities
        numOfOpenSpaces = doc.entity('dat:billy108_zhouy13_jw0208#numOfOpenSpaces',
                                     {prov.model.PROV_LABEL: 'number of open spaces per neighborhood',
                                      prov.model.PROV_TYPE: 'ont:DataSet'})
        numOfPools = doc.entity('dat:billy108_zhouy13_jw0208#numOfPools',
                                {prov.model.PROV_LABEL: 'number of public swimming pools per neighborhood',
                                 prov.model.PROV_TYPE: 'ont:DataSet'})
        numOfHubwayPerNeigh = doc.entity('dat:billy108_zhouy13_jw0208#numOfHubwayPerNeigh',
                                         {prov.model.PROV_LABEL: 'number of hubway stations per neighborhood',
                                          prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(numOfOpenSpaces, this_script)
        doc.wasGeneratedBy(numOfOpenSpaces, doCount, endTime)
        doc.wasDerivedFrom(numOfOpenSpaces, resource_allOpenSpacesInBoston, doCount, doCount, doCount)

        doc.wasAttributedTo(numOfPools, this_script)
        doc.wasGeneratedBy(numOfPools, doCount, endTime)
        doc.wasDerivedFrom(numOfPools, resource_allPoolsInBoston, doCount, doCount, doCount)

        doc.wasAttributedTo(numOfHubwayPerNeigh, this_script)
        doc.wasGeneratedBy(numOfHubwayPerNeigh, doCount, endTime)
        doc.wasDerivedFrom(numOfHubwayPerNeigh, resource_hubwaysInNeigh, doCount, doCount, doCount)

        repo.logout()

        return doc

# count.execute()
# doc = count.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
