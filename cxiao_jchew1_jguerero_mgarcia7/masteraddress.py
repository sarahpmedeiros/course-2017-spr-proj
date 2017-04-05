# Downloads the master address list for Boston
import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from shapely.geometry import shape, Point

class masteraddress(dml.Algorithm):
    contributor = 'cxiao_jchew1_jguerero_mgarcia7'
    reads = ['cxiao_jchew1_jguerero_mgarcia7.neighborhoods']
    writes = ['cxiao_jchew1_jguerero_mgarcia7.masteraddress']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cxiao_jchew1_jguerero_mgarcia7', 'cxiao_jchew1_jguerero_mgarcia7')

        # Download json
        url = 'https://data.cityofboston.gov/resource/je5q-tbjf.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)

        # Select only residential addreses from the master list
        def select(R, s):
            return [d for d in R if s(d)]

        residential_codes = set(['A', 'CD', 'R1', 'R2', 'R3', 'R4', 'RC'])
        residential_add = select(r, lambda d: d.get('land_usage') in residential_codes)

        # For each food source, standardize the neighborhoods by looking at the latitude and longitude and finding out what neighborhood it fits into
        neighborhoods = repo['cxiao_jchew1_jguerero_mgarcia7.neighborhoods']

        # Create shapeobjects for each neighborhood
        neighborhood_shapes = {n['name']:shape(n['the_geom']) for n in neighborhoods.find({})}

        print(neighborhood_shapes.keys())

        # Find which neighborhood each point belongs to
        for row in residential_add:
            if row['longitude'] is None or row['latitude'] is None:
                continue

            row['longitude'] = float(row['longitude'])
            row['latitude'] = float(row['latitude'])
                
            loc = Point(row['longitude'],row['latitude'])
            for name,shp in neighborhood_shapes.items():
                if shp.contains(loc):
                    row['neighborhood'] = name
                    break


        repo.dropCollection("masteraddress")
        repo.createCollection("masteraddress")
        repo['cxiao_jchew1_jguerero_mgarcia7.masteraddress'].insert_many(residential_add)
        repo['cxiao_jchew1_jguerero_mgarcia7.masteraddress'].metadata({'complete':True})
        print(repo['cxiao_jchew1_jguerero_mgarcia7.masteraddress'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
    
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        '''
            Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
            '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('cxiao_jchew1_jguerero_mgarcia7', 'cxiao_jchew1_jguerero_mgarcia7')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:cxiao_jchew1_jguerero_mgarcia7#masteraddress', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:je5q-tbjf', {'prov:label':'Master Address List', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_masteraddress = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_masteraddress, this_script)
        doc.usage(get_masteraddress, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )


        masteraddress = doc.entity('dat:cxiao_jchew1_jguerero_mgarcia7#masteraddress', {prov.model.PROV_LABEL:'Master Address List', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(masteraddress, this_script)
        doc.wasGeneratedBy(masteraddress, get_masteraddress, endTime)
        doc.wasDerivedFrom(masteraddress, resource, get_masteraddress, get_masteraddress, get_masteraddress)

        repo.logout()
                  
        return doc



masteraddress.execute()
## eof
