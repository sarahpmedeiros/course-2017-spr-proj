# http://synthpopviewer.rti.org/obesity/index.html
# <iframe id="download_iframe" style="display:none;" src="http://synthpopviewer.rti.org/obesity/downloads/MA.zip"></iframe>
# 

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import shapefile #pip install pyshp

class obesitystats(dml.Algorithm):
    contributor = 'jguerero_mgarcia7'
    reads = []
    writes = ['jguerero_mgarcia7.obesitystats']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jguerero_mgarcia7', 'jguerero_mgarcia7')

        # Download shapefile
        opener=urllib.request.build_opener()
        opener.addheaders=[('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36')]
        urllib.request.install_opener(opener)

        shp_url = 'http://datamechanics.io/data/jguerero_mgarcia7/MA.dbf'
        shpfilepath, response = urllib.request.urlretrieve(shp_url)

        dbf_url = 'http://datamechanics.io/data/jguerero_mgarcia7/MA.shp'
        dbffilepath, response = urllib.request.urlretrieve(dbf_url)

        r = convert_shpfile_to_json(dbffilepath,shpfilepath)

        repo.dropCollection("obesitystats")
        repo.createCollection("obesitystats")
        repo['jguerero_mgarcia7.obesitystats'].insert_many(r)
        repo['jguerero_mgarcia7.obesitystats'].metadata({'complete':True})
        print(repo['jguerero_mgarcia7.obesitystats'].metadata())

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
        repo.authenticate('jguerero_mgarcia7', 'jguerero_mgarcia7')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('rti', 'http://synthpopviewer.rti.org/obesity/index.html')

        this_script = doc.agent('alg:jguerero_mgarcia7#obesitystats', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('rti:MA', {'prov:label':'MA Obesity Stats', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'shp'})
        get_obesitystats = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_obesitystats, this_script)
        doc.usage(get_obesitystats, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )


        obesitystats = doc.entity('dat:jguerero_mgarcia7#obesitystats', {prov.model.PROV_LABEL:'Massachusetts Obesity Stats', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(obesitystats, this_script)
        doc.wasGeneratedBy(obesitystats, get_obesitystats, endTime)
        doc.wasDerivedFrom(obesitystats, resource, get_obesitystats, get_obesitystats, get_obesitystats)

        repo.logout()
                  
        return doc

def convert_shpfile_to_json(shpfilepath,dbffilepath):
    ''' This is the code that was used to grab the zip file with the shapefiles in it, and then convert it to geojson '''

    '''
    import zipfile

    # Download zip file with shapefile in it
    opener=urllib.request.build_opener()
    opener.addheaders=[('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36')]
    urllib.request.install_opener(opener)

    url = 'http://synthpopviewer.rti.org/obesity/downloads/MA.zip'
    filepath, response = urllib.request.urlretrieve(url)

    # Extract files from the zipped file
    z = zipfile.ZipFile(filepath)
    z.extract('MA.shp')
    z.extract('MA.dbf')
    '''

    # Create a shpfile object
    myshp = open(shpfilepath,'rb')
    mydbf = open(dbffilepath,'rb')
    shpfile = shapefile.Reader(shp=myshp,dbf=mydbf)

    # Convert data to geojson to insert into the database
    fields = shpfile.fields[1:]
    field_names = [field[0] for field in fields]
    r = []
    for sr in shpfile.shapeRecords():
        atr = dict(zip(field_names, sr.record))
        geom = sr.shape.__geo_interface__
        r.append(dict(type="Feature", \
        geometry=geom, properties=atr)) 

    return r

## eof
