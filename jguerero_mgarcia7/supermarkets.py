import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class supermarkets(dml.Algorithm):
    contributor = 'jguerero_mgarcia7'
    reads = []
    writes = ['jguerero_mgarcia7.supermarkets']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jguerero_mgarcia7', 'jguerero_mgarcia7')

        url = 'http://datamechanics.io/data/jguerero_mgarcia7/supermarkets.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)


        repo.dropCollection("supermarkets")
        repo.createCollection("supermarkets")
        repo['jguerero_mgarcia7.supermarkets'].insert_many(r)
        repo['jguerero_mgarcia7.supermarkets'].metadata({'complete':True})
        print(repo['jguerero_mgarcia7.supermarkets'].metadata())

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
        doc.add_namespace('bra', 'http://www.bostonplans.org/getattachment/') # Boston Redevelopment Authority

        this_script = doc.agent('alg:jguerero_mgarcia7#supermarkets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bra:5f0e8d2d-2fb3-4b81-a08b-3cc9b918cd0e', {'prov:label':'Grocery Stores in Boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'pdf'})
        get_supermarkets = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_supermarkets, this_script)
        doc.usage(get_supermarkets, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'}
                  )

        supermarkets = doc.entity('dat:jguerero_mgarcia7#supermarkets', {prov.model.PROV_LABEL:'List of Supermarkets over 7000 sq ft in Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(supermarkets, this_script)
        doc.wasGeneratedBy(supermarkets, get_supermarkets, endTime)
        doc.wasDerivedFrom(supermarkets, resource, get_supermarkets, get_supermarkets, get_supermarkets)


        repo.logout()
                  
        return doc

def grouper(iterable, n, fillvalue=None):
    "Collect data into fixed-length chunks or blocks"
    from itertools import zip_longest

    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx"
    args = [iter(iterable)] * n
    return zip_longest(*args, fillvalue=fillvalue)

def scrape_pdf_file():
    ''' This function scrapes the pdf file with a table of supermarkets in it, and converts it into json '''
    import PyPDF2 # pip install PyPDF2

    # Download pdf file
    opener=urllib.request.build_opener()
    opener.addheaders=[('User-Agent','Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1941.0 Safari/537.36')]
    urllib.request.install_opener(opener)

    url = 'http://www.bostonplans.org/getattachment/5f0e8d2d-2fb3-4b81-a08b-3cc9b918cd0e/'
    filepath, response = urllib.request.urlretrieve(url)


    # Parse table text into a json object
    pdfFileObj = open(filepath, 'rb')
    pdfReader = PyPDF2.PdfFileReader(pdfFileObj)
    pageObj = pdfReader.getPage(1)
    table_as_string = pageObj.extractText().split('\n')


    rows = grouper(table_as_string[52:-6],6) # Groups all of the items into tuples of 6
    r = []
    for row in rows:
        info = {'neighborhood':row[0], 'store':row[1], 'address':row[2], 'year':row[3], 'total sq ft':row[4], 'type':row[5]}
        r.append(info)

'''
#supermarkets.execute()
doc = supermarkets.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
