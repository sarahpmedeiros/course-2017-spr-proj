import urllib.request
import json, googlemaps
import dml, prov.model, uuid
import datetime, sys 

TRIAL_LIMIT = 5000

class rent(dml.Algorithm):
    contributor = 'minteng_tigerlei_zhidou'
    reads = []
    writes = ['minteng_tigerlei_zhidou.rent']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets.'''
        startTime = datetime.datetime.now()
        print("rent start!")

        if trial:
            print(" Now you are running trial mode")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('minteng_tigerlei_zhidou', 'minteng_tigerlei_zhidou')
        gmaps = googlemaps.Client(key = dml.auth['services']['googlemapsportal']['key'])

        def getPostalCode(city, key):
            # find the everage location coordinate
            city = city.replace(' ','+')
            loc = gmaps.geocode(city + ', Boston, MA')[0]
            loc = loc['geometry']['location']
            crime_info = gmaps.reverse_geocode((loc['lat'], loc['lng']))

            for i in crime_info[0]['address_components']:
                if 'postal_code' in i['types']: break
            return i['long_name']

        #new dataset 1: Rent Data
        url='http://datamechanics.io/data/minteng_zhidou/rent.txt'
        if trial:
            rents = urllib.request.urlopen(url).readlines(TRIAL_LIMIT)
        else:
            rents = urllib.request.urlopen(url).readlines()
        rent=[]
        for r in rents[1:]:
            s=str(r).split(',')
            temp={}
            temp['area']=s[1]
            temp['avg_rent']=int(s[2])
            temp['postal_code'] = getPostalCode(s[1], dml.auth['services']['googlemapsportal']['key'])
            
            rent.append(temp)

        repo.dropCollection("rent")
        repo.createCollection("rent")
        repo['minteng_tigerlei_zhidou.rent'].insert_many(rent)

        print("End!")
        
        
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
        repo.authenticate('minteng_tigerlei_zhidou', 'minteng_tigerlei_zhidou')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.

        
        this_script = doc.agent('alg:minteng_tigerlei_zhidou#rent', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        ### data 1: rent
        rent_resource = doc.entity('dat:minteng_tigerlei_zhidou#rent', {'prov:label':'City Average Rent', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txt'})
        
        get_rent = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_rent, this_script)
        doc.usage(get_rent, rent_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})


        rent = doc.entity('dat:minteng_tigerlei_zhidou#rent', {prov.model.PROV_LABEL:'City Average Rents', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(rent, this_script)
        doc.wasGeneratedBy(rent, get_rent, endTime)
        doc.wasDerivedFrom(rent, rent_resource, get_rent, get_rent, get_rent)

        
        repo.logout()
                  
        return doc

# if 'trial' in sys.argv:
#     rent.execute(True)
# else:
#     rent.execute()

# doc = rent.provenance()
# # print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

