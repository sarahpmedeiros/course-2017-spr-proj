import urllib.request
import json
import dml
import prov.model
import datetime 
import uuid
import sys

TRIAL_LIMIT = 500

class location(dml.Algorithm):
    contributor = 'minteng_tigerlei_zhidou'
    reads = ['minteng_tigerlei_zhidou.crime']
    writes = ['minteng_tigerlei_zhidou.location']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets.'''
        startTime = datetime.datetime.now()

        print("location start!")

        if trial:
            print(" Now you are running trial mode")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('minteng_tigerlei_zhidou', 'minteng_tigerlei_zhidou')
        
        limit = 50000
        #new dataset 2: location data with key: (location, tag), tag: food, transport and crime
        ####food data
        if trial: 
            limit = TRIAL_LIMIT

        url='https://data.cityofboston.gov/resource/fdxy-gydq.json?$limit='+ str(limit)
        response = urllib.request.urlopen(url).read().decode("utf-8")
        food=json.loads(response)
        food_info=[]
        for f in food:
            try:
                temp={}
                temp['address']=f['address']
                temp['businessname']=f['businessname']
                temp['city']=f['city']
                temp['location']=f['location']['coordinates'][::-1]
                temp['type'] = 'food'
                temp['zip']=f['zip']
                food_info.append(temp)
            except KeyError:
                continue
        
        ###transport data
        url='http://datamechanics.io/data/minteng_zhidou/stops.txt'
        if trial:
            stops = urllib.request.urlopen(url).readlines(TRIAL_LIMIT)
        else:
            stops = urllib.request.urlopen(url).readlines()
        transport=[]
        for stop in stops[1:]:
            s=str(stop).split(',')
            temp={}
            temp['station']=s[2]
            temp['type'] = 'transport'
            try:
                temp['location']=[float(s[4]),float(s[5])]
            except ValueError:
                continue
            transport.append(temp)
        
        ###crime data
        crime=[]  

        for item in repo['minteng_tigerlei_zhidou.crime'].find({'year':{'$eq':2016}}):
            temp={}
            temp['type'] = item['type']
            temp['location'] = item['location']
            temp['year'] = item['year']
            crime.append(temp)

        temp=food_info+transport+crime
        union = [t for t in temp if 0 not in t['location']]
        repo.dropCollection("location")
        repo.createCollection("location")
        repo['minteng_tigerlei_zhidou.location'].insert_many(union)
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
        doc.add_namespace('bd', 'https://data.cityofboston.gov/resource/')

        
        this_script = doc.agent('alg:minteng_tigerlei_zhidou#location', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        ### data 2: transport
        transport_resource = doc.entity('dat:minteng_tigerlei_zhidou#stop', {'prov:label':'MBTA Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txt'})
        get_transport = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_transport, this_script)
        doc.usage(get_transport, transport_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        ### data 3: food
        food_resource = doc.entity('bd:gb6y-34cq', {'prov:label':'Active Food Establishment Licenses', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_food = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_food, this_script)
        doc.usage(get_food, food_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?$select=address,city,businessname,location,zip'})
        ### data 4: safety/crime
        crime_resource = doc.entity('dat:minteng_tigerlei_zhidou#crime', {'prov:label':'crime incidents in boston', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(get_crime, crime_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
       

        ### new data 2: location
        location = doc.entity('dat:minteng_tigerlei_zhidou#location', {prov.model.PROV_LABEL:'Locations with tag and infomation', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(location, this_script)
        doc.wasGeneratedBy(location, get_transport, endTime)
        doc.wasDerivedFrom(location, transport_resource, get_transport, get_transport, get_transport)

        doc.wasGeneratedBy(location, get_food, endTime)
        doc.wasDerivedFrom(location, food_resource, get_food, get_food, get_food)

        doc.wasGeneratedBy(location, get_crime, endTime)
        doc.wasDerivedFrom(location, crime_resource, get_crime, get_crime, get_crime)


        repo.logout()
                  
        return doc

# if 'trial' in sys.argv:
#     location.execute(True)
# else:
#     location.execute()
# doc = location.provenance()
# #print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
