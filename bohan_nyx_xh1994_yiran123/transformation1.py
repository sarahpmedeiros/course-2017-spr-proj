import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

from math import radians, sqrt, sin, cos, atan2


#helper function
def geodistance(lat1, lon1, lat2, lon2):
        lat1 = radians(lat1)
        lon1 = radians(lon1)
        lat2 = radians(lat2)
        lon2 = radians(lon2)

        dlon = lon1 - lon2

        EARTH_R = 6372.8

        y = sqrt(
            (cos(lat2) * sin(dlon)) ** 2
            + (cos(lat1) * sin(lat2) - sin(lat1) * cos(lat2) * cos(dlon)) ** 2
            )
        x = sin(lat1) * sin(lat2) + cos(lat1) * cos(lat2) * cos(dlon)
        c = atan2(y, x)
        return EARTH_R * c

class transformation1(dml.Algorithm):
    contributor = 'bohan_nyx_xh1994_yiran123'
    reads = ['bohan_nyx_xh1994_yiran123.Active_Food_Establishment_Licenses', 'bohan_nyx_xh1994_yiran123.crime_boston']
    writes = ['bohan_nyx_xh1994_yiran123.Restaurants_safety']




    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohan_nyx_xh1994_yiran123', 'bohan_nyx_xh1994_yiran123')  
        AFoodL = repo.bohan_nyx_xh1994_yiran123.Active_Food_Establishment_Licenses.find()
        crime = repo.bohan_nyx_xh1994_yiran123.crime_boston.find()
        crimes = [c for c in crime]
        # print(crime[0]['location'])
        # print(FoodEI[0])

        #Foodlocation_name = FoodEI.project(lambda t: (t['businessname'],t['location']))
        #crime_location = crime.project(lambda t: (t[-2]))
        #safety_level = []
        repo.dropCollection("Restaurants_safety")
        repo.createCollection("Restaurants_safety")
        setdis = []
        #counter = 0
        #jcounter=0
        for i in AFoodL:
            #jcounter+=1
            crime_incident_within_akm = 0
            #print(i['location'])
            #print('i',jcounter)
            #print(len(crimes))
            for j in crimes:
                #counter+=1
                #print('j',counter)
                #print(j['location'])
                #print(i)
                #try:
                    #print("yoyoyoy"+double(vincenty(i['location'], j['location']['coordinates'])))
                    # foodloc = (i['location']['coordinates'][0],i['location']['coordinates'][1])
                    # crimeloc = (j['location']['coordinates'][0], j['location']['coordinates'][1])
                    #distance = vincenty(foodloc,crimeloc)
                    #distance = (vincenty((i['location'][0],i['location'][1]), (j['location']['coordinates'][0], j['location']['coordinates'][1]), mile = True))
                distance = geodistance(i['location']['coordinates'][0],i['location']['coordinates'][1],j['location']['coordinates'][0],j['location']['coordinates'][1])
                #print(distance)
                #setdis.append(j)
                    #print('distance ', distance)
                    #print('icoorinates ', i['location']['coordinates'])
                # except:
                #     distance = 10.0
                setdis.append(distance)
                if distance<=1:
                    crime_incident_within_akm+=1
            
            insertMaterial = {'Businessname':i['businessname'], 'location':i['location'], 'crime incidents number within akm':crime_incident_within_akm}
            #insertMaterial = {'Businessname':i['businessname'], 'location':None, 'crime incidents number within amile':crime_incident_within_amile}
  
            repo['bohan_nyx_xh1994_yiran123.Restaurants_safety'].insert_one(insertMaterial)
            setdis=[]

        #repo['bohan_nyx_xh1994_yiran123.Restaurants_safety'].insert_many(safety_level)
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
        repo.authenticate('bohan_nyx_xh1994_yiran123', 'bohan_nyx_xh1994_yiran123')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('airbnbr','http://datamechanics.io/?prefix=bohan_xh1994/')

        this_script = doc.agent('alg:bohan_nyx_xh1994_yiran123#transformation1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        Rest_safe = doc.entity('dat:bohan_nyx_xh1994_yiran123#Restaurants_safety', {prov.model.PROV_LABEL:'safety_level of restaurant', prov.model.PROV_TYPE:'ont:DataSet'})
        get_safe = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_safe, this_script)
        doc.usage(get_safe, Rest_safe, startTime
                   , {prov.model.PROV_TYPE:'ont:Retrieval',
                   'ont:Computation':'?type=crime_incident_within_akm&$select=type,location,bussinessname,crime_incident_within_akm'
                  }
                  )
        rest_s = doc.entity('dat: bohan_xh1994#transformation1',
                            {prov.model.PROV_LABEL:'Safty level of restaurant',
                             prov.model.PROV_TYPE: 'ont:DataSet'})
        #lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Rest_safe, this_script)
        doc.wasGeneratedBy(Rest_safe, get_safe, endTime)
        doc.wasDerivedFrom(rest_s, Rest_safe, get_safe, get_safe, get_safe)

        repo.logout()
                  
        return doc

transformation1.execute()
doc = transformation1.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
