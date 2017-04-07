import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import radians, sqrt, sin, cos, atan2
from geopy.distance import vincenty


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

class transformation2_newwithMBTA(dml.Algorithm):
    contributor = 'bohan_nyx_xh1994_yiran123'
    reads = ['bohan_nyx_xh1994_yiran123.MBTA_Bus_stops', 'bohan_nyx_xh1994_yiran123.airbnb_rating', 'bohan_nyx_xh1994_yiran123.Entertainment_Licenses']
    writes = ['bohan_nyx_xh1994_yiran123.airbnb_rating_relation_with_MBTAstops_num_and_entertainment']




    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohan_nyx_xh1994_yiran123', 'bohan_nyx_xh1994_yiran123')  
        MBTA_Bus_stops = repo.bohan_nyx_xh1994_yiran123.MBTA_Bus_stops.find()
        stops = [c['features'] for c in MBTA_Bus_stops]
        stops = stops[0]
        #print(stops[1])
        airbnb_rating = repo.bohan_nyx_xh1994_yiran123.airbnb_rating.find()
        airbnbrate = [a for a in airbnb_rating]
        Entertainment_Licenses = repo.bohan_nyx_xh1994_yiran123.Entertainment_Licenses.find()
        entertainment = [b for b in Entertainment_Licenses]


        # print(crime[0]['location'])
        # print(FoodEI[0])

        #Foodlocation_name = FoodEI.project(lambda t: (t['businessname'],t['location']))
        #crime_location = crime.project(lambda t: (t[-2]))
        #safety_level = []
        repo.dropCollection("airbnb_rating_relation_with_MBTAstops_num_and_entertainment")
        repo.createCollection("airbnb_rating_relation_with_MBTAstops_num_and_entertainment")
        #setdis = []
        #print(len(entertainment))
        #print(stops)

        for i in airbnbrate:
            #print(i)
            stopsnum = 0
            numentertainment = 0
            #print(i['weekly_price'])
            for j in stops:
                #print(j[1])
                #print(20220020202020202020)
                try:
                    #distance = geodistance(i['latitude'],i['longitude'],j[0]['geometry']['coordinates'][1],j[0]['geometry']['coordinates'][0])
                    distance = vincenty((i['latitude'],i['longitude']), (j['geometry']['coordinates'][1], j['geometry']['coordinates'][0])).miles

                    #print(distance)
                except:
                    distance = 30.0
                #setdis.append(j)
                    #print('distance ', distance)
                    #print('icoorinates ', i['location']['coordinates'])
                # except:
                #     distance = 10.0
                #setdis.append(distance)
                if distance<=2.0:
                    stopsnum+=1

            #print(20202020202020202020202202020)
            for k in entertainment:
                #print('yyoyoyo', k['location'][0])
                #print(k['location'][1:12])

                try:
                    klat = float(k['location'][1:12])
                    klong = float(k['location'][15:28])
                except:
                    klat = 42
                    klong = -71
                distance = geodistance(i['latitude'],i['longitude'],klat,klong)
                if distance<=2.0:

                    numentertainment+=1

            insertMaterial = {'longitude':i['longitude'], 'latitude':i['latitude'], 'review_scores_rating':i['review_scores_rating'], 'weekly_price':i['weekly_price'], 'name':i['name'], 'MBTA stops num within 2km':stopsnum, 'entertainment around number':numentertainment}
            #insertMaterial = {'Businessname':i['businessname'], 'location':None, 'crime incidents number within amile':crime_incident_within_amile}
            
            repo['bohan_nyx_xh1994_yiran123.airbnb_rating_relation_with_MBTAstops_num_and_entertainment'].insert_one(insertMaterial)
            #setdis=[]
        #print(stops[1])

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
        doc.add_namespace('MBTAbusr','http://datamechanics.io/?prefix=wuhaoyu_yiran123/')
        

        this_script = doc.agent('alg:bohan_nyx_xh1994_yiran123#transformation2_newwithMBTA', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_mbta_stops = doc.entity('dat:bohan_nyx_xh1994_yiran123#MBTA_Bus_stops', {prov.model.PROV_LABEL:'MBTA Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_airbnb_rate = doc.entity('dat:bohan_nyx_xh1994_yiran123#airbnb_rating', {prov.model.PROV_LABEL:'Airbnb Rating', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_entertainment_licenses = doc.entity('dat:bohan_nyx_xh1994_yiran123#Entertainment_Licenses', {prov.model.PROV_LABEL:'Entertainment Licenses', prov.model.PROV_TYPE:'ont:DataSet'})

        get_airbnb_rate_relation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_airbnb_rate_relation, this_script)

        doc.usage(get_airbnb_rate_relation, resource_mbta_stops, startTime, None, 
                    {prov.model.PROV_TYPE:'ont:Computation',})
        doc.usage(get_airbnb_rate_relation, resource_airbnb_rate, startTime, None, 
                    {prov.model.PROV_TYPE:'ont:Computation',})
        doc.usage(get_airbnb_rate_relation, resource_entertainment_licenses, startTime, None, 
                    {prov.model.PROV_TYPE:'ont:Computation',})

        #airbnbrating = doc.entity('dat:bohan_1994#lost', {prov.model.PROV_LABEL:'airbnb_rating_relation_with_MBTAstops_num_and_entertainment', prov.model.PROV_TYPE:'ont:DataSet'})
        airbnb_rating_relation_with_MBTAstops_num_and_entertainment = doc.entity('dat:bohan_nyx_xh1994_yiran123#airbnb_rating_relation_with_MBTAstops_num_and_entertainment',
                                {prov.model.PROV_LABEL:'Airbnb rating relation with MBTA stops num and entertainment',
                                 prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(airbnb_rating_relation_with_MBTAstops_num_and_entertainment, this_script)
        
        doc.wasGeneratedBy(airbnb_rating_relation_with_MBTAstops_num_and_entertainment, get_airbnb_rate_relation, endTime)
        
        doc.wasDerivedFrom(airbnb_rating_relation_with_MBTAstops_num_and_entertainment, resource_mbta_stops, get_airbnb_rate_relation, get_airbnb_rate_relation, get_airbnb_rate_relation)
        doc.wasDerivedFrom(airbnb_rating_relation_with_MBTAstops_num_and_entertainment, resource_airbnb_rate, get_airbnb_rate_relation, get_airbnb_rate_relation, get_airbnb_rate_relation)
        doc.wasDerivedFrom(airbnb_rating_relation_with_MBTAstops_num_and_entertainment, resource_entertainment_licenses, get_airbnb_rate_relation, get_airbnb_rate_relation, get_airbnb_rate_relation)

        repo.logout()
                  
        return doc

transformation2_newwithMBTA.execute()
doc = transformation2_newwithMBTA.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
