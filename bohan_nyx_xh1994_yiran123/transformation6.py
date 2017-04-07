import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

from math import radians, sqrt, sin, cos, atan2

from geopy.distance import vincenty


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

class transformation6(dml.Algorithm):
    contributor = 'bohan_nyx_xh1994_yiran123'
    reads = ['bohan_nyx_xh1994_yiran123.restaurant_score_system', 'bohan_nyx_xh1994_yiran123.airbnb_rating']
    writes = ['bohan_nyx_xh1994_yiran123.Airbnb_surrounding_restauranScoreAVG']




    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohan_nyx_xh1994_yiran123', 'bohan_nyx_xh1994_yiran123')  
        rest_score = repo.bohan_nyx_xh1994_yiran123.restaurant_score_system.find()
        airbnb = repo.bohan_nyx_xh1994_yiran123.airbnb_rating.find()
        airbnbrate = [a for a in airbnb]
        restScore = [s for s in rest_score]
       

    
        repo.dropCollection("Airbnb_surrounding_restauranScoreAVG")
        repo.createCollection("Airbnb_surrounding_restauranScoreAVG")
        #counter = 0
        #jcounter=0
        for i in airbnbrate:
            #print(i)
            #jcounter+=1
            restaurant_num = 0
            TotalScore = 0

            for j in restScore:
                #print(i['latitude'])
                #print(j['location']['coordinates'][0])
                
                try:
                    #distance = geodistance(i['latitude'],i['longitude'],j[0]['geometry']['coordinates'][1],j[0]['geometry']['coordinates'][0])
                    distance = geodistance(i['latitude'],i['longitude'], j['location']['coordinates'][1],j['location']['coordinates'][0])
                    #print(distance)
                except:
                    distance = 30.0
                #print(distance)
                
                if distance<=1:
                    restaurant_num +=1
                    TotalScore += float(j['overall score'])
            
            if restaurant_num == 0:
                restaurant_num =999999999
            #print(TotalScore)
            #print("restnum")
            #print(restaurant_num)

            AvgScore = TotalScore/restaurant_num
            insertMaterial = {'longitude':i['longitude'], 'latitude':i['latitude'], 'Surrounding Restaurants num':restaurant_num, 'Avg Restaurants Score':AvgScore}


            #insertMaterial = {'Businessname':i['businessname'], 'location':None, 'crime incidents number within amile':crime_incident_within_amile}
  
            repo['bohan_nyx_xh1994_yiran123.Airbnb_surrounding_restauranScoreAVG'].insert_one(insertMaterial)

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

        this_script = doc.agent('alg:bohan_nyx_xh1994_yiran123#transformation6', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_restaurant_ss = doc.entity('dat:bohan_nyx_xh1994_yiran123#restaurant_score_system', {prov.model.PROV_LABEL:'Restaurant Score System', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_airbnb_rating = doc.entity('dat:bohan_nyx_xh1994_yiran123#airbnb_rating', {prov.model.PROV_LABEL:'Airbnb Rating', prov.model.PROV_TYPE:'ont:DataSet'})


        get_airbnb_surr_restaurant_score_avg = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_airbnb_surr_restaurant_score_avg, this_script)

        doc.usage(get_airbnb_surr_restaurant_score_avg, resource_restaurant_ss, startTime, None, 
                   {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(get_airbnb_surr_restaurant_score_avg, resource_airbnb_rating, startTime, None, 
                   {prov.model.PROV_TYPE:'ont:Computation'})

        airbnb_surr_restaurant_score_avg = doc.entity('dat:bohan_nyx_xh1994_yiran123#Airbnb_surrounding_restauranScoreAVG',
                                                {prov.model.PROV_LABEL:'Airbnb Surrounding Restaurant Score Average'})

        #lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(airbnb_surr_restaurant_score_avg, this_script)

        doc.wasGeneratedBy(airbnb_surr_restaurant_score_avg, get_airbnb_surr_restaurant_score_avg, endTime)

        doc.wasDerivedFrom(airbnb_surr_restaurant_score_avg, resource_restaurant_ss, get_airbnb_surr_restaurant_score_avg, get_airbnb_surr_restaurant_score_avg, get_airbnb_surr_restaurant_score_avg)
        doc.wasDerivedFrom(airbnb_surr_restaurant_score_avg, resource_airbnb_rating, get_airbnb_surr_restaurant_score_avg, get_airbnb_surr_restaurant_score_avg, get_airbnb_surr_restaurant_score_avg)

        repo.logout()
                  
        return doc

transformation6.execute()
doc = transformation6.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
