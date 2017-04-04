import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import radians, sqrt, sin, cos, atan2


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

class transformation2(dml.Algorithm):
    contributor = 'bohan_xh1994'
    reads = ['bohan_xh1994.TRAFFIC_SIGNALS', 'bohan_xh1994.airbnb_rating', 'bohan_xh1994.Entertainment_Licenses']
    writes = ['bohan_xh1994.airbnb_rating_relation_with_traffic_signal_number_and_entertainment']




    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohan_xh1994', 'bohan_xh1994')  
        TRAFFIC_SIGNALS = repo.bohan_xh1994.TRAFFIC_SIGNALS.find()
        traffic = [c for c in TRAFFIC_SIGNALS]
        airbnb_rating = repo.bohan_xh1994.airbnb_rating.find()
        Entertainment_Licenses = repo.bohan_xh1994.Entertainment_Licenses.find()
        entertainment = [b for b in Entertainment_Licenses]


        # print(crime[0]['location'])
        # print(FoodEI[0])

        #Foodlocation_name = FoodEI.project(lambda t: (t['businessname'],t['location']))
        #crime_location = crime.project(lambda t: (t[-2]))
        #safety_level = []
        repo.dropCollection("airbnb_rating_relation_with_traffic_signal_number_and_entertainment")
        repo.createCollection("airbnb_rating_relation_with_traffic_signal_number_and_entertainment")
        #setdis = []

        for i in airbnb_rating:
            trafficsignum = 0
            numentertainment = 0
            #print(len(TRAFFIC_SIGNALS))
            for j in traffic:
                #print(j['geometry']['coordinates'][1])
                try:
                    distance = geodistance(i['latitude'],i['longitude'],j['geometry']['coordinates'][1],j['geometry']['coordinates'][0])
                except:
                    distance = 10.0
                #setdis.append(j)
                    #print('distance ', distance)
                    #print('icoorinates ', i['location']['coordinates'])
                # except:
                #     distance = 10.0
                #setdis.append(distance)
                if distance<=2.0:
                    trafficsignum+=1

            #print(len(Entertainment_Licenses))
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

            insertMaterial = {'longitude':i['longitude'], 'latitude':i['latitude'], 'review_scores_rating':i['review_scores_rating'], 'weekly_price':i['weekly_price'], 'name':i['name'], 'traffic signal num':trafficsignum, 'entertainment around number':numentertainment}
            #insertMaterial = {'Businessname':i['businessname'], 'location':None, 'crime incidents number within amile':crime_incident_within_amile}
  
            repo['bohan_xh1994.airbnb_rating_relation_with_traffic_signal_number_and_entertainment'].insert_one(insertMaterial)
            #setdis=[]

        #repo['bohan_xh1994.Restaurants_safety'].insert_many(safety_level)
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
        repo.authenticate('bohan_xh1994', 'bohan_xh1994')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('airbnbr','http://datamechanics.io/?prefix=bohan_xh1994/')
        

        this_script = doc.agent('alg:bohan_xh1994#transformation2', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        Res_airbnb_rate = doc.entity('dat:bohan_xh1994#airbnb_rating_relation_with_traffic_signal_number_and_entertainment', {prov.model.PROV_LABEL:'airbnb_rating_relation_with_traffic_signal_number_and_entertainment', prov.model.PROV_TYPE:'ont:DataSet'})
        get_airbnb_rate_relation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_airbnb_rate_relation, this_script)
        doc.used(get_airbnb_rate_relation, Res_airbnb_rate, startTime
                  , {prov.model.PROV_TYPE:'ont:Retrieval',
                   'ont:Computation':'?type=airbnb_rating+entertainment_license+&$select=entertainmentnum,airbnbname,airbnblocation,airbnbrating, weekly price'
                   }
                  )

        #lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(Res_airbnb_rate, this_script)
        doc.wasGeneratedBy(Res_airbnb_rate, get_airbnb_rate_relation, endTime)
        #doc.wasDerivedFrom(Rest_safe, resource, get_lost, get_lost, get_lost)

        repo.logout()
                  
        return doc

transformation2.execute()
doc = transformation2.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
