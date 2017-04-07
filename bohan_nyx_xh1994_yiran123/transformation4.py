import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
from math import radians, sqrt, sin, cos, atan2
from random import shuffle
from math import sqrt


def permute(x):
    shuffled = [xi for xi in x]
    shuffle(shuffled)
    return shuffled

def avg(x): # Average
    return sum(x)/len(x)

def stddev(x): # Standard deviation.
    m = avg(x)
    return sqrt(sum([(xi-m)**2 for xi in x])/len(x))

def cov(x, y): # Covariance.
    return sum([(xi-avg(x))*(yi-avg(y)) for (xi,yi) in zip(x,y)])/len(x)

def corr(x, y): # Correlation coefficient.
    if stddev(x)*stddev(y) != 0:
        return cov(x, y)/(stddev(x)*stddev(y))

def p(x, y):
    c0 = corr(x, y)
    corrs = []
    for k in range(0, 2000):
        y_permuted = permute(y)
        corrs.append(corr(x, y_permuted))
    return len([c for c in corrs if abs(c) > c0])/len(corrs)




class transformation4(dml.Algorithm):
    contributor = 'bohan_nyx_xh1994_yiran123'
    reads = ['bohan_nyx_xh1994_yiran123.restaurant_cleanness_level','bohan_nyx_xh1994_yiran123.Restaurants_safety', 'bohan_nyx_xh1994_yiran123.airbnb_rating_relation_with_MBTAstops_num_and_entertainment']
    writes = ['bohan_nyx_xh1994_yiran123.restaurant_correlation_distance_analysis_filtered', 'bohan_nyx_xh1994_yiran123.newairbnb_eliminated_version']




    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohan_nyx_xh1994_yiran123', 'bohan_nyx_xh1994_yiran123')  
        rests_safe = repo.bohan_nyx_xh1994_yiran123.Restaurants_safety.find()
        rests_clean = repo.bohan_nyx_xh1994_yiran123.restaurant_cleanness_level.find()
        similarity = False
        crimenum = [c['crime incidents number within akm'] for c in rests_safe]
        #print(crimenum)
        passrate = [b['cleanness level'] for b in rests_clean]
        crimenum = list(map(int, crimenum))
        passrate = list(map(float, passrate))
        rests_safe = repo.bohan_nyx_xh1994_yiran123.Restaurants_safety.find()
        rests_clean = repo.bohan_nyx_xh1994_yiran123.restaurant_cleanness_level.find()
        clean = [a for a in rests_clean]
        safe = [d for d in rests_safe]
        crr_crime_clean = corr(crimenum,passrate)


        repo.dropCollection("restaurant_correlation_distance_analysis_filtered")
        repo.createCollection("restaurant_correlation_distance_analysis_filtered")
        if(crr_crime_clean >= 0.5):
            similarity = True
            #this is a moderate or a strong positive relation, we only keep one field, lets keep cleanness
            repo['bohan_nyx_xh1994_yiran123.restaurant_correlation_distance_analysis_filtered'].insert(safe)

        else:
            #keep both since they are not too similar which can give us good sense of rating
            # print(2020202020)
            #print(safe)
            for i in safe:
                #print(2020202020)
                #print(i['location']['coordinates'][0])
                if(i['location']['coordinates'][0] != i['location']['coordinates'][1]):
                    #print(i['location'])
                    for j in clean:
                        #print(j['Businessname'])
                        #print(i['location'])
                        if(str(i['location']['coordinates']) == str(j['location']['coordinates'])):
                            #print(j['location'])
                            insertMaterial = {'Businessname':i['Businessname'], 'location':i['location'], 'crimes within one km':i['crime incidents number within akm'], 'clean level':j['cleanness level']}
                            repo['bohan_nyx_xh1994_yiran123.restaurant_correlation_distance_analysis_filtered'].insert(insertMaterial)


        repo.dropCollection('newairbnb_eliminated_version')
        repo.createCollection('newairbnb_eliminated_version')
        airbnb_score_entertainment_MBTA = repo.bohan_nyx_xh1994_yiran123.airbnb_rating_relation_with_MBTAstops_num_and_entertainment.find()
        airbnb_score_enter_MB_list = [h for h in airbnb_score_entertainment_MBTA]
        MBTAnum = [e['MBTA stops num within 2km'] for e in airbnb_score_enter_MB_list]
        #rating = [f['review_scores_rating'] for f in airbnb_score_enter_MB_list]
        entertainnum = [g['entertainment around number'] for g in airbnb_score_enter_MB_list]
        MBTAnum = list(map(int, MBTAnum))
        #rating = list(map(int, rating))
        entertainnum = list(map(int, entertainnum))
        crr_MBTA_entertain = corr(MBTAnum,entertainnum)
        #crr_MBTA_rating = corr(MBTAnum,rating)
        #crr_rating_entertain = corr(rating,entertainnum)
        keepMBTA = False
        keepentertain = True
        #print(crr_MBTA_entertain)
        if(crr_MBTA_entertain>=0.2):
            keepMBTA = False
        #if(crr_MBTA_rating>=0.3):
           #keepMBTA = False
        #if(crr_rating_entertain>=0.3):
            #keepentertain = False

        '''if(keepentertain == False and keepMBTA == False):
            for i in airbnb_score_enter_MB_list:
                inserM = {'airbnb name': i['name'], 'longitude': i['longitude'], 'latitude': i['latitude'], 'rating': i['review_scores_rating']}
                repo['bohan_nyx_xh1994_yiran123.newairbnb_eliminated_version'].insert(inserM)'''

        '''elif(keepentertain == False and keepMBTA == True):
            for i in airbnb_score_enter_MB_list:
                inserM = {'airbnb name': i['name'], 'longitude': i['longitude'], 'latitude': i['latitude'], 'rating': i['review_scores_rating'], 'entertainment around number': i['entertainment around number']}
                repo['bohan_nyx_xh1994_yiran123.newairbnb_eliminated_version'].insert(inserM)'''
        if(keepentertain == True and keepMBTA == False):
            #print(3030303030)
            for i in airbnb_score_enter_MB_list:
                inserM = {'airbnb name': i['name'], 'longitude': i['longitude'], 'latitude': i['latitude'], 'rating': i['review_scores_rating'], 'MBTA stops num within 2km': i['MBTA stops num within 2km']}
                repo['bohan_nyx_xh1994_yiran123.newairbnb_eliminated_version'].insert(inserM)

        else:
            repo['bohan_nyx_xh1994_yiran123.newairbnb_eliminated_version'].insert_many(airbnb_score_entertainment_MBTA)
           # print(909090990909090909009)



        repo.logout()
        #print(crr_crime_clean)

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
        #doc.add_namespace('airbnbr','http://datamechanics.io/?prefix=bohan_xh1994/')
        

        this_script = doc.agent('alg:bohan_xh1994#transformation4', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        resource_safe = doc.entity('dat:bohan_xh1994#Restaurants_safety', {prov.model.PROV_LABEL:'Restaurant Safety', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_rest_clean = doc.entity('dat:bohan_nyx_xh1994_yiran123#restaurant_cleanness_level', {prov.model.PROV_LABEL:'Restaurant cleanness', prov.model.PROV_TYPE:'ont:DataSet'})
        resource_airbnb_mbta = doc.entity('dat:bohan_nyx_xh1994_yiran123#airbnb_rating_relation_with_MBTAstops_num_and_entertainment', {prov.model.PROV_LABEL:'Airbnb with MBTA', prov.model.PROV_TYPE:'ont:DataSet'})
        
        get_rest_correlation = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_newairbnb_eliminated_version = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_rest_correlation, this_script)
        doc.wasAssociatedWith(get_newairbnb_eliminated_version, this_script)

        doc.usage(get_rest_correlation, resource_safe, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Computation'}
                 # , {prov.model.PROV_TYPE:'ont:Retrieval',
                  # 'ont:Computation':'?type=airbnb_rating+entertainment_license+&$select=entertainmentnum,airbnbname,airbnblocation,airbnbrating, weekly price'
                  # }
                  )
        doc.usage(get_rest_correlation, resource_rest_clean, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Computation'})
        doc.usage(get_rest_correlation, resource_airbnb_mbta, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Computation'})

        restaurant_correlation_distance_analysis_filtered = doc.entity('dat:bohan_nyx_xh1994_yiran123#restaurant_correlation_distance_analysis_filtered',
                                                                prov.model.PROV_LABEL:'Restaurant Correlation Distance Analysis',
                                                                prov.model.PROV_TYPE: 'ont:DataSet')

        newairbnb_eliminated_version = doc.entity('dat:bohan_nyx_xh1994_yiran123#newairbnb_eliminated_version',
                                                                prov.model.PROV_LABEL:'New Airbnb Eliminated Version',
                                                                prov.model.PROV_TYPE: 'ont:DataSet')


        #lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(restaurant_correlation_distance_analysis_filtered, this_script)
        doc.wasAttributedTo(newairbnb_eliminated_version, this_script)

        doc.wasGeneratedBy(restaurant_correlation_distance_analysis_filtered, get_rest_correlation, endTime)
        doc.wasGeneratedBy(newairbnb_eliminated_version, get_newairbnb_eliminated_version, endTime)

        doc.wasDerivedFrom(restaurant_correlation_distance_analysis_filtered, resource_rest_clean, get_rest_correlation, get_rest_correlation, get_rest_correlation)
        doc.wasDerivedFrom(restaurant_correlation_distance_analysis_filtered, resource_safe, get_rest_correlation, get_rest_correlation, get_rest_correlation)
        doc.wasDerivedFrom(newairbnb_eliminated_version, resource_airbnb_mbta, get_newairbnb_eliminated_version, get_newairbnb_eliminated_version, get_newairbnb_eliminated_version)
        
        #doc.wasDerivedFrom(Rest_safe, resource, get_lost, get_lost, get_lost)

        repo.logout()
                  
        return doc

transformation4.execute()
doc = transformation4.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
