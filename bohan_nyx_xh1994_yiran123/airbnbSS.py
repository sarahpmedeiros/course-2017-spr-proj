import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class airbnbSS(dml.Algorithm):
    contributor = 'bohan_nyx_xh1994_yiran123'
    reads = ['bohan_nyx_xh1994_yiran123.airbnb_rating', 'bohan_nyx_xh1994_yiran123.Entertainment_Licenses', 'bohan_nyx_xh1994_yiran123.Airbnb_surrounding_restaurantScoreAVG']
    writes = ['bohan_nyx_xh1994_yiran123.airbnb_score_system']

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohan_nyx_xh1994_yiran123','bohan_nyx_xh1994_yiran123')

        airbnb_rating = repo.bohan_nyx_xh1994_yiran123.airbnb_rating.find()
        airbnb_MBTA_entertain = repo.bohan_nyx_xh1994_yiran123.airbnb_rating_relation_with_MBTAstops_num_and_entertainment.find()
        
        rest = repo.bohan_nyx_xh1994_yiran123.Airbnb_surrounding_restaurantScoreAVG.find()


        repo.dropCollection("airbnb_score_system")
        repo.createCollection("airbnb_score_system")
    
        total_review = []
        overall_rating = []
        clean_level = []
        accuracy_level = []
        communication = []
        location = []
        coor=[]
        longitude=[]

        for i in airbnb_rating:
            i_review = str(i['number_of_reviews'])
            i_orating = str(i['review_scores_rating'])
            i_clean = str(i['review_scores_cleanliness'])
            i_accu = str(i['review_scores_accuracy'])
            i_com = str(i['review_scores_communication'])
            i_location = str(i['review_scores_location'])
            i_longitude=float(i['longitude'])
            i_latitude=float(i['latitude'])
            total_review.append(i_review)
            overall_rating.append(i_orating)
            clean_level.append(i_clean)
            accuracy_level.append(i_accu)
            communication.append(i_com)
            location.append(i_location)
            coor.append((i_longitude,i_latitude))
            longitude.append(i_longitude)
        
        #print(clean_level)

        #print(total_review)
        #print(overall_rating)

        #Rating System
        # print (coor)
        rating_system = []
        new_rate = 0
        for i in range(len(total_review)):
            #print(overall_rating[i])
            if overall_rating[i] == 'None':
                overall_rating[i] = 0
            new_rate = int(overall_rating[i]) * 0.6 
            if int(total_review[i]) > 10:
                new_rate = new_rate + 5
            if str(clean_level[i]) == 'None':
                clean_level[i] = 0
            if accuracy_level[i] == 'None':
                accuracy_level[i] = 0
            if str(communication[i]) == 'None':
                communication[i] = 0
            if str(location[i] == 'None'):
                location[i] = 0
            new_rate = new_rate + (int(clean_level[i]) + int(accuracy_level[i]) + int(communication[i]) + int(location[i]))
            rating_system.append(new_rate)
        

        #Lit of Best Airbnb
        rating2=[]
        l=len(rating_system)

        relation=[]
        for i in airbnb_MBTA_entertain:
        
            relation.append((i['MBTA stops num within 2km'],i['entertainment around number']))
           



        result = []

        for i in range(len(rating_system)):
            result.append((rating_system[i],coor[i],relation[i]))

        

        index_good = []
        for i in range(len(rating_system)):
            if rating_system[i] > 90:
                index_good.append(str(i))

        for i in result:

            insertMaterial={'longitude':i[1][0], 'latitude':i[1][1], 'score': i[0], 'MBTA stops num within 2km': i[2][0], 'entertainment around number': i[2][1], 'surrounding restaurant num': rest[i]['Surrounding Restaurants num'], 'restaurantAVG': rest[i]['Avg Restaurants Score']}
        
            repo['bohan_nyx_xh1994_yiran123.airbnb_score_system'].insert_one(insertMaterial)
        print (insertMaterial)

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime = None, endTime = None):

        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohan_nyx_xh1994_yiran123','bohan_nyx_xh1994_yiran123')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format
        doc.add_namespace('dat', 'http://datamechanics.io/data/')
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:bohan_nyx_xh1994_yiran123#airbnbSS', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        airbnb_ss = doc.entity('dat:bohan_nyx_xh1994_yiran123',{'prov:label':'Airbnb Score System', prov.model.PROV_TYPE:'ont:DataSet'})

        mbta_airbnb = doc.entity('dat:bohan_nyx_xh1994_yiran123', {'prov:label':'MBTA Airbnb Score', prov.model.PROV_TYPE:'ont:DataSet'})

        get_Airbnb = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_Airbnb, this_script)
        doc.usage(get_Airbnb, airbnb_ss, startTime, None,
                  {prov.model.PROV_TYPE: 'ont:Computation'})

        Airbnb = doc.entity('dat:bohan_nyx_xh1994_yiran123#airbnbSS',
                            {prov.model.PROV_LABEL:'Airbnb Score System',
                             prov.model.PROV_TYPE: 'ont:DataSet'})

        doc.wasAttributedTo(Airbnb, this_script)
        doc.wasGeneratedBy(Airbnb, get_Airbnb, endTime)
        doc.wasDerivedFrom(Airbnb, airbnb_ss, get_Airbnb, get_Airbnb, get_Airbnb)
        doc.wasDerivedFrom(Airbnb, mbta_airbnb, get_Airbnb, get_Airbnb, get_Airbnb)

        repo.logout()

        return doc

















