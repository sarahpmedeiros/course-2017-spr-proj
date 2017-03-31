import urllib.request
import json
import dml
import prov.model
import datetime 
import uuid
import sys

TRIAL_LIMIT = 5000

class box_count(dml.Algorithm):
    contributor = 'minteng_tigerlei_zhidou'
    reads = ['minteng_tigerlei_zhidou.location']
    writes = ['minteng_tigerlei_zhidou.box_count']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets.'''
        startTime = datetime.datetime.now()
        
        if trial:
            print(" Now you are running trial mode")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('minteng_tigerlei_zhidou', 'minteng_tigerlei_zhidou')

        #get the boston bound
        import googlemaps
        api_key='AIzaSyCAZ-b2oPtcqrw1NNQ_jK2JhHptVmMKwgI'
        gmaps = googlemaps.Client(key=api_key)
        geocode_result = gmaps.geocode('boston')
        bound=geocode_result[0]['geometry']['bounds']
        lat_range=[bound['southwest']['lat'],bound['northeast']['lat']]
        lng_range=[bound['southwest']['lng'],bound['northeast']['lng']]


        #new dataset : box count
        
        k=10 ####set the #of box
        d1=lat_range[1]-lat_range[0]
        d2=lng_range[1]-lng_range[0]

        result=[]
        for i in range(k):
            for j in range(k):
                left=[lat_range[0]+i*d1/k,lng_range[0]+j*d2/k]
                right=[lat_range[0]+(i+1)*d1/k,lng_range[0]+(j+1)*d2/k]
                box=[left,right]
                #print(box)
                a=repo['minteng_tigerlei_zhidou.location'].find({'location':{'$geoWithin':{ '$box': box}}})
                d={'food':0,'transport':0,'crime':0}
                for item in a:
                    d[item['type']]+=1
                if sum(d.values())==0:
                    continue
                result.append({'box':box,'count':d})
                    
        #base on the count, generate the grade
        crime=[]
        transport=[]
        food=[]
        for r in result:
            crime.append(r['count']['crime'])
            transport.append(r['count']['transport'])
            food.append(r['count']['food'])
        c_max=max(crime)+0.001
        t_max=max(transport)+0.001
        f_max=max(food)+0.001

        k=5##grade from 1 - 5
        for r in result:
            r1=int(r['count']['crime']/c_max*k)+1
            r2=int(r['count']['food']/f_max*k)+1
            r3=int(r['count']['transport']/t_max*k)+1
            r['grade']={'crime':r1,'food':r2, 'transport':r3}

        repo.dropCollection("box_count")
        repo.createCollection("box_count")
        repo['minteng_tigerlei_zhidou.box_count'].insert_many(result)
        repo['minteng_tigerlei_zhidou.box_count'].metadata({'complete':True})
        print(repo['minteng_tigerlei_zhidou.box_count'].metadata())
        
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
        doc.add_namespace('dbr', 'http://datamechanics.io/data/')
        
        this_script = doc.agent('alg:minteng_tigerlei_zhidou#box_count', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        ### data : box_count
        box_count_resource = doc.entity('dbr:minteng_tigerlei_zhidou#location', {'prov:label':'Locations with tag and infomation', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        
        get_box_count = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_box_count, this_script)
        doc.usage(get_box_count, box_count_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})


        box_count = doc.entity('dat:minteng_tigerlei_zhidou#box_count', {prov.model.PROV_LABEL:'box count and grade', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(box_count, this_script)
        doc.wasGeneratedBy(box_count, get_box_count, endTime)
        doc.wasDerivedFrom(box_count, box_count_resource, get_box_count, get_box_count, get_box_count)

        
        repo.logout()
                  
        return doc

if 'trial' in sys.argv:
    box_count.execute(True)
# else:
#     box_count.execute()

# doc = box_count.provenance()
# #print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

