import urllib.request
import json
import dml
import prov.model
import datetime 
import uuid
import sys
import googlemaps

TRIAL_LIMIT = 5000

class box_count(dml.Algorithm):
    contributor = 'minteng_tigerlei_zhidou'
    reads = ['minteng_tigerlei_zhidou.location', 'minteng_tigerlei_zhidou.rent']
    writes = ['minteng_tigerlei_zhidou.box_count']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets.'''
        startTime = datetime.datetime.now()
        print("box_count start!")
        if trial:
            print(" Now you are running trial mode")

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('minteng_tigerlei_zhidou', 'minteng_tigerlei_zhidou')

        #get the boston bound
        gmaps = googlemaps.Client(key = dml.auth['services']['googlemapsportal']['key'])
        geocode_result = gmaps.geocode('boston')
        bound=geocode_result[0]['geometry']['bounds']
        lat_range=[bound['southwest']['lat'],bound['northeast']['lat']]
        lng_range=[bound['southwest']['lng'],bound['northeast']['lng']]
        #change parameter
        lat_range[1] =float(42.399531)
        lng_range[1] =float(-70.922160)
        #new dataset : box count
        
        k=10 ####set the #of box
        d1=lat_range[1]-lat_range[0]
        d2=lng_range[1]-lng_range[0]
        radius=max(d1,d2)/k
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
                #increase the radius of food             
                if d['food'] < 20:
                    d['food'] = 0
                    center=[0.5*(left[0]+right[0]),0.5*(left[1]+right[1])]
                    b=repo['minteng_tigerlei_zhidou.location'].find({'location':{'$geoWithin':{ '$center': [ center, radius] }}})
                    for item in b:
                        if item['type']=='food':
                            d['food']+=1

                if d['crime']==0:
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
        def get_range(x):
            xx=sorted(x)
            k=int(len(xx)/5)
            x1=xx[0];x2=xx[k];x3=xx[2*k]
            x4=xx[3*k];x5=xx[4*k];x6=xx[len(xx)-1]
            return [x1,x2,x3,x4,x5,x6]
        def get_grade(c,ranges):
            [x1,x2,x3,x4,x5,x6]=ranges
            if c==0:
                return 1
            elif x1<=c<x2:
                return 1
            elif x2<=c<x3:
                return 2
            elif x3<=c<x4:
                return 3
            elif x4<=c<x5:
                return 4
            else:
                return 5
        c=get_range(crime)
        t=get_range(transport)
        f=get_range(food)
        for r in result:
            r1=get_grade(r['count']['crime'],c)
            r2=get_grade(r['count']['food'],f)
            r3=get_grade(r['count']['transport'],t)
            r['grade']={'safety':r1,'food':r2, 'transport':r3}
        #reverse grade for crime:
        for i in result:
            if i['grade']['safety']==1:
                i['grade']['safety']=5
            elif i['grade']['safety']==2:
                i['grade']['safety']=4
            elif i['grade']['safety']==4:
                i['grade']['safety']=2
            elif i['grade']['safety']==5:
                i['grade']['safety']=1
        #find the rent and the grade for rent
        def find_rent(zipcode):
            rent=repo['minteng_tigerlei_zhidou.rent'].find()
            for i in rent:
                if i['postal_code']==zipcode:
                    return i['area'],i['avg_rent']
            area=gmaps.geocode(zipcode)[0]['formatted_address'].split()[0]
            rent=repo['minteng_tigerlei_zhidou.rent'].find()
            for j in rent:
                if j['area']==area:
                    return j['area'],j['avg_rent']
            return "Not found", "Not found"

        for i in result:
            #center=[0.5*(i['box'][0][0]+i['box'][1][0]),.5*(i['box'][0][1]+i['box'][1][1])]
            box=i['box']
            a=repo['minteng_tigerlei_zhidou.location'].find({'type':'crime','location':{'$geoWithin':{ '$box': box}}})
            x=[]
            y=[]
            for j in a:
                x.append(j['location'][0])
                y.append(j['location'][1])
            center=[sum(x)/len(x),sum(y)/len(y)]
            zipcode=(gmaps.reverse_geocode(center)[0]['formatted_address'].split()[-2].replace(",",""))
            #if(len(zipcode))<5:
            #    zipcode='Not found'
            #print(zipcode)
            i['postal_code']=zipcode
            i['area'],i['avg_rent']=find_rent(zipcode)
        rr=[]
        for i in result:
            if i['avg_rent']!='Not found':
                rr.append(i['avg_rent'])
        r=get_range(rr)
        for i in result:
            if i['avg_rent']=='Not found':
                i['grade']['rent']='Not found'
            else:
                i['grade']['rent']=get_grade(i['avg_rent'],r)
        #reverse grade for rent:
        for i in result:
            if i['grade']['rent']==1:
                i['grade']['rent']=5
            elif i['grade']['rent']==2:
                i['grade']['rent']=4
            elif i['grade']['rent']==4:
                i['grade']['rent']=2
            elif i['grade']['rent']==5:
                i['grade']['rent']=1
        
        repo.dropCollection("box_count")
        repo.createCollection("box_count")
        repo['minteng_tigerlei_zhidou.box_count'].insert_many(result)

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
        
        this_script = doc.agent('alg:minteng_tigerlei_zhidou#box_count', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        ### data : box_count
        #derive from location
        box_count_resource = doc.entity('dat:minteng_tigerlei_zhidou#location', {'prov:label':'Locations with tag and infomation', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        box_count_resource2 = doc.entity('dat:minteng_tigerlei_zhidou#rent', {'prov:label':'Rent data', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        box_count = doc.entity('dat:minteng_tigerlei_zhidou#box_count', {prov.model.PROV_LABEL:'box count and grade', prov.model.PROV_TYPE:'ont:DataSet'})
        
        get_box_count = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_box_count, this_script)
        doc.usage(get_box_count, box_count_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})

        doc.usage(get_box_count, box_count_resource2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
       
        doc.wasAttributedTo(box_count, this_script)
        doc.wasGeneratedBy(box_count, get_box_count, endTime)
        doc.wasDerivedFrom(box_count, box_count_resource, get_box_count, get_box_count, get_box_count)
        doc.wasDerivedFrom(box_count, box_count_resource2, get_box_count, get_box_count, get_box_count)
        repo.logout()
                  
        return doc

# if 'trial' in sys.argv:
#     box_count.execute(True)
# else:
#     box_count.execute()

# doc = box_count.provenance()
# #print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

