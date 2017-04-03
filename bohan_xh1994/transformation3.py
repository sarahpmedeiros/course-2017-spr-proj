import urllib.request
import json
import dml
import prov.model
import datetime
import uuid


class transformation3(dml.Algorithm):
    contributor = 'bohan_xh1994'
    reads = ['bohan_xh1994.Active_Food_Establishment_Licenses','bohan_xh1994.Food_Establishment_Inspections']
    writes = ['bohan_xh1994.restaurant_cleanness_level']


    def union(R, S):
        return R + S

    def difference(R, S):
        return [t for t in R if t not in S]

    def intersect(R, S):
        return [t for t in R if t in S]

    def project(R, p):
        return [p(t) for t in R]

    def select(R, s):
        return [t for t in R if s(t)]
 
    def product(R, S):
        return [(t,u) for t in R for u in S]

    def aggregate(R, f):
        keys = {r[0] for r in R}
        return [(key, f([v for (k,v) in R if k == key])) for key in keys]



    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohan_xh1994', 'bohan_xh1994')  
        FoodAL = repo.bohan_xh1994.Active_Food_Establishment_Licenses.find()
        Food = repo.bohan_xh1994.Food_Establishment_Inspections.find({"licstatus": 'Active'})
        FoodEI = [c for c in Food]


        #Foodlocation_name = FoodEI.project(lambda t: (t['businessname'],t['location']))
        #crime_location = crime.project(lambda t: (t[-2]))
        #safety_level = []
        repo.dropCollection("restaurant_cleanness_level")
        repo.createCollection("restaurant_cleanness_level")

        for i in FoodAL:
            i_name = str(i['businessname'])
            total = 0
            pass_time = 0
            passrate = 0
            for j in FoodEI:
                j_name = str(j['businessname'])
                if i_name == j_name:
                    temp = 'Pass'
                    total+=1
                    try:
                        j_vol = str(j['violstatus'])
                    except:
                        j_vol = None
                    if j_vol == temp:
                        pass_time +=1
            # print('total:')
            # print(total)
            # print('pass-time')
            # print(pass_time)
            if total == 0:
                passrate =0

            else:                    
                passrate = '{:.1%}'.format(pass_time/total)
            insertMaterial = {'Businessname':i['businessname'], 'location':i['location'],'total inspections': total, 'pass inspectins': pass_time, 'cleanness level' :passrate}

            repo['bohan_xh1994.restaurant_cleanness_level'].insert_one(insertMaterial)

            

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

        this_script = doc.agent('alg:bohan_xh1994#transformation3', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        clean = doc.entity('dat:bohan_xh1994#restaurant_cleanness_level', {prov.model.PROV_LABEL:'food establishment cleanness level', prov.model.PROV_TYPE:'ont:DataSet'})
        get_clean= doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_clean, this_script)
        doc.used(get_clean, clean, startTime
                  ,{prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Food_Establishment_cleanness&$select=type,businessname,location,total inspections, pass inspections, cleanness level'
                  }
                  )

        #lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(clean, this_script)
        doc.wasGeneratedBy(clean, get_clean, endTime)
        #doc.wasDerivedFrom(Rest_safe, resource, get_lost, get_lost, get_lost)

        repo.logout()
                  
        return doc

transformation3.execute()
doc = transformation3.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))