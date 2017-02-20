#import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import ast
import sodapy
import time 

# this transformation will check how many comm gardens and food pantries there are for each area
# we want to take (zipcode, #comm gardens) (zipcode, #food pantries) --> (area, #food pantries#comm gardens)
# I think this is a selection and aggregation

class transformation_one(dml.Algorithm):

    contributor = 'mrhoran_rnchen'
    reads = ['mrhoran_rnchen.community_gardens','mrhoran_rnchen.food_pantries']
    writes = ['mrhoran_rnchen.healthy_options']
    @staticmethod
    def execute(trial = False):
        
        startTime = datetime.datetime.now()

        client = dml.pymongo.MongoClient()

        repo = client.repo
        
        repo.authenticate('mrhoran_rnchen', 'mrhoran_rnchen')
        
        # make a new tuple (zipcode, #comm gardens) from
        #{"area":"Fenway/Kenmore","coordinates":"42.3444242,-71.0952061",
        #"location":"15 Park Dr.","map_location_location":"15 Park Dr.",
        #"site":"Richard Parker Memorial CG","state":"MA","zip_code":"2115"}


        #test_arr = []
        #temp = [o for o in repo.mrhoran_rnchen.community_gardens.find({})]
        #for g in temp:
        #     test_arr.append({g['site'],g['zip_code']})
        #print(test_arr)

        #X = project(select('mrhoran_rnchen.community_gardens', lambda t: t[0] == "zip_code"), lambda t: (t[1],1))

        X = project([o for o in repo.mrhoran_rnchen.community_gardens.find({})], getGZips)
        #print(X)

        # agg those tuples

        commgarden_zip_count = dict(project(aggregate(X, sum), lambda t: (t[0], ('comm_gardens',t[1]))))

        repo.mrhoran_rnchen.commgarden_zip_count.insert_many(commgarden_zip_count)
      
	#insert, insert_many, or insertMany? 
        #print(commgarden_zip_count)
 
        # make a new tuple (zipcode, #food_pantries) from {"area":"South End",
        #"hours":"Saturdays 10:30 - 12:00","location":"1860 Washington St",
        #"location_1_city":"South End","location_1_location":"1860 Washington St",
        #"location_1_state":"MA","name":"Grant Manor","site_number":"FB81",
        #"source":"http://www.fairfoods.org/dollarbag.html","zip_code":"21178"}

 #       Y = project(select('mrhoran_rnchen.food_pantries', lambda t: t[0] == "zip_code"), lambda t: (t[1],1))
  
        Y = project([p for p in repo.mrhoran_rnchen.food_pantries.find({})], getPZips)

        # agg those tuples
        # agg those new tuples

        foodpantry_zip_count = project(aggregate(Y,sum), lambda t: (t[0], ('food_pantry',t[1])))

        repo.mrhoran_rnchen.foodpantry_zip_count.insertMany(foodpantry_zip_count)
       
        # combine them to make a new data set like (zip, (comm,1), (foodp, 1))

        temp = product(commgarden_zip_count, foodpantry_zip_count)

        result = project(select(temp, lambda t: t[0][0] == t[1][0]), lambda t: (t[0][0], t[0][1], t[1][1]))
       

        for z in commgarden_zip_count:
            if z[0] not in result:
                 result.append((z[0],z[1],('food_pantry',0)))

        for p in foodpantry_zip_count:
            if p[0] not in result:
                 result.append((p[0],('comm_gardens',0),p[1]))

        print(result)

	
        repo.mrhoran_rnchen.garden_pantry_agg.insertMany(result)
       
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
        repo.authenticate('mrhoran_rnchen', 'mrhoran_rnchen')
        
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:mrhoran_rnchen#getData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        # label section might be wrong
        resource1 = doc.entity('bdp:rdqf-ter7', {'prov:label':'Community Gardens', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_community_gardens = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

        doc.wasAssociatedWith(get_community_gardens, this_script)

        doc.usage(get_community_gardens, resource1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'
                  #'ont:Query':'location, area, coordinates, zip_code' #?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        community_gardens = doc.entity('dat:mrhoran_rnchen#community_gardens', {prov.model.PROV_LABEL:'Community Gardens', prov.model.PROV_TYPE:'ont:DataSet','ont:Extension':'json'})
        doc.wasAttributedTo(community_gardens, this_script)
        doc.wasGeneratedBy(community_gardens, get_community_gardens, endTime)
        doc.wasDerivedFrom(community_gardens, resource1, get_community_gardens, get_community_gardens, get_community_gardens)
        repo.logout()
                  
        return doc


def aggregate(R, f):
    keys = {r[0] for r in R}
    return [(key, f([v for (k,v) in R if k == key])) for key in keys]
    
def select(R, s):
    return [t for t in R if s(t)]

def project(R, p):
    return [p(t) for t in R]

def product(R, S):
    return [(t,u) for t in R for u in S]

def getGZips(garden):
    return(['0'+garden['zip_code'],1])

def getPZips(foodpantry):
    return([foodpantry['zip_code'],1])



transformation_one.execute()
doc = transformation_one.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
