import urllib.request
import json
import dml
import prov.model
import datetime 
import uuid

class rent(dml.Algorithm):
    contributor = 'minteng_tigerlei_zhidou'
    reads = []
    writes = ['minteng_tigerlei_zhidou.rent', 'minteng_tigerlei_zhidou.location','minteng_tigerlei_zhidou.salary']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets.'''
        startTime = datetime.datetime.now()
        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('minteng_tigerlei_zhidou', 'minteng_tigerlei_zhidou')


        def getPostalCode(city, key):
            # find the everage location coordinate
            city = city.replace(' ','+')
            url = "https://maps.googleapis.com/maps/api/geocode/json?address="+city+",+Boston,+MA&key="+key
            response = urllib.request.urlopen(url).read().decode("utf-8")
            crime_info=json.loads(response)
            loc = crime_info['results'][0]['geometry']['location']
            
            url = "https://maps.googleapis.com/maps/api/geocode/json?latlng="+str(loc['lat'])+","+str(loc['lng'])+"&key="+key
            response = urllib.request.urlopen(url).read().decode("utf-8")
            crime_info=json.loads(response)
            
            for i in crime_info['results'][0]['address_components']:
                if 'postal_code' in i['types']: break
            return i['long_name']

        #new dataset 1: Rent Data
        url='http://datamechanics.io/data/minteng_zhidou/rent.txt'
        rents = urllib.request.urlopen(url).readlines()
        rent=[]
        for r in rents[1:]:
            s=str(r).split(',')
            temp={}
            temp['area']=s[1]
            temp['avg_rent']=int(s[2])
            temp['postal_code'] = getPostalCode(s[1], dml.auth['services']['googlemapsportal']['key'])
            
            rent.append(temp)

        repo.dropCollection("rent")
        repo.createCollection("rent")
        repo['minteng_tigerlei_zhidou.rent'].insert_many(rent)
        repo['minteng_tigerlei_zhidou.rent'].metadata({'complete':True})
        print(repo['minteng_tigerlei_zhidou.rent'].metadata())
        
        
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
        
        this_script = doc.agent('alg:minteng_tigerlei_zhidou#rent', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        ### data 1: rent
        rent_resource = doc.entity('dbr:minteng_tigerlei_zhidou#rent', {'prov:label':'City Average Rent', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txt'})
        
        get_rent = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_rent, this_script)
        doc.usage(get_rent, rent_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})


        rent = doc.entity('dat:minteng_tigerlei_zhidou#rent', {prov.model.PROV_LABEL:'City Average Rents', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(rent, this_script)
        doc.wasGeneratedBy(rent, get_rent, endTime)
        doc.wasDerivedFrom(rent, rent_resource, get_rent, get_rent, get_rent)

        
        repo.logout()
                  
        return doc

# project1.execute()
# doc = project1.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))

