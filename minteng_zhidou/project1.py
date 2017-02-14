import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
class project1(dml.Algorithm):
    contributor = 'minteng_zhido'
    reads = []
    writes = ['minteng_zhido.rent', 'minteng_zhido.location','minteng_zhido.salary']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('minteng_zhidou', 'minteng_zhidou')

        #new dataset 1: Rent Data
        url='http://datamechanics.io/data/minteng_zhidou/rent.txt'
        rents = urllib.request.urlopen(url).readlines()
        rent=[]
        for r in rents[1:]:
            s=str(r).split(',')
            temp={}
            temp['city']=s[1]
            temp['avg_rent']=int(s[2])
            rent.append(temp)
        repo.dropCollection("rent")
        repo.createCollection("rent")
        repo['minteng_zhidou.rent'].insert_many(rent)
        repo['minteng_zhidou.rent'].metadata({'complete':True})
        print(repo['minteng_zhidou.rent'].metadata())
        
        #new dataset 2: location data with key: (location, tag), tag: food, transport and crime
        ####food data
        url='https://data.cityofboston.gov/resource/fdxy-gydq.json?$limit=50000'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        food=json.loads(response)
        food_info=[]
        for f in food:
            try:
                temp={}
                temp['address']=f['address']
                temp['businessname']=f['businessname']
                temp['city']=f['city']
                temp['location']=(f['location']['coordinates'],"food")
                temp['zip']=f['zip']
                food_info.append(temp)
            except KeyError:
                continue
        
        ###transport data
        url='http://datamechanics.io/data/minteng_zhidou/stops.txt'
        stops = urllib.request.urlopen(url).readlines()
        transport=[]
        for stop in stops[1:]:
            s=str(stop).split(',')
            temp={}
            temp['name']=s[2]
            try:
                temp['location']=([float(s[4]),float(s[5])],"transport")
            except ValueError:
                continue
            transport.append(temp)
        
        ###safety/crime data
        url='https://data.cityofboston.gov/resource/29yf-ye7n.json?$limit=50000'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        crime_info=json.loads(response)
        crime=[]
        for c in crime_info:
            try:
                temp={}
                temp['location']=(c['location']['coordinates'],"crime")
                temp['street']=c['street']
                crime.append(temp)
            except KeyError:
                continue
    
        url='https://data.cityofboston.gov/resource/29yf-ye7n.json?$limit=50000&$offset=50001'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        crime_info=json.loads(response)
        for c in crime_info:
            try:
                temp={}
                temp['location']=(c['location']['coordinates'],"crime")
                temp['street']=c['street']
                crime.append(temp)
            except KeyError:
                continue
    
        url='https://data.cityofboston.gov/resource/29yf-ye7n.json?$limit=50000&$offset=100001'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        crime_info=json.loads(response)
        for c in crime_info:
            try:
                temp={}
                temp['location']=(c['location']['coordinates'],"crime")
                temp['street']=c['street']
                crime.append(temp)
            except KeyError:
                continue

        union=food_info+transport+crime
        repo.dropCollection("location")
        repo.createCollection("location")
        repo['minteng_zhidou.location'].insert_many(union)
        repo['minteng_zhidou.location'].metadata({'complete':True})
        print(repo['minteng_zhidou.location'].metadata())

        #new dataset 3: Salary data
        url='https://data.cityofboston.gov/resource/bejm-5s9g.json?$limit=50000' ###2015
        response = urllib.request.urlopen(url).read().decode("utf-8")
        salary_info=json.loads(response)
        salary=[]
        for s in salary_info:
            temp={}
            temp['name']=s['department_name']
            temp['job']=s['title']
            temp['salary']=s['total_earnings']
            temp['year']=2015
            salary.append(temp)
    
        url='https://data.cityofboston.gov/resource/ntv7-hwjm.json?$limit=50000' ###2014
        response = urllib.request.urlopen(url).read().decode("utf-8")
        salary_info=json.loads(response)
        for s in salary_info:
            temp={}
            temp['name']=s['department_name']
            temp['job']=s['title']
            temp['salary']=s['total_earnings']
            temp['year']=2014
            salary.append(temp)
            
        repo.dropCollection("salary")
        repo.createCollection("salary")
        repo['minteng_zhidou.salary'].insert_many(salary)
        repo['minteng_zhidou.salary'].metadata({'complete':True})
        print(repo['minteng_zhidou.salary'].metadata())
        
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
        repo.authenticate('minteng_zhidou', 'minteng_zhidou')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
        doc.add_namespace('bdp1', 'http://datamechanics.io/data/minteng_zhidou/')
        
        this_script = doc.agent('alg:minteng_zhidou#project1', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        ### data 1: rent
        rent_resource = doc.entity('bdp1:rent', {'prov:label':'City Average Rent', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txt'})
        get_rent = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_rent, this_script)
        doc.usage(get_rent, rent_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        ### data 2: transport
        transport_resource = doc.entity('bdp1:stops', {'prov:label':'MBTA Stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'txt'})
        get_transport = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_transport, this_script)
        doc.usage(get_transport, transport_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval'})
        ### data 3: food
        food_resource = doc.entity('bdp:gb6y-34cq', {'prov:label':'Active Food Establishment Licenses', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_food = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_food, this_script)
        doc.usage(get_food, food_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?$select=address,city,businessname,location,zip'})
        ### data 4: safety/crime
        crime_resource = doc.entity('bdp:fqn4-4qap', {'prov:label':'Crime Incident Reports', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_crime, this_script)
        doc.usage(get_crime, crime_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?$select=location,street'})
        ### data 5: salary 2015
        salary1_resource = doc.entity('bdp:ah28-sywy', {'prov:label':'Employee Earnings Report 2015', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_salary1 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_salary1, this_script)
        doc.usage(get_salary1, salary1_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?$select=department_name,title,total_earnings'})
        ### data 6: salary 2014
        salary2_resource = doc.entity('bdp:4swk-wcg8', {'prov:label':'Employee Earnings Report 2014', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_salary2 = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_salary2, this_script)
        doc.usage(get_salary2, salary2_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval','ont:Query':'?$select=department_name,title,total_earnings'})

       
        ### new data 1: rent
        rent = doc.entity('dat:minteng_zhidou#rent', {prov.model.PROV_LABEL:'City Average Rents', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(rent, this_script)
        doc.wasGeneratedBy(rent, get_rent, endTime)
        doc.wasDerivedFrom(rent, rent_resource, get_rent, get_rent, get_rent)

        ### new data 2: location
        location = doc.entity('dat:minteng_zhidou#location', {prov.model.PROV_LABEL:'Locations with tag and infomation', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(location, this_script)
        doc.wasGeneratedBy(location, get_transport, endTime)
        doc.wasDerivedFrom(location, transport_resource, get_transport, get_transport, get_transport)

        doc.wasGeneratedBy(location, get_food, endTime)
        doc.wasDerivedFrom(location, food_resource, get_food, get_food, get_food)

        doc.wasGeneratedBy(location, get_crime, endTime)
        doc.wasDerivedFrom(location, crime_resource, get_crime, get_crime, get_crime)

        ### new data 3: salary
        salary = doc.entity('dat:minteng_zhidou#salary', {prov.model.PROV_LABEL:'Salary 2015', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(salary, this_script)
        doc.wasGeneratedBy(salary, get_salary1, endTime)
        doc.wasDerivedFrom(salary, salary1_resource, get_salary1, get_salary1, get_salary1)

        doc.wasGeneratedBy(salary, get_salary2, endTime)
        doc.wasDerivedFrom(salary, salary2_resource, get_salary2, get_salary2, get_salary2)


        repo.logout()
                  
        return doc

project1.execute()
doc = project1.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

