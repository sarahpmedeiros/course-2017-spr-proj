import urllib.request
import json
import dml
import prov.model
import datetime 
import uuid
from collections import defaultdict
class salary(dml.Algorithm):
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

        #new dataset 3: Salary data
        salary=[]
        map={'2014':'ntv7-hwjm.json', '2015':'bejm-5s9g.json'}
        for year in map:
            url='https://data.cityofboston.gov/resource/'+map[year]+'?$limit=50000'
            response = urllib.request.urlopen(url).read().decode("utf-8")
            salary_info=json.loads(response)
            for s in salary_info:
                temp={}
                temp['name']=s['department_name']
                temp['job']=s['title'].replace(".","_")
                temp['salary']=s['total_earnings']
                temp['year']=year
                salary.append(temp)

        def aggregate_salary(R):
            res=defaultdict(list)
            for r in R:
                temp={}
                temp['name']=r['name']
                temp['salary']=r['salary']
                temp['year']=r['year']
                res[r['job']].append(temp)

            res1=[]
            for key in res:
                temp={}
                salarys=[float(values['salary'])for values in res[key]]
                avg=sum(salarys)/len(salarys)
                t={}
                t['avg_salary']=avg
                t['data']=res[key]
                temp[key]=t
                res1.append(temp)
            return res1
            
        salary1=aggregate_salary(salary)
        repo.dropCollection("salary")
        repo.createCollection("salary")
        repo['minteng_tigerlei_zhidou.salary'].insert_many(salary1)
        repo['minteng_tigerlei_zhidou.salary'].metadata({'complete':True})
        print(repo['minteng_tigerlei_zhidou.salary'].metadata())
        
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
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        
        this_script = doc.agent('alg:minteng_tigerlei_zhidou#salary', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        
        
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

        ### new data 3: salary
        salary = doc.entity('dat:minteng_tigerlei_zhidou#salary', {prov.model.PROV_LABEL:'Salary 2015', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(salary, this_script)
        doc.wasGeneratedBy(salary, get_salary1, endTime)
        doc.wasDerivedFrom(salary, salary1_resource, get_salary1, get_salary1, get_salary1)
        doc.wasGeneratedBy(salary, get_salary2, endTime)
        doc.wasDerivedFrom(salary, salary2_resource, get_salary2, get_salary2, get_salary2)


        repo.logout()       
        return doc

