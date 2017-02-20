# Get population income by neighborhood data
# https://www.census.gov/data/developers/data-sets/acs-1year.html (dk how to get it but apparently it's from here)

from bs4 import BeautifulSoup

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import re

d = []
instance = {}

class population(dml.Algorithm):
    contributor = 'jguerero_mgarcia7'
    reads = []
    writes = ['jguerero_mgarcia7.population']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now() #gets data by year and date

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jguerero_mgarcia7', 'jguerero_mgarcia7')

        url = 'http://www.city-data.com/nbmaps/neigh-Boston-Massachusetts.html#N46'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        soup = BeautifulSoup(response, 'html.parser') #parse data to be able to extract information

        for i in soup.findAll("div", {"class": "neighborhood"}): #returns a result set
            data = i.text
            instance = {}
            instance['Neighborhood'] = exc(re.findall('(.+) neighborhood in', data))
            instance['Area (in Square miles)'] = exc(re.findall('Area: (.+) square miles', data))
            instance['Population'] = exc(re.findall('Population: (.+)Population', data))

            try: 
                a = regexFix(str(i.contents[10].contents[0].contents[2].contents[0].contents[1].b.string))
            except AttributeError:
                pass

            t = a + '(.+) people per square mileBoston'
            instance['Population Density: '] = exc(re.findall(t, data))

            b = 'mileMedian household income in 2015: ' + a + '\$(\d+,?\d+)'
            instance['Median Household Income in 2015 ($)'] = exc(re.findall(b, data))

            c = 'Median rent in in 2015: ' + a + '\$(\d+,?\d+)'
            instance['Median rent in 2015 ($)'] = exc(re.findall(c, data))

            instance['Male Population'] = exc(re.findall('Males:(\d+,?\d+)Females:', data))
            instance['Female Population'] = exc(re.findall('Females:(.+)Median ', data))
            instance['Median Age in Male Population (years)'] = exc(re.findall('ageMales:(.+) yearsFemales', data))
            instance['Median Age in Female Population (years)'] = exc(re.findall('yearsFemales:(.+) years', data))

            d.append(instance)


        new = json.loads(json.dumps(d))
        repo.dropCollection("population")
        repo.createCollection("population")
        repo['jguerero_mgarcia7.population'].insert_many(new)
        repo['jguerero_mgarcia7.population'].metadata({'complete':True})
        print(repo['jguerero_mgarcia7.population'].metadata())

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
        repo.authenticate('jguerero_mgarcia7')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent('alg:jguerero_mgarcia7#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
        resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        doc.wasAssociatedWith(get_found, this_script)
        doc.wasAssociatedWith(get_lost, this_script)
        doc.usage(get_found, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )
        doc.usage(get_lost, resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                  }
                  )

        lost = doc.entity('dat:jguerero_mgarcia7#population', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(lost, this_script)
        doc.wasGeneratedBy(lost, get_lost, endTime)
        doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

        found = doc.entity('dat:jguerero_mgarcia7#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(found, this_script)
        doc.wasGeneratedBy(found, get_found, endTime)
        doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

        repo.logout()
                  
        return doc

def regexFix(string):
    if '(' in string or ')' in string:
        first = string.index('(')
        second = string.index(')')
        s = string[0:first] + '\\' + string[first:second] + '\\' + string[second:] 
    else:
        s = string
    return s

def exc(x):
    try:
        return x[0]
    except IndexError:
        return None


population.execute()
#doc = population.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
