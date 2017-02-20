import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import ssl
from pymongo import MongoClient

class filtermarket(dml.Algorithm):
     contributor = 'lwj'
     reads = []
     writes = ["lwj.market"]

     @staticmethod
     def selection(market_data):
          citymarket = []
          city = ["allston", "back bay", "boston", "bay village", "beacon village", "brighton", "charlestown", "chinatown", "dorchester", "downtown", "east boston", "fenway", "hyde park",\
                  "jamaica plain", "mattapan", "mid-dorchester", "mission hill", "north end", "roslindale", "roxbury", "south boston", "south end", "west end", "west roxbury"]
          for i in range (len(market_data)):
               if (market_data[i]['town'].lower() in city):
                    citymarket.append(market_data[i])
               
     #30
          return citymarket



     @staticmethod
     def execute(trial = False):
          startTime = datetime.datetime.now()
          context = ssl._create_unverified_context()
          url = 'https://data.mass.gov/resource/66t5-f563.json'
          response = urllib.request.urlopen(url, context = context).read().decode("utf-8")
          market_data = json.loads(response)
          market_data = filtermarket.selection(market_data)
          client = dml.pymongo.MongoClient()
          project = client.project
          project.authenticate("lwj", "lwj")
          project.market.drop()
          market = project.market
          for i in market_data:
               market.insert_one(i)
          project.logout()
          endTime = datetime.datetime.now()
          return {"star":startTime, "end":endTime}
     
     @staticmethod
     def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
          doc = prov.model.ProvDocument()
          doc.add_namespace('alg', 'lwj#filtermarket')
          doc.add_namespace('dat', 'lwj#market')
          doc.add_namespace('ont', 'lwj/ontology#')
          doc.add_namespace('log', 'lwj/log/')
          doc.add_namespace('dmg', 'https://data.mass.gov/resource/')
          e = doc.entity('dat:lwj#market', {
               prov.model.PROV_TYPE: "ont:File",
               prov.model.PROV_LABEL:'Market'
          })
          r = doc.entity('dmg:66t5-f563', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:market_data', 'ont:Extension':'json'})
          a = doc.agent('alg:lwj#filtermarket', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extentsion':'py'})
          get_filtermarket = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

          doc.usage(get_filtermarket, r, startTime, None,
                    {prov.model.PROV_TYPE:'ont:Retrieval',
                     'ont:Query':'?$select=*'
                     }
                    )
          doc.wasAssociatedWith(get_filtermarket, a)
          doc.wasAttributedTo(e, a)
          doc.wasGeneratedBy(e, get_filtermarket, endTime)
          doc.wasDerivedFrom(e, r, get_filtermarket,get_filtermarket,get_filtermarket)

          return doc

     
filtermarket.execute()
doc = filtermarket.provenance()
print(doc.get_provn())


     
               

          
