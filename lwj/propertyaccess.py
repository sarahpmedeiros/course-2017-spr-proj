import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import ssl
from pymongo import MongoClient

class propertyaccess(dml.Algorithm):
     contributor = 'lwj'
     reads = []
     writes = ['lwj.propertydb']

     @staticmethod
     def projection(property_data):
          propertyinfo = []
          count = 0
          count2 = 0
          for i in range(len(property_data)):
               try:
                    item = {}
                    item['addr'] = property_data[i][21]
                    item['addr2'] = property_data[i][22]
                    item['value'] = property_data[i][26]
                    item['area'] = property_data[i][32]
      #              print(item['value'], item['area'])
                    if (not (item['area'] == '0' or item['value'] == '0')):
                         item['avgvalue'] = str(int(int(item['value'])/(int(item['area'])*1.0)))
                         propertyinfo.append(item)
                         count2 +=1
               except:
                    count += 1
                    continue
          print(len(property_data))
          print(count, "errors")
          print(count2, "success")
          return propertyinfo

               

     @staticmethod
     def execute(trial = False):
          startTime = datetime.datetime.now()
          context = ssl._create_unverified_context()
          with open("./data/property.json","r") as f:
               response = f.readlines()
          res = ""
          for i in response:
               res += i
          #print(type(response))
          property_data = json.loads(res)
          property_data = property_data["data"]
          property_data = propertyaccess.projection(property_data)
          #print(property_data)
          client = dml.pymongo.MongoClient()
          project = client.project
          project.authenticate("lwj", "lwj")
          project.propertydb.drop()
          propertydb = project.propertydb
          for i in property_data:
               propertydb.insert_one(i)
          project.logout()
          endTime = datetime.datetime.now()
          return {"star":startTime, "end":endTime}

     @staticmethod
     def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
          doc = prov.model.ProvDocument()    

          doc.add_namespace('alg', 'lwj#union')
          doc.add_namespace('dat', 'lwj#propertydb')
          doc.add_namespace('ont', 'lwj/ontology#')
          doc.add_namespace('log', 'lwj/log/')
          doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
          e = doc.entity('dat:lwj#propertydb', {
              prov.model.PROV_TYPE: "ont:File",
              prov.model.PROV_LABEL:'Propertydb'
          })

          r = doc.entity('bdp:5b5-xrwi', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:property_data', 'ont:Extension':'json'})

          a = doc.agent('alg:lwj#propertyaccess', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extentsion':'py'})

          get_propertyaccess = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

          doc.usage(get_propertyaccess, r, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?$select=mail_address,mail_cs,av_total,living_area'
                  }
                  )

     
          doc.wasAssociatedWith(get_propertyaccess, a)
          doc.wasAttributedTo(e, a)
          doc.wasGeneratedBy(e, get_propertyaccess, endTime)
          doc.wasDerivedFrom(e, r, get_propertyaccess,get_propertyaccess,get_propertyaccess)


          return doc
                            
          

propertyaccess.execute()
doc = propertyaccess.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))


