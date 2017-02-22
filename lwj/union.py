import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import ssl
from pymongo import MongoClient

class union(dml.Algorithm):
     contributor = 'lwj'
     reads = ["lwj.market"]
     writes = ["lwj.access"]

     @staticmethod
     def unionF(hospital_data, garden_data, market_data, college_data):
          access = []
          count = 0
          for i in range(len(hospital_data)):
               try:
                    item = {}
                    item['addr'] = hospital_data[i]['ad']
                    item['type'] = 'hospital'
                    #x, y len=8 (string)
                    item['y'] = hospital_data[i]['xcoord']
                    item['x'] = hospital_data[i]['ycoord']
                    #database gets the reversed xcoord and ycoord
                    access.append(item)
               except:
                    count += 1
                    #print(count, " mistakes occured.")
                    continue
               #24

          for i in range(1,len(garden_data)):         
               try:
                    item = {}
                    addr = garden_data[i]['location']
                    if(addr[-1:] == "\xa0"):
                         addr = addr[:-1]
                    item["addr"] = addr.upper()
                    item['type'] = 'garden'
                    coord = garden_data[i]['coordinates']
                    coords = coord.split(",")
                    xcoor = coords[0].split(".")
                    ycoor = coords[1].split(".")
                    xcoord = xcoor[0] + xcoor[1]
                    ycoord = ycoor[0][1:] + ycoor[1]
                    while(len(xcoord) < 8):
                         xcoord += "0"
                    while(len(ycoord) < 8):
                         ycoord += "0"
                    if(len(xcoord) > 8):
                         xcoord = xcoord[:8]
                    if(len(ycoord) > 8):
                         ycoord = ycoord[:8]
                    item["x"] = xcoord
                    item["y"] = ycoord
                    access.append(item)
               except:
                    count += 1
                    #print(count, " mistakes occured.")
                    continue
               #182

          for i in range(len(market_data)):
               try:
                    item = {}
                    addr = market_data[i]['addr_1']
                    item['addr'] = addr.upper()
                    item['type'] = 'market'
                    coords = market_data[i]['location']['coordinates']
                    #databse gets the reversed xcoord and ycoord
                    xcoor = str(coords[1]).split(".")
                    ycoor = str(coords[0]).split(".")
                    xcoord = xcoor[0]+xcoor[1]
                    ycoord = ycoor[0][1:]+ycoor[1]
                    while(len(xcoord) < 8):
                         xcoord += "0"
                    while(len(ycoord) < 8):
                         ycoord += "0"
                    if(len(xcoord) > 8):
                         xcoord = xcoord[:8]
                    if(len(ycoord) > 8):
                         ycoord = ycoord[:8]
                    item["x"] = xcoord
                    item["y"] = ycoord
                    access.append(item)
               except:
                    count += 1
                    #print(count, " mistakes occured.")
                    continue
               #288
                 
          for i in range(len(college_data["features"])):
               try:
                    item = {}
                    addr = college_data["features"][i]['properties']['Address']
                    if(addr[-1] in "1234567890"):
                         addr = addr[:-6]
                         addr = addr.split(",")[0]
                    item['addr'] = addr.upper()
                    item['type'] = 'college'
                    xcoor = str(college_data["features"][i]['geometry']['coordinates'][1]).split(".")
                    xcoord = xcoor[0] + xcoor[1]
                    ycoor = str(college_data["features"][i]['geometry']['coordinates'][0]).split(".")
                    ycoord = ycoor[0][1:] + ycoor[1]
                    while(len(xcoord) < 8):
                         xcoord += "0"
                    while(len(ycoord) < 8):
                         ycoord += "0"
                    if(len(xcoord) > 8):
                         xcoord = xcoord[:8]
                    if(len(ycoord) > 8):
                         ycoord = ycoord[:8]
                    item["x"] = xcoord
                    item["y"] = ycoord
                    access.append(item)
               except:
                    count += 1
                    #print(count, " mistakes occured.")
                    continue
               #60
               
          return access
               
 
     @staticmethod
     def execute(trial = False):
          startTime = datetime.datetime.now()
          client = dml.pymongo.MongoClient()
          project = client.project
          project.authenticate("lwj", "lwj")
          market = project.market
          market_find = market.find()
          market_data = []
          for i in market_find:
               market_data.append(i)
                    
          context = ssl._create_unverified_context()
          url = 'https://data.cityofboston.gov/resource/u6fv-m8v4.json'
          response = urllib.request.urlopen(url, context = context).read().decode("utf-8")
          hospital_data = json.loads(response)

          url = 'https://data.cityofboston.gov/resource/rdqf-ter7.json'
          response = urllib.request.urlopen(url, context = context).read().decode("utf-8")
          garden_data = json.loads(response)

          url = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/cbf14bb032ef4bd38e20429f71acb61a_2.geojson'
          response = urllib.request.urlopen(url, context = context).read().decode("utf-8")
          college_data = json.loads(response)

          url = 'https://data.cityofboston.gov/resource/g5b5-xrwi.json'
          response = urllib.request.urlopen(url, context = context).read().decode("utf-8")
          property_data = json.loads(response)

          accessdb = union.unionF(hospital_data, garden_data, market_data, college_data)
         
          project.access.drop()
          access = project.access
          access.insert_many(accessdb)
          project.logout()
          endTime = datetime.datetime.now()
          return {"start":startTime, "end":endTime}
     
     @staticmethod
     def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
          doc = prov.model.ProvDocument()

          doc.add_namespace('alg', 'lwj#union')
          doc.add_namespace('dat', 'lwj#access')
          doc.add_namespace('ont', 'lwj/ontology#')
          doc.add_namespace('log', 'lwj/log/')
          doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
          doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
          e = doc.entity('dat:lwj#access', {
              prov.model.PROV_TYPE: "ont:File",
              prov.model.PROV_LABEL:'Access'
          })

          r1 = doc.entity('bdp:u6fv-m8v4', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:hospital', 'ont:Extension':'json'})
          r2 = doc.entity('bdp:rdqf-ter7', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:garden', 'ont:Extension':'json'})
          r3 = doc.entity('dat:market', {prov.model.PROV_TYPE:'ont:market_data'})
          r4 = doc.entity('bod:cbf14bb032ef4bd38e20429f71acb61a_2', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:college', 'ont:Extension':'json'})
          

          a = doc.agent('alg:lwj#union', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extentsion':'py'})

          get_access = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)

          doc.usage(get_access, r1, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?$select=ad,xcoord,ycoord'
                  }
                  )
          doc.usage(get_access, r2, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?$select=location,coordinates'
                  }
                  )
          doc.usage(get_access, r3, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?$select=addr_1,location'
                  }
                  )
          doc.usage(get_access, r4, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?$select=features'
                  }
                  ) 
     
          doc.wasAssociatedWith(get_access, a)
          doc.wasAttributedTo(e, a)
          doc.wasGeneratedBy(e, get_access, endTime)
          doc.wasDerivedFrom(e, r1, get_access,get_access,get_access)
          doc.wasDerivedFrom(e, r2, get_access,get_access,get_access)
          doc.wasDerivedFrom(e, r3, get_access,get_access,get_access)
          doc.wasDerivedFrom(e, r4, get_access,get_access,get_access)

          return doc
                            
          
#union.execute()
#doc = union.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
