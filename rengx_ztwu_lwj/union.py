import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import ssl
from pymongo import MongoClient
import sodapy
class union(dml.Algorithm):
       contributor = 'rengx_ztwu_lwj'
       reads = ["rengx_ztwu_lwj.market"]
       writes = ["rengx_ztwu_lwj.access"]

       @staticmethod
       def unionF(hospital_data, garden_data, market_data, police_data):
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
                        
               for i in range(len(police_data)):
                  item = {}
                  coords = police_data[i]['location']['coordinates']
                  xcoor = str(int(coords[1]*1000000))
                  ycoor = str(int(coords[0]*(-1000000)))
                  item["x"] = xcoor
                  item["y"] = ycoor
                  item["type"] = "police"
                  item["addr"] = police_data[i]['location_location']
                  access.append(item)

               
               #288
               return access
                      
 
       @staticmethod
       def execute(trial = False):
               startTime = datetime.datetime.now()
               client = dml.pymongo.MongoClient()
               repo = client.repo
               repo.authenticate("rengx_ztwu_lwj", "rengx_ztwu_lwj")
               market = repo.market
               market_find = market.find()
               market_data = []
               for i in market_find:
                      market_data.append(i)
               
               ss = sodapy.Socrata("data.cityofboston.gov", "x92LG4iaFto5qWQGFk3lDdv6p", username="lwj@bu.edu",password = "KatrinaLu2017")
               response = ss.get("u6fv-m8v4")
               r = json.loads(json.dumps(response, sort_keys=True, indent=2))
               hospital_data = r #26
               
               response = ss.get("rdqf-ter7")
               r = json.loads(json.dumps(response, sort_keys=True, indent=2))
               garden_data = r
               #184

               # get Police Station data in Boston
               response = ss.get("pyxn-r3i2")
               r = json.loads(json.dumps(response, sort_keys=True, indent=2))
               police_data = r
               accessdb = union.unionF(hospital_data, garden_data, market_data, police_data)
               repo.access.drop()
               access = repo.access
               access.insert_many(accessdb)
               repo.logout()
               endTime = datetime.datetime.now()
               #print("union complete")
               return {"start":startTime, "end":endTime}
       
       @staticmethod
       def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
         return doc
                                          
               
#union.execute()
#doc = union.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
