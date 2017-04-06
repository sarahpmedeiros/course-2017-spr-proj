#Note: property.json has to be downloaded before running, it is too big to use urlib request

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import ssl
from pymongo import MongoClient
from math import radians, cos, sin, asin, sqrt

class geo(dml.Algorithm):
     contributor = 'rengx_ztwu_lwj'
     reads = ["rengx_ztwu_lwj.access", "rengx_ztwu_lwj.kmeans", "rengx_ztwu_lwj.centrals"]
     writes = ["rengx_ztwu_lwj.publicschool_accessibility", "rengx_ztwu_lwj.metadata"]
     @staticmethod
     def getDis(lon1, lat1, lon2, lat2):
          lon1 = int(lon1)/(1000000.0)
          lon2 = int(lon2)/(1000000.0)
          lat1 = 0 - int(lat1)/(1000000.0)
          lat2 = 0- int(lat2)/(1000000.0)
          lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
          dlon = lon2 - lon1
          dlat = lat2 - lat1
          a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
          c = 2 * asin(sqrt(a))
          km = 6367 * c
          return km
          

     @staticmethod
     def mapD(pes, aes):
          #pes for property
          #aes for access
          #print(pes[0])
          #print(aes[0])
          #print("analyzing the schools' accessibility")
          #print(len(aes))
          for p in pes:
 #              print(p)
               p['access'] = {}
               p['access']['hospital'] = []
               p['access']['garden'] = []
               p['access']['police'] = []
               p['access']['market'] = []
               for a in aes:
                   km = geo.getDis(p['x'], p['y'],  a['x'], a['y'])
                   #print(km)
                   if km < 2:
                        p['access'][a['type']].append(a)
 #         print(pes)
                   
          return pes
          
     @staticmethod
     def score(data, ps):
      summ = 0
      scs = []
      rs = [0,0,0,0,0]
      ns = [0,0,0,0,0]
      ps = [i["coords"] for i in ps]
      #print(ps)
      #print(ps)
      for i in data:
        #print(i)
        x = int(i["x"])
        y = int(i["y"])
        score = 0
        hos = len(i['access']['hospital'])
        gar = len(i['access']['garden'])
        pol = len(i['access']['police'])
        mar = len(i['access']['market'])
        score = gar + 1.5*pol + mar + hos*1.5
        i['access_score'] = score
        scs.append(score)
        summ += score
        cid = int(i["cid"])
        xx = ps[cid][0]
        yy = ps[cid][1]
        xx = int(str(xx)[:8])
        yy = int(str(yy)[:8])
        dis = geo.getDis(x, y, xx, yy)
        #print(dis)
        i["dist_central"] = dis
        rs[cid] += score
        ns[cid] += 1
      #print(ns)
      for i in range(len(rs)):
        rs[i] = rs[i]/(1.0*ns[i])
      
      avg = summ/(1.0*len(data))
      x = scs
      std = sqrt(sum([(xi-avg)**2 for xi in x])/len(x))
      #print(avg).  = 56.0300
      return data, avg, std, rs, ns
      
      
     @staticmethod
     def execute(trial = False):
          startTime = datetime.datetime.now()
          client = dml.pymongo.MongoClient()
          repo = client.repo
          repo.authenticate("rengx_ztwu_lwj", "rengx_ztwu_lwj")
          access = repo.access
          access_find = access.find()
          access_data = []
          for i in access_find:
               access_data.append(i)
          cf = repo.centrals.find()
          ps = []
          for i in cf:
            ps.append(i)
          #print(len(access_data))
 #         print(access_data[0])
          publicschool_find = repo.kmeans.find()
          
          publicschool_data = []
          for i in publicschool_find:
               publicschool_data.append(i)
          #print(publicschool_data[0])
          if(trial):
            publicschool_data = publicschool_data[0:50]
            access_data = access_data[0:50]
          #print(publicschool_data)
 #         print(property_data[0])
 #         geo.getDis(property_data[0]['addr'], access_data[0]['addr'])
          publicschool_data = geo.mapD(publicschool_data, access_data)
          publicschool_data, avg, std, rs, ns = geo.score(publicschool_data, ps)
          repo.publicschool_accessibility.drop()
          publicschool_accessibility = repo.publicschool_accessibility
          publicschool_accessibility.insert_many(publicschool_data)
          
          repo.metadata.drop()
          repo.metadata.insert_one({"avg_score":avg, "std":std})
          repo.logout()
          endTime = datetime.datetime.now()
          #print("geo complete")
          return {"start":startTime, "end":endTime}
     
     @staticmethod
     def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
          return doc
                            
          
#geo.execute()
#doc = union.provenance()
#print(doc.get_provn())
#print(json.dumps(json.loads(doc.serialize()), indent=4))
