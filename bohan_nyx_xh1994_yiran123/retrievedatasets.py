import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy

class retrievedatasets(dml.Algorithm):
    contributor = 'bohan_nyx_xh1994_yiran123'
    reads = []
    writes = ['bohan_nyx_xh1994_yiran123.crime_boston', 'bohan_nyx_xh1994_yiran123.Food_Establishment_Inspections', 'bohan_nyx_xh1994_yiran123.Entertainment_Licenses', 'bohan_nyx_xh1994_yiran123.airbnb_rating','bohan_nyx_xh1994_yiran123.Active_Food_Establishment_Licenses', 'bohan_nyx_xh1994_yiran123.MBTA_Bus_stops']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets (not using the API here for the sake of simplicity).'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('bohan_nyx_xh1994_yiran123', 'bohan_nyx_xh1994_yiran123')

        # city of boston crime incident July 2012 - August 2015
        #url = 'https://data.cityofboston.gov/resource/ufcx-3fdn.json'
        #response_crime = urllib.request.urlopen(url).read().decode("utf-8")
        client = sodapy.Socrata("data.cityofboston.gov", dml.auth['services']['cityofbostondataportal']['token'])
        r = client.get("29yf-ye7n", limit=3000)
        #r = json.loads(response_crime)
        #s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("crime_boston")
        repo.createCollection("crime_boston")
        repo['bohan_nyx_xh1994_yiran123.crime_boston'].insert_many(r)
        repo['bohan_nyx_xh1994_yiran123.crime_boston'].metadata({'complete':True})
        #print(repo['bohan_nyx_xh1994_yiran123.crime_boston'].metadata())

        #city of boston property assessment 2014
        '''url_property = 'https://data.cityofboston.gov/resource/jsri-cpsq.json'
        response_property = urllib.request.urlopen(url_property).read().decode("utf-8")
        r = json.loads(response_property)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("property_boston_assessment_2014")
        repo.createCollection("property_boston_assessment_2014")
        repo['bohan_nyx_xh1994_yiran123.property_boston_assessment_2014'].insert_many(r)'''

        #Food Establishment Inspections
        #url_foodIE = 'https://data.cityofboston.gov/resource/427a-3cn5.json'
        #response_foodIE = urllib.request.urlopen(url_foodIE).read().decode("utf-8")
        client = sodapy.Socrata("data.cityofboston.gov", dml.auth['services']['cityofbostondataportal']['token'])
        r = client.get("427a-3cn5", limit=467558)#467558
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("Food_Establishment_Inspections")
        repo.createCollection("Food_Establishment_Inspections")
        repo['bohan_nyx_xh1994_yiran123.Food_Establishment_Inspections'].insert_many(r)

        #https://data.cityofboston.gov/resource/fdxy-gydq.json
        client = sodapy.Socrata("data.cityofboston.gov", dml.auth['services']['cityofbostondataportal']['token'])
        r = client.get("fdxy-gydq", limit=3000)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("Active_Food_Establishment_Licenses")
        repo.createCollection("Active_Food_Establishment_Licenses")
        repo['bohan_nyx_xh1994_yiran123.Active_Food_Establishment_Licenses'].insert_many(r)

        #entertainment Licenses
        #url_entertainmentL = 'https://data.cityofboston.gov/resource/cz6t-w69j.json'
        #response_entertainmentL = urllib.request.urlopen(url_entertainmentL).read().decode("utf-8")
        client = sodapy.Socrata("data.cityofboston.gov", dml.auth['services']['cityofbostondataportal']['token'])
        r = client.get("cz6t-w69j", limit=5223)#5223
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("Entertainment_Licenses")
        repo.createCollection("Entertainment_Licenses")
        repo['bohan_nyx_xh1994_yiran123.Entertainment_Licenses'].insert_many(r)

        #mbta stops location
        '''url_stopbylocation = 'http://realtime.mbta.com/developer/api/v2/stopsbylocation'#?api_key=wX9NwuHnZU2ToO7GmGR9uw&lat=42.346961&lon=-71.076640&format=json'
        api_key_mbta = 'wX9NwuHnZU2ToO7GmGR9uw'
        mbta_api_key = dml.auth['services']['mbtadeveloperportal']['key']
        response_stopbylocation = requests.get(url_stopbylocation + '?api_key=' + mbta_api_key + '&route=' + route)
        r = json.loads(response_stopbylocation)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("mbta_stop_by_location")
        repo.createCollection("mbta_stop_by_location")
        repo['bohan_nyx_xh1994_yiran123.mbta_stop_by_location'].insert_many(r)'''
        #the lat and long is [4]and[5],.....stopsbylocation return a list of the stops nearest a particular location

        #TRAFFIC SIGNALS
        '''url_traffic_signal = 'http://bostonopendata-boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_0.geojson'
        response_traffic_signal = urllib.request.urlopen(url_traffic_signal).read().decode("utf-8")
        r = json.loads(response_traffic_signal)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("TRAFFIC_SIGNALS")
        repo.createCollection("TRAFFIC_SIGNALS")
        repo['bohan_nyx_xh1994_yiran123.TRAFFIC_SIGNALS'].insert(r)'''
        

        url_airbnb = 'http://datamechanics.io/data/bohan_xh1994/airbnb.json'
        response_airbnb_rating = urllib.request.urlopen(url_airbnb).read().decode("utf-8")
        r = json.loads(response_airbnb_rating)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("airbnb_rating")
        repo.createCollection("airbnb_rating")
        repo['bohan_nyx_xh1994_yiran123.airbnb_rating'].insert(r)
        

        url_MBTA_Bus_stops = 'http://datamechanics.io/data/wuhaoyu_yiran123/MBTA_Bus_Stops.geojson'
        response_MBTA_Bus_stops = urllib.request.urlopen(url_MBTA_Bus_stops).read().decode("utf-8")
        r = json.loads(response_MBTA_Bus_stops)
        s = json.dumps(r, sort_keys=True, indent=2)
        repo.dropCollection("MBTA_Bus_stops")
        repo.createCollection("MBTA_Bus_stops")
        repo['bohan_nyx_xh1994_yiran123.MBTA_Bus_stops'].insert(r)
        repo.logout()


        endTime = datetime.datetime.now()

        return {"start":startTime, "end":endTime}
        
    @staticmethod
    def provenance(doc = prov.model.ProvDocument(), startTime = None, endTime = None):
        
            #    Create the provenance document describing everything happening
             #   in this script. Each run of the script will generate a new
              #  document describing that invocation event.''

            # Set up the database connection.
            client = dml.pymongo.MongoClient()
            repo = client.repo
            repo.authenticate('bohan_nyx_xh1994_yiran123', 'bohan_nyx_xh1994_yiran123')
            doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
            doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
            doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
            doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
            doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')
            doc.add_namespace('mbta', 'http://realtime.mbta.com/developer/api/v2/')
            #doc.add_namespace('analyzeBoston', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/')
            doc.add_namespace('airbnbr','http://datamechanics.io/?prefix=bohan_xh1994/')
            doc.add_namespace('MBTAbusr','http://datamechanics.io/data/?prefix=wuhaoyu_yiran123/')

            this_script = doc.agent('alg:bohan_nyx_xh1994_yiran123#retrievedatasets', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
            
            crime_resource = doc.entity('bdp:ufcx-3fdn', {'prov:label':'crime', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
            get_crime = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            #get_property = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.wasAssociatedWith(get_crime, this_script)
            #doc.wasAssociatedWith(get_lost, this_script)
            doc.usage(get_crime, crime_resource, startTime, None,
              {prov.model.PROV_TYPE:'ont:Retrieval'
                      #'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
                      }
                      )
            
            food_EI_resource = doc.entity('bdp:427a-3cn5', {'prov:label':'foodEI', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
            get_foodEI = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.usage(get_foodEI, food_EI_resource, startTime, None,
              {prov.model.PROV_TYPE:'ont:Retrieval'
                      #'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                      }
                      )

            Afood_EL_resource = doc.entity('bdp:fdxy-gydq', {'prov:label':'foodEL', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
            get_AfoodEL = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.usage(get_AfoodEL, Afood_EL_resource, startTime, None,
              {prov.model.PROV_TYPE:'ont:Retrieval'
                      #'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                      }
                      )            

            entertainmentL_resource = doc.entity('bdp:cz6t-w69j', {'prov:label':'entertainmentL', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
            get_entertainmentL = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.usage(get_entertainmentL, entertainmentL_resource, startTime, None,
              {prov.model.PROV_TYPE:'ont:Retrieval'
                      #'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                      }
                      )

            '''traffic_signal_resource = doc.entity('analyzeBoston:de08c6fe69c942509089e6db98c716a3_0', {'prov:label':'traffic_signal', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
            get_traffic_signal = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.usage(get_traffic_signal, traffic_signal_resource, startTime, None,
              {prov.model.PROV_TYPE:'ont:Retrieval'
                      #'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                      }
                      )'''
            airbnb_resource = doc.entity('airbnbr:airbnb', {'prov:label':'airbnb_rating', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
            get_airbnb = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.usage(get_airbnb, airbnb_resource, startTime, None,
              {prov.model.PROV_TYPE:'ont:Retrieval'
                      #'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                      }
                      )

            MBTAbus_resource = doc.entity('MBTAbusr:MBTAbus', {'prov:label':'MBTA_Bus_stops', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'geojson'})
            get_MBTAbus = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
            doc.usage(get_MBTAbus, MBTAbus_resource, startTime, None,
              {prov.model.PROV_TYPE:'ont:Retrieval'
                      #'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
                      }
                      )  

            crime = doc.entity('dat:bohan_nyx_xh1994_yiran123#crime', {prov.model.PROV_LABEL:'crime incident data', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(crime, this_script)
            doc.wasGeneratedBy(crime, get_crime, endTime)
            doc.wasDerivedFrom(crime, crime_resource, get_crime, get_crime, get_crime)

            foodEI = doc.entity('dat:bohan_nyx_xh1994_yiran123#foodEI', {prov.model.PROV_LABEL:'food establishment inspection', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(foodEI, this_script)
            doc.wasGeneratedBy(foodEI, get_foodEI, endTime)
            doc.wasDerivedFrom(foodEI, food_EI_resource, get_foodEI, get_foodEI, get_foodEI)

            AfoodEL = doc.entity('dat:bohan_nyx_xh1994_yiran123#AfoodEL', {prov.model.PROV_LABEL:'Active Food Establishment Licenses', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(AfoodEL, this_script)
            doc.wasGeneratedBy(AfoodEL, get_AfoodEL, endTime)
            doc.wasDerivedFrom(AfoodEL, Afood_EL_resource, get_AfoodEL, get_AfoodEL, get_AfoodEL)

            entertainmentL = doc.entity('dat:bohan_nyx_xh1994_yiran123#entertainmentL', {prov.model.PROV_LABEL:'Entertainment License', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(entertainmentL, this_script)
            doc.wasGeneratedBy(entertainmentL, get_entertainmentL, endTime)
            doc.wasDerivedFrom(entertainmentL, entertainmentL_resource, get_entertainmentL, get_entertainmentL, get_entertainmentL)

            '''traffic_signal = doc.entity('dat:bohan_nyx_xh1994_yiran123#traffic_signal', {prov.model.PROV_LABEL:'Traffic Signal', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(traffic_signal, this_script)
            doc.wasGeneratedBy(traffic_signal, get_traffic_signal, endTime)
            doc.wasDerivedFrom(traffic_signal, traffic_signal_resource, get_traffic_signal, get_traffic_signal, get_traffic_signal)
            '''
            
            airbnb_rating = doc.entity('dat:bohan_nyx_xh1994_yiran123#airbnb_rating', {prov.model.PROV_LABEL:'Airbnb Rating', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(airbnb_rating, this_script)
            doc.wasGeneratedBy(airbnb_rating, get_airbnb, endTime)
            doc.wasDerivedFrom(airbnb_rating, airbnb_resource, airbnb_rating, airbnb_rating, airbnb_rating)

            MBTA_Bus_stops = doc.entity('dat:bohan_nyx_xh1994_yiran123#MBTA_Bus_stops', {prov.model.PROV_LABEL:'MBTA Bus Stops', prov.model.PROV_TYPE:'ont:DataSet'})
            doc.wasAttributedTo(MBTA_Bus_stops, this_script)
            doc.wasGeneratedBy(MBTA_Bus_stops, get_MBTAbus, endTime)
            doc.wasDerivedFrom(MBTA_Bus_stops, MBTAbus_resource, MBTA_Bus_stops, MBTA_Bus_stops, MBTA_Bus_stops)



            repo.logout()
            
            return doc

retrievedatasets.execute()
doc = retrievedatasets.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
