'''
Pauline Ramirez & Carlos Syquia
correlation3.py

Calculates the correlation between sleep rates and proximity to a university

'''


import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import sodapy
from geopy.distance import vincenty
from geopy.geocoders import Nominatim
import scipy.stats

class correlation3(dml.Algorithm):
    contributor = 'pgr_syquiac'
    reads = ['pgr_syquiac.obesity_pools_stores']
    writes = ['pgr_syquiac.pools_obesity']

    ''' Example data point
    {'state': 'MA', 'address': '1010 Bennington St', 
    'obesity_rates': [{'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.02862, 42.342924], 'type': 'Point'}, 
    '_id': ObjectId('58e3add90d2c30b6b266f776'), 'measureid': 'OBESITY', 'cityname': 'Boston', 
    'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 
    'data_value_unit': '%', 'low_confidence_limit': '15.4', 'cityfips': '2507000', 'high_confidence_limit': 
    '18.6', 'datavaluetypeid': 'CrdPrv', 'populationcount': '207', 'year': '2014', 'datasource': 'BRFSS', 
    'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025981202', 
    'category': 'Unhealthy Behaviors', 'data_value': '17.3', 'uniqueid': '2507000-25025981202', 
    'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': 
    {'coordinates': [-71.036948, 42.333467], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b266f781'), 
    'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 
    'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '18.6', 'cityfips': '2507000', 'high_confidence_limit': '21.9', 'datavaluetypeid': 'CrdPrv', 'populationcount': '3076', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025060301', 'category': 'Unhealthy Behaviors', 'data_value': '20.2', 'uniqueid': '2507000-25025060301', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.035758, 42.377578], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b266f8aa'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '26.9', 'cityfips': '2507000', 'high_confidence_limit': '29.5', 'datavaluetypeid': 'CrdPrv', 'populationcount': '5231', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025050200', 'category': 'Unhealthy Behaviors', 'data_value': '28.3', 'uniqueid': '2507000-25025050200', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.037902, 42.368218], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b266f8c1'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '26.1', 'cityfips': '2507000', 'high_confidence_limit': '28.6', 'datavaluetypeid': 'CrdPrv', 'populationcount': '2372', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025050400', 'category': 'Unhealthy Behaviors', 'data_value': '27.4', 'uniqueid': '2507000-25025050400', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.038213, 42.336868], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b266facf'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '16.4', 'cityfips': '2507000', 'high_confidence_limit': '19.4', 'datavaluetypeid': 'CrdPrv', 'populationcount': '3571', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025060501', 'category': 'Unhealthy Behaviors', 'data_value': '17.7', 'uniqueid': '2507000-25025060501', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.042898, 42.332399], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b266fb0f'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '17.2', 'cityfips': '2507000', 'high_confidence_limit': '20.2', 'datavaluetypeid': 'CrdPrv', 'populationcount': '4904', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025060400', 'category': 'Unhealthy Behaviors', 'data_value': '18.6', 'uniqueid': '2507000-25025060400', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.015725, 42.387252], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b266fb46'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '24.3', 'cityfips': '2507000', 'high_confidence_limit': '26.3', 'datavaluetypeid': 'CrdPrv', 'populationcount': '4089', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025051000', 'category': 'Unhealthy Behaviors', 'data_value': '25.3', 'uniqueid': '2507000-25025051000', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.042979, 42.372363], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b266fcf4'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '30.4', 'cityfips': '2507000', 'high_confidence_limit': '32.3', 'datavaluetypeid': 'CrdPrv', 'populationcount': '2251', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025050300', 'category': 'Unhealthy Behaviors', 'data_value': '31.4', 'uniqueid': '2507000-25025050300', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.033924, 42.363306], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b266fe37'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '23.4', 'cityfips': '2507000', 'high_confidence_limit': '25.7', 'datavaluetypeid': 'CrdPrv', 'populationcount': '2379', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025051200', 'category': 'Unhealthy Behaviors', 'data_value': '24.6', 'uniqueid': '2507000-25025051200', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.042451, 42.34911], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b2670013'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '16.3', 'cityfips': '2507000', 'high_confidence_limit': '18.1', 'datavaluetypeid': 'CrdPrv', 'populationcount': '2357', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025060600', 'category': 'Unhealthy Behaviors', 'data_value': '17.1', 'uniqueid': '2507000-25025060600', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.029798, 42.382372], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b2670087'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '27.1', 'cityfips': '2507000', 'high_confidence_limit': '29.7', 'datavaluetypeid': 'CrdPrv', 'populationcount': '4165', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025050901', 'category': 'Unhealthy Behaviors', 'data_value': '28.5', 'uniqueid': '2507000-25025050901', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.01224, 42.363453], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b267010b'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '18.9', 'cityfips': '2507000', 'high_confidence_limit': '21.7', 'datavaluetypeid': 'CrdPrv', 'populationcount': '389', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025981300', 'category': 'Unhealthy Behaviors', 'data_value': '20.2', 'uniqueid': '2507000-25025981300', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.039617, 42.381457], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b2670190'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '27.5', 'cityfips': '2507000', 'high_confidence_limit': '29.9', 'datavaluetypeid': 'CrdPrv', 'populationcount': '5115', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025050101', 'category': 'Unhealthy Behaviors', 'data_value': '28.8', 'uniqueid': '2507000-25025050101', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.033271, 42.375358], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b267044c'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '27', 'cityfips': '2507000', 'high_confidence_limit': '29.7', 'datavaluetypeid': 'CrdPrv', 'populationcount': '4504', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025050700', 'category': 'Unhealthy Behaviors', 'data_value': '28.5', 'uniqueid': '2507000-25025050700', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.03727, 42.371888], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b26704c4'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '29.6', 'cityfips': '2507000', 'high_confidence_limit': '32.1', 'datavaluetypeid': 'CrdPrv', 'populationcount': '2063', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025050600', 'category': 'Unhealthy Behaviors', 'data_value': '30.9', 'uniqueid': '2507000-25025050600', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-70.965134, 42.328663], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b2670600'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '46.2', 'cityfips': '2507000', 'high_confidence_limit': '48.4', 'datavaluetypeid': 'CrdPrv', 'populationcount': '535', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025980101', 'category': 'Unhealthy Behaviors', 'data_value': '47.3', 'uniqueid': '2507000-25025980101', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.03391, 42.369777], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b2670655'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '26.9', 'cityfips': '2507000', 'high_confidence_limit': '29.2', 'datavaluetypeid': 'CrdPrv', 'populationcount': '1857', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025050500', 'category': 'Unhealthy Behaviors', 'data_value': '28.1', 'uniqueid': '2507000-25025050500', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.052071, 42.379054], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b2670696'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '22.5', 'cityfips': '2507000', 'high_confidence_limit': '25.1', 'datavaluetypeid': 'CrdPrv', 'populationcount': '3900', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025040801', 'category': 'Unhealthy Behaviors', 'data_value': '23.8', 'uniqueid': '2507000-25025040801', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.004026, 42.385639], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b267080c'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '25', 'cityfips': '2507000', 'high_confidence_limit': '27', 'datavaluetypeid': 'CrdPrv', 'populationcount': '6093', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025051101', 'category': 'Unhealthy Behaviors', 'data_value': '26.1', 'uniqueid': '2507000-25025051101', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.02793, 42.334762], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b2670858'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '17.6', 'cityfips': '2507000', 'high_confidence_limit': '20.5', 'datavaluetypeid': 'CrdPrv', 'populationcount': '3216', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025060101', 'category': 'Unhealthy Behaviors', 'data_value': '19', 'uniqueid': '2507000-25025060101', 'geographiclevel': 'Census Tract'}, {'statedesc': 'Massachusetts', 'stateabbr': 'MA', 'geolocation': {'coordinates': [-71.03268, 42.332558], 'type': 'Point'}, '_id': ObjectId('58e3add90d2c30b6b26709a0'), 'measureid': 'OBESITY', 'cityname': 'Boston', 'measure': 'Obesity among adults aged >=18 Years', 'data_value_type': 'Crude prevalence', 'data_value_unit': '%', 'low_confidence_limit': '18', 'cityfips': '2507000', 'high_confidence_limit': '21.5', 'datavaluetypeid': 'CrdPrv', 'populationcount': '1916', 'year': '2014', 'datasource': 'BRFSS', 'categoryid': 'UNHBEH', 'short_question_text': 'Obesity', 'tractfips': '25025060200', 'category': 'Unhealthy Behaviors', 'data_value': '19.7', 'uniqueid': '2507000-25025060200', 'geographiclevel': 'Census Tract'}], 'coordinates': '42.3868595,-71.0063494', 'closest_pools': [{'ward': '6', 'st_no': '653', 'st_name': 'Summer', 'gps_x': '779574.5062', 'license_cat': 'SY', '_id': ObjectId('58e3adde0d2c30b6b2672909'), 'business_name': 'Boston Athletic Club', 'location_1_zip': '02210', 'suffix': 'ST', 'location_1_location': 'Summer', 'coordinates': [(-71.0376569833528, 42.3432453173255)], 'gps_y': '2949185.659', 'location_1_city': 'BOSTON', 'location_1': {'coordinates': [2949185.659, 779574.5062], 'type': 'Point'}, 'descript': 'Swimming Pools-Year Round'}, {'ward': '1', 'st_no': '101', 'st_name': 'Harborside', 'gps_x': '783955.1893', 'license_cat': 'SY', '_id': ObjectId('58e3adde0d2c30b6b267290c'), 'business_name': 'Harborside Hyatt Hotel (Swim)', 'location_1_zip': '02128', 'suffix': 'DR', 'location_1_location': 'Harborside', 'coordinates': [(-71.0272123267471, 42.35920815)], 'gps_y': '2956275.31', 'location_1_city': 'East Boston', 'location_1': {'coordinates': [2956275.31, 783955.1893], 'type': 'Point'}, 'descript': 'Swimming Pools-Year Round'}, {'ward': '6', 'st_no': '1', 'st_name': 'Seaport', 'gps_x': '780041.7243', 'license_cat': 'SY', '_id': ObjectId('58e3adde0d2c30b6b2672914'), 'business_name': 'Seaport Hotel', 'location_1_zip': '02210', 'suffix': 'LA', 'location_1_location': 'Seaport', 'coordinates': [(-71.0415198028004, 42.3491401)], 'gps_y': '2952652.421', 'location_1_city': 'BOSTON', 'location_1': {'coordinates': [2952652.421, 780041.7243], 'type': 'Point'}, 'descript': 'Swimming Pools-Year Round'}, {'ward': '1', 'st_no': '85', 'st_name': 'TERMINAL', 'gps_x': '785909', 'license_cat': 'SY', '_id': ObjectId('58e3adde0d2c30b6b2672918'), 'business_name': 'Boston Hilton (Swim)', 'location_1_zip': '02128', 'suffix': ' ', 'location_1_location': 'TERMINAL', 'coordinates': [(-71.020431, 42.3660988)], 'gps_y': '2959710', 'location_1_city': 'EAST BOSTON', 'location_1': {'coordinates': [2959710, 785909], 'type': 'Point'}, 'descript': 'Swimming Pools-Year Round'}, {'ward': '1', 'st_no': '207', 'st_name': 'Porter', 'gps_x': '782705.4375', 'license_cat': 'SY', '_id': ObjectId('58e3adde0d2c30b6b2672927'), 'business_name': 'EMBASSY SUITES BOSTON (SY)', 'location_1_zip': '02128', 'suffix': 'ST', 'location_1_location': 'Porter', 'coordinates': [(-71.031742, 42.37043)], 'gps_y': '2960363.5', 'location_1_city': 'East Boston', 'location_1': {'coordinates': [2960363.5, 782705.4375], 'type': 'Point'}, 'descript': 'Swimming Pools-Year Round'}, {'ward': '1', 'st_no': '207', 'st_name': 'Porter', 'gps_x': '782705.4375', 'license_cat': 'SY', '_id': ObjectId('58e3adde0d2c30b6b2672928'), 'business_name': 'EMBASSY SUITES BOSTON (SP)', 'location_1_zip': '02128', 'suffix': 'ST', 'location_1_location': 'Porter', 'coordinates': [(-71.031742, 42.37043)], 'gps_y': '2960363.5', 'location_1_city': 'East Boston', 'location_1': {'coordinates': [2960363.5, 782705.4375], 'type': 'Point'}, 'descript': 'Swimming Pools-Year Round'}, {'dba_name': 'Special Purpose Pool', 'ward': '1', 'st_no': '85', 'st_name': 'TERMINAL', 'gps_x': '785909', 'license_cat': 'SY', '_id': ObjectId('58e3adde0d2c30b6b267292b'), 'business_name': 'HILTON BOSTON LOGAN AIRPORT', 'location_1_zip': '02128', 'suffix': ' ', 'location_1_location': 'TERMINAL', 'coordinates': [(-71.020431, 42.3660988)], 'gps_y': '2959710', 'location_1_city': 'EAST BOSTON', 'location_1': {'coordinates': [2959710, 785909], 'type': 'Point'}, 'descript': 'Swimming Pools-Year Round'}, {'ward': '6', 'suffix': 'ST', 'st_no': '425', 'coordinates': [(-71.0431625, 42.3462734)], 'st_name': 'Summer', 'descript': 'Swimming Pools-Year Round', 'license_cat': 'SY', '_id': ObjectId('58e3adde0d2c30b6b267293c'), 'business_name': 'The Westin Boston Waterfront Swimming Pool'}, {'ward': '6', 'suffix': 'ST', 'st_no': '425', 'coordinates': [(-71.0431625, 42.3462734)], 'st_name': 'Summer', 'descript': 'Swimming Pools-Year Round', 'license_cat': 'SY', '_id': ObjectId('58e3adde0d2c30b6b267293d'), 'business_name': 'The Westin Boston Waterfront Special Purpose Pool'}, {'ward': '6', 'st_no': '653', 'st_name': 'Summer', 'gps_x': '779574.5062', 'license_cat': 'SY', '_id': ObjectId('58e3adde0d2c30b6b267293f'), 'business_name': 'Boston Athletic Club', 'location_1_zip': '02210', 'suffix': 'ST', 'location_1_location': 'Summer', 'coordinates': [(-71.0376569833528, 42.3432453173255)], 'gps_y': '2949185.659', 'location_1_city': 'BOSTON', 'location_1': {'coordinates': [2949185.659, 779574.5062], 'type': 'Point'}, 'descript': 'Swimming Pools-Year Round'}, {'ward': '1', 'st_no': '101', 'st_name': 'Harborside', 'gps_x': '783955.1893', 'license_cat': 'SY', '_id': ObjectId('58e3adde0d2c30b6b2672952'), 'business_name': 'Harborside Hyatt Hotel (Spa)', 'location_1_zip': '02128', 'suffix': 'DR', 'location_1_location': 'Harborside', 'coordinates': [(-71.0272123267471, 42.35920815)], 'gps_y': '2956275.31', 'location_1_city': 'East Boston', 'location_1': {'coordinates': [2956275.31, 783955.1893], 'type': 'Point'}, 'descript': 'Swimming Pools-Year Round'}, {'ward': '1', 'st_no': '225', 'st_name': 'William F Mcclellan', 'gps_x': '786771', 'license_cat': 'SY', '_id': ObjectId('58e3adde0d2c30b6b2672953'), 'business_name': 'Courtyard By Marriott (Swim Pool)', 'location_1_zip': '02128', 'suffix': 'HW', 'location_1_location': 'William F Mcclellan', 'coordinates': [(-71.0187375170014, 42.387059736343)], 'gps_y': '2966510', 'location_1_city': 'EAST BOSTON', 'location_1': {'coordinates': [2966510, 786771], 'type': 'Point'}, 'descript': 'Swimming Pools-Year Round'}, {'ward': '1', 'st_no': '225', 'st_name': 'William F Mcclellan', 'gps_x': '786771', 'license_cat': 'SY', '_id': ObjectId('58e3adde0d2c30b6b2672954'), 'business_name': 'Courtyard By Marriott (Sp. Purpose Pool)', 'location_1_zip': '02128', 'suffix': 'HW', 'location_1_location': 'William F Mcclellan', 'coordinates': [(-71.0187375170014, 42.387059736343)], 'gps_y': '2966510', 'location_1_city': 'EAST BOSTON', 'location_1': {'coordinates': [2966510, 786771], 'type': 'Point'}, 'descript': 'Swimming Pools-Year Round'}, {'ward': '1', 'st_no': '225', 'st_name': 'William F Mcclellan', 'gps_x': '786771', 'license_cat': 'SY', '_id': ObjectId('58e3adde0d2c30b6b267295d'), 'business_name': 'Courtyard by Marriott (Swimming Pool)', 'location_1_zip': '02128', 'suffix': 'HW', 'location_1_location': 'William F Mcclellan', 'coordinates': [(-71.0187375170014, 42.387059736343)], 'gps_y': '2966510', 'location_1_city': 'EAST BOSTON', 'location_1': {'coordinates': [2966510, 786771], 'type': 'Point'}, 'descript': 'Swimming Pools-Year Round'}, {'ward': '1', 'st_no': '225', 'st_name': 'William F Mcclellan', 'gps_x': '786771', 'license_cat': 'SY', '_id': ObjectId('58e3adde0d2c30b6b267295e'), 'business_name': 'Courtyard by Marriott (Special Purpose Pool)', 'location_1_zip': '02128', 'suffix': 'HW', 'location_1_location': 'William F Mcclellan', 'coordinates': [(-71.0187375170014, 42.387059736343)], 'gps_y': '2966510', 'location_1_city': 'EAST BOSTON', 'location_1': {'coordinates': [2966510, 786771], 'type': 'Point'}, 'descript': 'Swimming Pools-Year Round'}], 'zip': '2128', 'area': 'East Boston', 'location_location': '1010 Bennington St', '_id': ObjectId('58e3addf0d2c30b6b2672961'), 'store': 'El Paisa Butchery', 'location': {'coordinates': [-71.02881000038104, 42.378460000328914], 'type': 'Point'}}

    '''

    @staticmethod
    def execute(trial = False):
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('pgr_syquiac', 'pgr_syquiac')

        stores = repo.pgr_syquiac.obesity_pools_stores.find()
        # for i in stores:
        #     if len(i['obesity_rates']) > 0 and len(i['closest_pools']) > 0:
        #         print("rates: " + str(len(i['obesity_rates'])))
        #         print("pools: " + str(len(i['closest_pools'])))

        # Append tuples of (rate_of_obesity, num_pools)
        rates = []

        # Let the user choose the max distance from each pool
        distance = input("Please enter a distance in miles, or press enter to observe all data points: ")

        if not distance == '':
        	distance = float(distance)
        	print("Observing data points within a " + str(distance) + " mile distance of their nearest pools...")


        # keep track of data points being added if trial is on
        count = 0
        # get the distance of each data point from their closest university
        # Create a data structure of the form (num_pools, obesity rate)
        for i in stores:
            if trial and count > 1000:
                break
            else:
                count += 1 
                # Skip over points that don't have obesity rates
                if len(i['obesity_rates']) > 0 and len(i['closest_pools']) > 0:
                    for x in i['obesity_rates']:
                        pools = 0
                        for y in i['closest_pools']:
                            # Find the distance between each data point and all the pools
                            d = vincenty(x['geolocation']['coordinates'], y['coordinates']).miles
                            if distance == '':
                                pools += 1

                            else:
                                if d < distance:
                                    pools += 1
                        if 'data_value' in x:
                            rates.append({'num_pools': pools, 'obesity_rate': float(x['data_value'])})
                                
                            

        # print(rates)

        repo.dropPermanent("pools_obesity")
        repo.createPermanent("pools_obesity")
        repo['pgr_syquiac.pools_obesity'].insert_many(rates)
        print("Inserted new collection!")


        print("Calculating correlation coefficient and p-value...")
        x = []
        y = []
        # z = []
        for i in rates:
        	x.append(i['obesity_rate'])
        	y.append(i['num_pools'])


        math = scipy.stats.pearsonr(x, y)
        print("Correlation coefficient is " + str(math[0]))
        print("P-value is " + str(math[1]))


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
        repo.authenticate('pgr_syquiac', 'pgr_syquiac')

        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

        this_script = doc.agent(
            'alg:pgr_syquiac#correlation3',
            {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'}
        )
        resourceSleepUniversities = doc.entity(
            'dat:pgr_syquiac#sleepUniversities',
            {'prov:label':'Sleep Universities', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        this_run = doc.activity(
            'log:a'+str(uuid.uuid4()), startTime, endTime,
            {prov.model.PROV_TYPE:'ont:Computation'}
        )
        doc.wasAssociatedWith(this_run, this_script)
        doc.used(this_run, resourceSleepUniversities, startTime)

        correlationSleepUniversities = doc.entity(
            'dat:pgr_syquiac#correlation3',
            {prov.model.PROV_LABEL:'Correlation Sleep Universities', prov.model.PROV_TYPE:'ont:DataSet'}
        )
        doc.wasAttributedTo(correlationSleepUniversities, this_script)
        doc.wasGeneratedBy(correlationSleepUniversities, this_run, endTime)
        doc.wasDerivedFrom(correlationSleepUniversities, resourceSleepUniversities, this_run, this_run, this_run)

        repo.logout()

        return doc

correlation3.execute()
# doc = correlation3.provenance()
# print(doc.get_provn())
# print(json.dumps(json.loads(doc.serialize()), indent=4))
