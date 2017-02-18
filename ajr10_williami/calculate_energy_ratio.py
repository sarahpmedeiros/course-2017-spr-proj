import urllib.request
import sodapy
import json
import dml
import prov.model
import datetime
import uuid
import bson.code
from bson.json_util import dumps

class calculate_energy_ratio(dml.Algorithm):
    contributor = 'ajr10_williami'
    reads = ['ajr10_williami.area_spaces_cambridge',\
             'ajr10_williami.area_spaces_boston',\
             'ajr10_williami.cleaned_energy_cambridge',\
             'ajr10_williami.cleaned_energy_boston']
    writes = ['ajr10_williami.energy_ratio']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets and store in mongodb collections.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_williami', 'ajr10_williami')

        # Perform cleaning transformation here
        
        repo.dropCollection('ajr10_williami.energy_ratio')
        repo.createCollection('ajr10_williami.energy_ratio')

        open_spaces_cambridge = repo["ajr10_williami.area_spaces_cambridge"].find()
        open_spaces_boston = repo["ajr10_williami.area_spaces_boston"].find()
        energy_cambridge = repo["ajr10_williami.cleaned_energy_cambridge"].find()
        energy_boston = repo["ajr10_williami.cleaned_energy_boston"].find()

        energy_ratio_cambridge = {}
        energy_ratio_boston = {}
        energy_ratio_cambridge["total_open_space_cambridge"] = 0
        energy_ratio_boston["total_open_space_boston"] = 0
        energy_ratio_cambridge["total_CO2_cambridge"] = 0
        energy_ratio_boston["total_CO2_boston"] = 0
        energy_ratio_cambridge["total_mmbtu_cambridge"] = 0
        energy_ratio_boston["total_mmbtu_boston"] = 0

        # Known Data
        total_area_cambridge = 1.988 * pow(10,8) # 7.131 square miles
        total_area_boston = 2.4225 * pow(10,9) # 89.63 square miles
        population_cambridge = 107289 # as of 2013 census
        population_boston = 645966 # as of 2013 census

        for cambridge_open_space in open_spaces_cambridge:
            energy_ratio_cambridge["total_open_space_cambridge"] += float(cambridge_open_space['area'])

        for cambridge_energy in energy_cambridge:
            energy_ratio_cambridge["total_CO2_cambridge"] += float(cambridge_energy['CO2'])
            energy_ratio_cambridge["total_mmbtu_cambridge"] += float(cambridge_energy['mmbtu'])

        for boston_open_space in open_spaces_boston:
            energy_ratio_boston["total_open_space_boston"] += boston_open_space['area']

        for boston_energy in energy_boston:
            energy_ratio_boston["total_CO2_boston"] += float(boston_energy['CO2'])
            energy_ratio_boston["total_mmbtu_boston"] += float(boston_energy['mmbtu'])

        # Calculate ratios and weights
        energy_ratio_cambridge["open_space_ratio_cambridge"] = energy_ratio_cambridge["total_open_space_cambridge"] / total_area_cambridge
        energy_ratio_boston["open_space_ratio_boston"] = energy_ratio_boston["total_open_space_boston"] / total_area_boston
        energy_ratio_cambridge["CO2_per_resident_cambridge"] = energy_ratio_cambridge["total_CO2_cambridge"] / population_cambridge
        energy_ratio_boston["CO2_per_resident_boston"] = energy_ratio_boston["total_CO2_boston"] / population_boston
        energy_ratio_cambridge["mmbtu_per_resident_cambridge"] = energy_ratio_cambridge["total_mmbtu_cambridge"] / population_cambridge
        energy_ratio_boston["mmbtu_per_resident_boston"] = energy_ratio_boston["total_mmbtu_boston"] / population_boston

        repo['ajr10_williami.energy_ratio'].insert(energy_ratio_cambridge)
        repo['ajr10_williami.energy_ratio'].insert(energy_ratio_boston)

        # logout and return start and end times
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
        repo.authenticate('ajr10_williami', 'ajr10_williami')
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
        doc.add_namespace('awc', 'ajr10_williami')

        this_script = doc.agent('alg:ajr10_williami#calculate_energy_ratio', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        areas_cambridge_resource = doc.entity('awc:area_spaces_cambridge', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        areas_boston_resource = doc.entity('awc:area_spaces_boston', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        energy_cambridge_resource = doc.entity('awc:cleaned_energy_cambridge', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        energy_boston_resource = doc.entity('awc:cleaned_energy_boston', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_areas_cambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_areas_boston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_energy_cambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_energy_boston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_areas_cambridge, this_script)
        doc.wasAssociatedWith(get_areas_boston, this_script)
        doc.wasAssociatedWith(get_energy_cambridge, this_script)
        doc.wasAssociatedWith(get_energy_boston, this_script)

        doc.usage(get_areas_cambridge, areas_cambridge_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Get+Areas+Cambridge'
                  }
                  )
        doc.usage(get_areas_boston, areas_boston_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Get+Areas+Boston'
                  }
                  )
        doc.usage(get_energy_cambridge, energy_cambridge_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Get+Energy+Cambridge'
                  }
                  )
        doc.usage(get_energy_boston, energy_boston_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Get+Energy+Boston'
                  }
                  )

        energy_ratio = doc.entity('dat:ajr10_williami#energy_ratio', {prov.model.PROV_LABEL:'Energy Ratios', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(energy_ratio, this_script)
        doc.wasGeneratedBy(energy_ratio, get_areas_cambridge, endTime)
        doc.wasGeneratedBy(energy_ratio, get_areas_boston, endTime)
        doc.wasGeneratedBy(energy_ratio, get_energy_cambridge, endTime)
        doc.wasGeneratedBy(energy_ratio, get_energy_boston, endTime)
        doc.wasDerivedFrom(energy_ratio, areas_cambridge_resource, get_areas_cambridge, get_areas_cambridge, get_areas_cambridge)
        doc.wasDerivedFrom(energy_ratio, areas_boston_resource, get_areas_boston, get_areas_boston, get_areas_boston)
        doc.wasDerivedFrom(energy_ratio, energy_cambridge_resource, get_energy_cambridge, get_energy_cambridge, get_energy_cambridge)
        doc.wasDerivedFrom(energy_ratio, energy_boston_resource, get_energy_boston, get_energy_boston, get_energy_boston)

        repo.logout()

        return doc

calculate_energy_ratio.execute()

doc = calculate_energy_ratio.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
