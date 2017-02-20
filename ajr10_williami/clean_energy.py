import urllib.request
import sodapy
import json
import dml
import prov.model
import datetime
import uuid
import bson.code
from bson.json_util import dumps

class clean_energy(dml.Algorithm):
    contributor = 'ajr10_williami'
    reads = ['ajr10_williami.energy_cambridge',\
             'ajr10_williami.energy_boston']
    writes = ['ajr10_williami.cleaned_energy_cambridge',\
              'ajr10_williami.cleaned_energy_boston']

    @staticmethod
    def execute(trial = False):
        '''Retrieve some data sets and store in mongodb collections.'''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('ajr10_williami', 'ajr10_williami')

        # Perform cleaning transformation here
        
        repo.dropCollection('ajr10_williami.cleaned_energy_cambridge')
        repo.createCollection('ajr10_williami.cleaned_energy_cambridge')

        repo.dropCollection('ajr10_williami.cleaned_energy_boston')
        repo.createCollection('ajr10_williami.cleaned_energy_boston')

        energy_cambridge = repo['ajr10_williami.energy_cambridge'].find()
        energy_boston = repo['ajr10_williami.energy_boston'].find()

        for cambridge_energy in energy_cambridge:
            CO2 = cambridge_energy['co2_kg']
            mmbtu = cambridge_energy['use_mmbtu']

            new_energy = {}
            new_energy["CO2"] = CO2
            new_energy["mmbtu"] = mmbtu
            repo['ajr10_williami.cleaned_energy_cambridge'].insert(new_energy)

        for boston_energy in energy_boston:
            CO2 = boston_energy['emission_co2']
            mmbtu = boston_energy['use_mmbtu']

            new_energy = {}
            new_energy["CO2"] = CO2
            new_energy["mmbtu"] = mmbtu
            repo['ajr10_williami.cleaned_energy_boston'].insert(new_energy)

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

        this_script = doc.agent('alg:ajr10_williami#clean_energy', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})

        energy_cambridge_resource = doc.entity('awc:energy_cambridge', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
        energy_boston_resource = doc.entity('awc:energy_boston', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})

        get_energys_cambridge = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        get_energys_boston = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
        
        doc.wasAssociatedWith(get_energys_cambridge, this_script)
        doc.wasAssociatedWith(get_energys_boston, this_script)

        doc.usage(get_energys_cambridge, energy_cambridge_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Get+Energys+Cambridge'
                  }
                  )
        doc.usage(get_energys_boston, energy_boston_resource, startTime, None,
                  {prov.model.PROV_TYPE:'ont:Retrieval',
                  'ont:Query':'?type=Get+Energys+Boston'
                  }
                  )

        energys_cambridge = doc.entity('dat:ajr10_williami#cleaned_energy_cambridge', {prov.model.PROV_LABEL:'Energys Cambridge', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(energys_cambridge, this_script)
        doc.wasGeneratedBy(energys_cambridge, get_energys_cambridge, endTime)
        doc.wasDerivedFrom(energys_cambridge, energy_cambridge_resource, get_energys_cambridge, get_energys_cambridge, get_energys_cambridge)

        energys_boston = doc.entity('dat:ajr10_williami#cleaned_energy_boston', {prov.model.PROV_LABEL:'Energys Boston', prov.model.PROV_TYPE:'ont:DataSet'})
        doc.wasAttributedTo(energys_boston, this_script)
        doc.wasGeneratedBy(energys_boston, get_energys_boston, endTime)
        doc.wasDerivedFrom(energys_boston, energy_boston_resource, get_energys_boston, get_energys_boston, get_energys_boston)

        repo.logout()

        return doc

'''
clean_energy.execute()

doc = clean_energy.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''
## eof
