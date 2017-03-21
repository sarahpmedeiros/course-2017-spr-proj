# retrieveData.py
# retrieve raw data from online data portal

import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
#import sodapy

class retrieveData(dml.Algorithm):
    contributor = 'echogu_wei0496'
    reads = []
    writes = ['echogu_wei0496.buses', 'echogu_wei0496.grade_safe_distance', 'echogu_wei0496.safety_scores', 'echogu_wei0496.schools', 'echogu_wei0496.students']

    @staticmethod
    def execute(trial=False):
        ''' Retrieve some data sets.
        '''
        startTime = datetime.datetime.now()

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('echogu_wei0496', 'echogu_wei0496')

        # buses
        url = 'http://datamechanics.io/data/_bps_transportation_challenge/buses.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("buses")
        repo.createCollection("buses")
        repo['echogu_wei0496.buses'].insert_many(r)
        repo['echogu_wei0496.buses'].metadata({'complete': True})
        print(repo['echogu_wei0496.buses'].metadata())

        # grade-safe-distance
        url = 'http://datamechanics.io/data/_bps_transportation_challenge/grade-safe-distance.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("grade_safe_distance")
        repo.createCollection("grade_safe_distance")
        repo['echogu_wei0496.grade_safe_distance'].insert(r)
        repo['echogu_wei0496.grade_safe_distance'].metadata({'complete': True})
        print(repo['echogu_wei0496.grade_safe_distance'].metadata())

        # safety-scores
        url = 'http://datamechanics.io/data/_bps_transportation_challenge/safety-scores.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("safety_scores")
        repo.createCollection("safety_scores")
        repo['echogu_wei0496.safety_scores'].insert_many(r)
        repo['echogu_wei0496.safety_scores'].metadata({'complete': True})
        print(repo['echogu_wei0496.safety_scores'].metadata())

        # schools
        url = 'http://datamechanics.io/data/_bps_transportation_challenge/schools.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("schools")
        repo.createCollection("schools")
        repo['echogu_wei0496.schools'].insert_many(r)
        repo['echogu_wei0496.schools'].metadata({'complete': True})
        print(repo['echogu_wei0496.schools'].metadata())

        # students
        url = 'http://datamechanics.io/data/_bps_transportation_challenge/students.json'
        response = urllib.request.urlopen(url).read().decode("utf-8")
        r = json.loads(response)
        repo.dropCollection("students")
        repo.createCollection("students")
        repo['echogu_wei0496.students'].insert_many(r)
        repo['echogu_wei0496.students'].metadata({'complete': True})
        print(repo['echogu_wei0496.students'].metadata())

        repo.logout()

        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}

    @staticmethod
    def provenance(doc=prov.model.ProvDocument(), startTime=None, endTime=None):
        ''' Create the provenance document describing everything happening
            in this script. Each run of the script will generate a new
            document describing that invocation event.
        '''

        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('echogu_wei0496', 'echogu_wei0496')

        # create document object and define namespaces
        doc.add_namespace('alg', 'http://datamechanics.io/algorithm/')  # The scripts are in <folder>#<filename> format.
        doc.add_namespace('dat', 'http://datamechanics.io/data/')  # The data sets are in <user>#<collection> format.
        doc.add_namespace('ont', 'http://datamechanics.io/ontology#')  # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
        doc.add_namespace('log', 'http://datamechanics.io/log/')  # The event log.
        doc.add_namespace('bps', 'http://datamechanics.io/data/_bps_transportation_challenge/')
        # doc.add_namespace('bod', 'http://bostonopendata-boston.opendata.arcgis.com/datasets/') # City of Boston Maps Open Data
        # doc.add_namespace('cdp', 'https://data.cambridgema.gov/resource/') # City of Cambridge Cambridge Open Data Portal
        # doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/') # City Of Boston Data Portal
        # doc.add_namespace('mod', 'http://maps-massgis.opendata.arcgis.com/datasets/') #MassGIS Open Data

        # define entity to represent resources
        this_script = doc.agent('alg:echogu_wei0496#retrieveData', {prov.model.PROV_TYPE: prov.model.PROV['SoftwareAgent'], 'ont:Extension': 'py'})
        resource_buses = doc.entity('bps:buses', {'prov:label': 'buses', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        resource_grade_safe_distance = doc.entity('bps:grade_safe_distance', {'prov:label': 'grade_safe_distance', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        resource_safety_scores = doc.entity('bps:safety_scores', {'prov:label': 'safety_scores', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        resource_schools = doc.entity('bps:schools', {'prov:label': 'schools', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})
        resource_students = doc.entity('bps:students', {'prov:label': 'students', prov.model.PROV_TYPE: 'ont:DataResource', 'ont:Extension': 'json'})

        # define activity to represent invocation of the script
        get_buses = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_grade_safe_distance  = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_safety_scores = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_schools = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)
        get_students = doc.activity('log:uuid' + str(uuid.uuid4()), startTime, endTime)

        # associate the activity with the script
        doc.wasAssociatedWith(get_buses, this_script)
        doc.wasAssociatedWith(get_grade_safe_distance, this_script)
        doc.wasAssociatedWith(get_safety_scores, this_script)
        doc.wasAssociatedWith(get_schools, this_script)
        doc.wasAssociatedWith(get_students, this_script)

        # indicate that an activity used the entity
        doc.usage(get_buses, resource_buses, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_grade_safe_distance, resource_grade_safe_distance, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_safety_scores, resource_safety_scores, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_schools, resource_schools, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})
        doc.usage(get_students, resource_students, startTime, None, {prov.model.PROV_TYPE: 'ont:Retrieval'})

        # for the data obtained, indicate that the entity was attributed to what agent, was generated by which activity and was derived from what entity
        buses = doc.entity('dat:echogu_wei0496#buses', {prov.model.PROV_LABEL: 'buses', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(buses, this_script)
        doc.wasGeneratedBy(buses, get_buses, endTime)
        doc.wasDerivedFrom(buses, resource_buses, get_buses, get_buses, get_buses)

        grade_safe_distance = doc.entity('dat:echogu_wei0496#grade_safe_distance', {prov.model.PROV_LABEL: 'grade_safe_distance', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(grade_safe_distance, this_script)
        doc.wasGeneratedBy(grade_safe_distance, get_grade_safe_distance, endTime)
        doc.wasDerivedFrom(grade_safe_distance, resource_grade_safe_distance, resource_grade_safe_distance, resource_grade_safe_distance, resource_grade_safe_distance)

        safety_scores = doc.entity('dat:echogu_wei0496#safety_scores', {prov.model.PROV_LABEL: 'safety_scores', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(safety_scores, this_script)
        doc.wasGeneratedBy(safety_scores, get_safety_scores, endTime)
        doc.wasDerivedFrom(safety_scores, resource_safety_scores, get_safety_scores, get_safety_scores, get_safety_scores)

        schools = doc.entity('dat:echogu_wei0496#schools', {prov.model.PROV_LABEL: 'schools', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(schools, this_script)
        doc.wasGeneratedBy(schools, get_schools, endTime)
        doc.wasDerivedFrom(schools, resource_schools, get_schools, get_schools, get_schools)

        students = doc.entity('dat:echogu_wei0496#students', {prov.model.PROV_LABEL: 'students', prov.model.PROV_TYPE: 'ont:DataSet'})
        doc.wasAttributedTo(students, this_script)
        doc.wasGeneratedBy(students, get_students, endTime)
        doc.wasDerivedFrom(students, resource_students, get_students, get_students, get_students)

        repo.logout()

        return doc

retrieveData.execute()
doc = retrieveData.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))

## eof
