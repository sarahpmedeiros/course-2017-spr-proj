import urllib.request
import json
import dml
import prov.model
import datetime
import uuid
import statistics



class healthEducation(dml.Algorithm):
    contributor = 'jw0208'
    reads = ['jw0208.education', 'jw0208.health']
    writes = ['jw0208.healthEducation']


    @staticmethod
    def execute(trial=False):
        startTime = datetime.datetime.now()


        # Set up the database connection.
        client = dml.pymongo.MongoClient()
        repo = client.repo
        repo.authenticate('jw0208', 'jw0208')



        education = repo[jw0208.education]
        health = repo[jw0208.health]

        health_array = []
        for document in health.find():
            if document['year'] == '2010':
                health_array.append({'state':document['locationabbr'], 'physically&mentallyUnhealthyDays':document['data_value']})


        # X = [(state, highschool,bachelor)]
        state = []
        for document in education.find():
            state.append({'state': document['state'], 'highschool': document['highschool'], 'bachelor': document['bachelor']})

        keys = {r[0] for r in state}
        averagepercentage = [(key, statistics.mean([v1,v2 for (k, v1, v2) in state if k == key])) for key in keys]

        education_array = []

        for x in averagepercentage:
            education_array.append ('state':x[0], 'highschool':x[1], 'bachelor': x[2])



        repo.dropPermanent('jw0208.healthEducation')
        repo.createPermanent('jw0208.healthEducation')
        repo['jw0208.healthEducation'].insert_many(education_array)



        repo.logout()
        endTime = datetime.datetime.now()

        return {"start": startTime, "end": endTime}