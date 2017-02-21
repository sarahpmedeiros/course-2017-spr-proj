# Add neighboorhood information to obesityperneighborhood, combine w boston shapefiles dataset??
# OUTPUT: dataset w/ neighborhood name, stats
import json
import dml
import prov.model
import datetime
import uuid
import shapefile #pip install pyshp
from collections import defaultdict
from shapely.geometry import shape # pip install shapely

class obesityperneighborhood(dml.Algorithm):
	contributor = 'jguerero_mgarcia7'
	reads = ['jguerero_mgarcia7.neighborhoods', 'jguerero_mgarcia7.obesitystats']
	writes = ['jguerero_mgarcia7.obesityperneighborhood']

	@staticmethod
	def execute(trial = False):
		startTime = datetime.datetime.now()

		# Set up the database connection.
		client = dml.pymongo.MongoClient()
		repo = client.repo
		repo.authenticate('jguerero_mgarcia7', 'jguerero_mgarcia7')


		neighborhoods = repo['jguerero_mgarcia7.neighborhoods']
		obesitystats = repo['jguerero_mgarcia7.obesitystats']

		# Create shapeobjects for each neighborhood
		neighborhood_shapes = {}
		for neighborhood in neighborhoods.find({}):
			neighborhood_pts = neighborhood['the_geom']
			neighborhood_shapes[neighborhood['name']] = shape(neighborhood_pts)

		# Check to see what neighborhood the obesity stat polygons are in
		d = defaultdict(list)
		for ob in obesitystats.find({}):
			ob_pts = ob['geometry']
			ob_shape = shape(ob_pts)
			for name,shp in neighborhood_shapes.items():
				if ob_shape.within(shp):
					print('Stat for {} found'.format(name))
					d[name].append(ob['properties'])
					break

		# Go through neighborhoods and combine all the obesity info per neighborhood
		r = []
		for n_name,stats in d.items():
			final = defaultdict(list)
			info = {'neighborhood': n_name}
			for item in stats:
				for key,val in item.items():
					if key in ('popgte20', 'popbmige30'):
						final[key].append(val)

			for key,val in final.items():
				info[key] = sum(val)

			info['pctbmige30'] = (info['popbmige30']/info['popgte20']) * 100
			r.append(info)



		repo.dropCollection("obesityperneighborhood")
		repo.createCollection("obesityperneighborhood")
		repo['jguerero_mgarcia7.obesityperneighborhood'].insert_many(r)
		repo['jguerero_mgarcia7.obesityperneighborhood'].metadata({'complete':True})
		print(repo['jguerero_mgarcia7.obesityperneighborhood'].metadata())

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
		repo.authenticate('alice_bob', 'alice_bob')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.
		doc.add_namespace('bdp', 'https://data.cityofboston.gov/resource/')

		this_script = doc.agent('alg:alice_bob#example', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		resource = doc.entity('bdp:wc8w-nujj', {'prov:label':'311, Service Requests', prov.model.PROV_TYPE:'ont:DataResource', 'ont:Extension':'json'})
		get_found = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		get_lost = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_found, this_script)
		doc.wasAssociatedWith(get_lost, this_script)
		doc.usage(get_found, resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Retrieval',
				  'ont:Query':'?type=Animal+Found&$select=type,latitude,longitude,OPEN_DT'
				  }
				  )
		doc.usage(get_lost, resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Retrieval',
				  'ont:Query':'?type=Animal+Lost&$select=type,latitude,longitude,OPEN_DT'
				  }
				  )

		lost = doc.entity('dat:alice_bob#lost', {prov.model.PROV_LABEL:'Animals Lost', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(lost, this_script)
		doc.wasGeneratedBy(lost, get_lost, endTime)
		doc.wasDerivedFrom(lost, resource, get_lost, get_lost, get_lost)

		found = doc.entity('dat:alice_bob#found', {prov.model.PROV_LABEL:'Animals Found', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(found, this_script)
		doc.wasGeneratedBy(found, get_found, endTime)
		doc.wasDerivedFrom(found, resource, get_found, get_found, get_found)

		repo.logout()
				  
		return doc


obesityperneighborhood.execute()
'''
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
