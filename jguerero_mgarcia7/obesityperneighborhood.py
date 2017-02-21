# Add neighboorhood information to obesityperneighborhood, combine w boston shapefiles dataset??
# OUTPUT: dataset w/ neighborhood name, stats
import json
import dml
import prov.model
import datetime
import uuid
from collections import defaultdict
from shapely.geometry import shape #pip install shapely
import pickle

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
		neighborhood_shapes = {n['name']:shape(n['the_geom']) for n in neighborhoods.find({})}

		# Go through the obesity areas and figure out what neighborhood it's from
		d = defaultdict(list)
		for ob in obesitystats.find({}):
			ob_pts = ob['geometry']
			ob_shape = shape(ob_pts)
			for name,shp in neighborhood_shapes.items():
				if ob_shape.within(shp):
					d[name].append(ob['properties'])
					break

		# Go through neighborhoods and aggregate all of the obesity stats per neighborhood

		def project(R, p):
			return [p(d) for d in R]

		def aggregate(D, f):
		    keys = D[0].keys()
		    return {key:f([v for d in D for k,v in d.items() if k == key]) for key in keys}

		r = []
		for n_name,stats in d.items():
			info = {'neighborhood': n_name}

			# Aggregate data per obesity area for a whole neighborhood
			info.update(aggregate(project(stats,lambda d: {'popgte20':d['popgte20'], 'popbmige30':d['popbmige30']}),sum))

			info['pctbmige30'] = (info['popbmige30']/info['popgte20']) * 100 # percentage of population w bmi >= 30
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
		repo.authenticate('jguerero_mgarcia7', 'jguerero_mgarcia7')
		doc.add_namespace('alg', 'http://datamechanics.io/algorithm/') # The scripts are in <folder>#<filename> format.
		doc.add_namespace('dat', 'http://datamechanics.io/data/jguerero_mgarcia7') # The data sets are in <user>#<collection> format.
		doc.add_namespace('ont', 'http://datamechanics.io/ontology#') # 'Extension', 'DataResource', 'DataSet', 'Retrieval', 'Query', or 'Computation'.
		doc.add_namespace('log', 'http://datamechanics.io/log/') # The event log.


		this_script = doc.agent('alg:jguerero_mgarcia7#obesityperneighborhood', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'})
		obesitystats_resource = doc.entity('dat:obesitystats', {'prov:label':'Obesity Statistics MA', prov.model.PROV_TYPE:'ont:DataSet'})
		neighborhoods_resource = doc.entity('dat:neighborhoods', {'prov:label':'Neighborhoods Shapefile', prov.model.PROV_TYPE:'ont:DataSet'})

		get_obesityperneighborhood = doc.activity('log:uuid'+str(uuid.uuid4()), startTime, endTime)
		doc.wasAssociatedWith(get_obesityperneighborhood, this_script)
		doc.usage(get_obesityperneighborhood, obesitystats_resource, startTime, None,
				  {prov.model.PROV_TYPE:'ont:Computation'}
				  )

		doc.usage(get_obesityperneighborhood, neighborhoods_resource, startTime, None,
		  {prov.model.PROV_TYPE:'ont:Computation'}
		  )

		obesityperneighborhood = doc.entity('dat:jguerero_mgarcia7#obesityperneighborhood', {prov.model.PROV_LABEL:'Obesity Statistics Per Neighborhood', prov.model.PROV_TYPE:'ont:DataSet'})
		doc.wasAttributedTo(obesityperneighborhood, this_script)
		doc.wasGeneratedBy(obesityperneighborhood, get_obesityperneighborhood, endTime)
		doc.wasDerivedFrom(obesityperneighborhood, resource, get_obesityperneighborhood, get_obesityperneighborhood, get_obesityperneighborhood)

		repo.logout()
				  
		return doc


obesityperneighborhood.execute()
'''
doc = example.provenance()
print(doc.get_provn())
print(json.dumps(json.loads(doc.serialize()), indent=4))
'''

## eof
