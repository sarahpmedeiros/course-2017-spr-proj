# Union of cornerstores, farmersmarkets, supermarkets.py to get food in general


# Farmers Market: 
# Neighborhood
# Name 
# Address (Location)
# Zipcode

# Cornerstores:
# Neighborhood (area)
# Store Name (site)
# Store Address (Location)
# Zip

# Supermarkets: 
# Neighborhood
# Store Name
# Store Address 

# Type : FM, C, S 


import dml
import json

client = dml.pymongo.MongoClient()
repo = client.repo
repo.authenticate('jguerero_mgarcia7', 'jguerero_mgarcia7')

#cursors for all information in datasetss
population_data_cursor = repo['jguerero_mgarcia7.population'].find()
supermarkets_data_cursor = repo['jguerero_mgarcia7.supermarkets'].find()
cornerstores_data_cursor = repo['jguerero_mgarcia7.allcornerstores'].find()
farmersmarkets_data_cursor = repo['jguerero_mgarcia7.farmersmarkets'].find()

new_d_list = []
temp_d = {}
temp_k = {}
index = 0

for i in supermarkets_data_cursor:
	#print (i['Jamaica Plain'])
	temp_d = i.copy()
	del temp_d['_id']
	new_d_list.append(temp_d)

def combine(cursor, keyname):
	for j in cursor:
		n = j[keyname]
		#print (n)
		y = [x for x in new_d_list if x['neighborhood'] == n]
		if y != []:
			if y[0]['neighborhood'] == 'Jamaica Plain':
				print (y)
			temp_j = j.copy()
			del temp_j['_id']
			del temp_j[keyname]
			y[0].update(temp_j)
			#print (y[0])
			for i,dic in enumerate(new_d_list): #get index to update it
				if dic['neighborhood'] == n:
					new_d_list[i] = y[0]
					break
	return new_d_list

#new = combine(supermarkets_data_cursor, 'neighborhood')
other = combine(farmersmarkets_data_cursor, 'area')

for i in other:
	if i['neighborhood'] == 'Jamaica Plain':
		print ('i')


	#print (n)

#group them together by neighborhood#

'''


for i in population_data_cursor:
	temp_d = i.copy()
	del temp_d['_id']
	new_d_list.append(temp_d)

'''

#repo.dropCollection("allfoodaccess")
#repo.createCollection("allfoodaccess")
#repo['jguerero_mgarcia7.allfoodaccess'].insert_many(d)
#repo['jguerero_mgarcia7.population'].metadata({'complete':True})
#print(repo['jguerero_mgarcia7.population'].metadata())



#unify information by neighborhood