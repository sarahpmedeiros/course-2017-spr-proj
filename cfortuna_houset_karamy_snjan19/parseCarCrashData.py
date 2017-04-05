import json

# Script to clean the dataset of car accidents in Boston
with open('2014_accidents.json') as data_file:    
    all_data = json.load(data_file)

for element in all_data: 
    del element['Crash Number']
    del element['City']
    del element['X Coordinate']
    del element['Y Coordinate']
    del element['Non Motorist Type']
    del element['Distance from Nearest Roadway Intersection']
    del element['Distance from Nearest Milemarker']
    del element['Distance from Nearest Exit']
    del element['Distance from Nearest Landmark']
    del element['Manner of Collision']
    del element['Vehicle Action Prior to Crash']
    del element['Vehicle Travel Directions']
    del element['Most Harmful Events']
    del element['Vehicle Configuration']

with open('CarCrashData.json', 'w') as out_file:
    out_file.write(json.dumps(all_data, indent=4))
