import csv
import json

#csvfile = open('test.csv','r')
csvfile = open('zipcodestolatlong.txt','r')
jsonfile = open('zipcodestolatlong.json','w')

# ZIP,LAT,LNG
fieldnames = ("zip_code", "lat", "long")

reader = csv.DictReader(csvfile, fieldnames, delimiter = ',')
for row in reader:
    json.dump(row, jsonfile)
    jsonfile.write(',')
    jsonfile.write('\n')