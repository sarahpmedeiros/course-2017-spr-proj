import csv
import json

csvfile = open('medicare.csv', 'r')
jsonfile = open('medicare.json', 'w')

fieldnames = ("hospital", "city", "state", "county", "score", "measurestart", "measureend")

reader = csv.DictReader(csvfile, fieldnames, delimiter=',')
for row in reader:
    json.dump(row, jsonfile)
    jsonfile.write(',')
    jsonfile.write('\n')
