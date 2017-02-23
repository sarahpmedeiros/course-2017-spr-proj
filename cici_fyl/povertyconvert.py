import csv
import json

csvfile = open('poverty.csv', 'r')
jsonfile = open('poverty.json', 'w')

fieldnames = ("Name", "Percent")

reader = csv.DictReader(csvfile, fieldnames, delimiter=',')
for row in reader:
    json.dump(row, jsonfile)
    jsonfile.write(',')
    jsonfile.write('\n')
