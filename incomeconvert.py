import csv
import json

csvfile = open('income.csv', 'r')
jsonfile = open('income.json', 'w')

fieldnames = ("ID", "state", "statename", "HHincome", "percentage")

reader = csv.DictReader(csvfile, fieldnames, delimiter=',')
for row in reader:
    json.dump(row, jsonfile)
    jsonfile.write(',')
    jsonfile.write('\n')
