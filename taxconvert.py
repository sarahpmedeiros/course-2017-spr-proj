import csv
import json

csvfile = open('tax.csv', 'r')
jsonfile = open('tax.json','w')

fieldnames = ("Geographic", "Year", "Meaning", "Amount")

reader = csv.DictReader(csvfile, fieldnames, delimiter =',')
for row in reader:
    json.dump(row,jsonfile)
    jsonfile.write(',')
    jsonfile.write('\n')
