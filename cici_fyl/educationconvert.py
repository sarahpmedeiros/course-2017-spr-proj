import csv
import json

csvfile = open('education.csv', 'r')
jsonfile = open('education.json', 'w')

fieldnames = ("state", "highschool", "bachelor")

reader = csv.DictReader(csvfile, fieldnames, delimiter=',')
for row in reader:
    json.dump(row, jsonfile)
    jsonfile.write(',')
    jsonfile.write('\n')
