import csv
import json

#csvfile = open('test.csv','r')
csvfile = open('zipCodeSallaries.csv','r')
jsonfile = open('zipCodeSallaries.json','w')

fieldnames = ("zip_code", "income")


reader = csv.DictReader(csvfile,fieldnames,delimiter=',')
for row in reader:
        json.dump(row,jsonfile)
        jsonfile.write(',')
        jsonfile.write('\n')

