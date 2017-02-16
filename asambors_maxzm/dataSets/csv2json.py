import csv
import json

#csvfile = open('test.csv','r')
csvfile = open('bostonTempData.txt','r')
jsonfile = open('bostonTempData.json','w')

fieldnames = ("STN---","WBAN","YEARMODA","TEMP","?","?","?", "SLP"  ,"?"  ,  "STP","?"," VISIB","?" ,  "WDSP","?", "MXSPD","GUST","MAX"  , " MIN"  ,"PRCP" ,"SNDP" , "FRSHTT")

reader = csv.DictReader(csvfile,fieldnames,delimiter=',')
for row in reader:
        json.dump(row,jsonfile)
        jsonfile.write(',')
        jsonfile.write('\n')

