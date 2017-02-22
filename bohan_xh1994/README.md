Project #1: Data Retrieval, Storage, Provenance, and Transformations
Han Xiao and Bohan Li

Overview:
	In this project, we’re trying to use various dataset, including food establishment distributions and cleanness inspections, entertainment in boston, crime incident, traffic in boston and airbnb information,  to find good airbnb housings in respect to safety, transportation convenience, accessibility to surrounding restaurants and entertainments.
	In the first transformation, we try to find the relationship between restaurants and crime incidents. We come up with a new dataset containing restaurants and incidents happens around them within radius of  1 km.
	In the second transformation, we try to find the number of restaurants and entertainments around each airbnb housing in boston, to evaluate whether it’s a good place to stay.
	In the third transformation, we try to evaluate the cleanness level of each food establishment by calculate the ratio between how many times it received inspections and how many time it passed inspections.

Data Sets:
	1. From city of Boston: 
		a. city of boston crime incident July 2012 - August 2015:
		    https://data.cityofboston.gov/resource/ufcx-3fdn.json
		b. Food Establishment Inspections:
		    https://data.cityofboston.gov/resource/427a-3cn5.json
		c. Active_Food_Establishment_Licenses:
		    https://data.cityofboston.gov/resource/fdxy-gydq.json
		d. Entertainment Licenses:
		    https://data.cityofboston.gov/resource/cz6t-w69j.json
	2. TRAFFIC SIGNALS:
	     http://bostonopendata-boston.opendata.arcgis.com/datasets/de08c6fe69c942509089e6db98c716a3_0.geojson
	3. Airbnb_boston:
	     http://datamechanics.io/data/bohan_xh1994/airbnb.json

To run:
Connect to set up mongodb server first: mongod --dbpath "<your_db_path>"
To run each file, simple do “python3 filename.py”



