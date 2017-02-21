# course-2017-spr-proj
Project repository for the course project in the Spring 2017 iteration of the Data Mechanics course at Boston University.

In this project, you will implement platform components that can obtain a some data sets from web services of your choice, and platform components that combine these data sets into at least two additional derived data sets. These components will interct with the backend repository by inserting and retrieving data sets as necessary. They will also satisfy a standard interface by supporting specified capabilities (such as generation of dependency information and provenance records).

**This project description will be updated as we continue work on the infrastructure.**

## MongoDB infrastructure

### Setting up

We have committed setup scripts for a MongoDB database that will set up the database and collection management functions that ensure users sharing the project data repository can read everyone's collections but can only write to their own collections. Once you have installed your MongoDB instance, you can prepare it by first starting `mongod` _without authentication_:
```
mongod --dbpath "<your_db_path>"
```
If you're setting up after previously running `setup.js`, you may want to reset (i.e., delete) the repository as follows.
```
mongo reset.js
```
Next, make sure your user directories (e.g., `alice_bob` if Alice and Bob are working together on a team) are present in the same location as the `setup.js` script, open a separate terminal window, and run the script:
```
mongo setup.js
```
Your MongoDB instance should now be ready. Stop `mongod` and restart it, enabling authentication with the `--auth` option:
```
mongod --auth --dbpath "<your_db_path>"
```

### Working on data sets with authentication

With authentication enabled, you can start `mongo` on the repository (called `repo` by default) with your user credentials:
```
mongo repo -u alice_bob -p alice_bob --authenticationDatabase "repo"
```
However, you should be unable to create new collections using `db.createCollection()` in the default `repo` database created for this project:
```
> db.createCollection("EXAMPLE");
{
  "ok" : 0,
  "errmsg" : "not authorized on repo to execute command { create: \"EXAMPLE\" }",
  "code" : 13
}
```
Instead, load the server-side functions so that you can use the customized `createCollection()` function, which creates a collection that can be read by everyone but written only by you:
```
> db.loadServerScripts();
> var EXAMPLE = createCollection("EXAMPLE");
```
Notice that this function also prefixes the user name to the name of the collection (unless the prefix is already present in the name supplied to the function).
```
> EXAMPLE
alice_bob.EXAMPLE
> db.alice_bob.EXAMPLE.insert({value:123})
WriteResult({ "nInserted" : 1 })
> db.alice_bob.EXAMPLE.find()
{ "_id" : ObjectId("56b7adef3503ebd45080bd87"), "value" : 123 }
```
If you do not want to run `db.loadServerScripts()` every time you open a new terminal, you can use a `.mongorc.js` file in your home directory to store any commands or calls you want issued whenever you run `mongo`.

## Other required libraries and tools

You will need the latest versions of the PROV and DML Python libraries. If you have `pip` installed, the following should install the latest versions automatically:
```
pip install prov --upgrade --no-cache-dir
pip install dml --upgrade --no-cache-dir
```
If you are having trouble installing `lxml` in a Windows environment, you could try retrieving it [here](http://www.lfd.uci.edu/~gohlke/pythonlibs/).

## Formatting the `auth.json` file

The `auth.json` file should remain empty and should not be submitted. When you are running your algorithms, you should use the file to store your credentials for any third-party data resources, APIs, services, or repositories that you use. An example of the contents you might store in your `auth.json` file is as follows:
```
{
    "services": {
        "cityofbostondataportal": {
            "service": "https://data.cityofboston.gov/",
            "username": "alice_bob@example.org",
            "token": "XxXXXXxXxXxXxxXXXXxxXxXxX",
            "key": "xxXxXXXXXXxxXXXxXXXXXXxxXxxxxXXxXxxX"
        },
        "mbtadeveloperportal": {
            "service": "http://realtime.mbta.com/",
            "username": "alice_bob",
            "token": "XxXX-XXxxXXxXxXXxXxX_x",
            "key": "XxXX-XXxxXXxXxXXxXxx_x"
        }
    }
}
```

## Running the execution script for a contributed project.

To execute all the algorithms for a particular contributor (e.g., `alice_bob`) in an order that respects their explicitly specified data flow dependencies, you can run the following from the root directory:
```
python execute.py alice_bob
```

## Datasets we use

From City of Boston Data Portal:

Property Assessment 2014: https://data.cityofboston.gov/dataset/Property-Assessment-2014/qz7u-kb7x
Property Assessment 2015: https://data.cityofboston.gov/Permitting/Property-Assessment-2015/yv8c-t43q
Crime Incident Reports: https://data.cityofboston.gov/Public-Safety/Crime-Incident-Reports-July-2012-August-2015-Sourc/7cdf-6fgx

From BostonMaps Open Data: 
Police Districts: http://bostonopendata-boston.opendata.arcgis.com/datasets/9a3a8c427add450eaf45a470245680fc_5?uiTab=table

From MassData:
FLD Complaints: https://data.mass.gov/dataset/FLD-Complaints/c5kv-hee8


## Short narrative of How our datasets are going to be used and What our project will be focus on

We are interested in “Boston crime incidents” in relation to “property values”, “police districts”, and  “fair labor division complaints”. Our main dataset will be Crime Incident Reports and we have come out three assumptions: First, there will be less crime incidents when “the total assessed value of property” rises. To do so, we look at data from Property Assessments from year 2014 and 2015 and compare if there are any correlations between the total assessed value for property and the number of crime incidents. Second, we assume that there will be less crime incidents within police districts. To do so, we look at the “district” column from Police Districts and check the proportion that the number of the districts where the crime incident equals to police district out of the total number of crimes. Last but not least, while there are many researches indicate that unemployment causes crime, we want to look that whether employees’ happiness also affects crime rate. We assume that there will be less crime rate when Fair Labor Division receives less complaints because people are happier and will not tend to behave silly. In this case, we compare data of FLD Complaints in Year 2015 in Boston area.


## Non-trivial transformations we implemented

The first non-trivial transformation we do is combining "Crime Incident Reports" with "Property Assessments" of 2014 and 2015, and we use projection to get columns of "av_total" and "location" from Property Assessments into property14_price_coordination and property15_price_coordination. We then extract the column of "location" from Crime Incident Reports into crime_14coordination and crime_15coordination, and see if there exists any relation between them. 
Second, we combine Police Districts and Crime Incident Reports and use projection to get the single "DISTRICT" column from Police Districts and "reptdistrict" from Crime Incident Reports to calculate the proportion mentioned earlier. 
Finally, we combine Crime Incident Reports and FLD Complaints, then use projection to extract columns of "business city == Boston" and "date received" within Jan-Aug of 2015 from FLD Complaints, and also use projection to get "year == 15" and "month <= 8" from Crime Incident Reports and find the correlation of Jan-Feb, Mar-Apr, May-Jun, Jul-Aug between the columns we use.
