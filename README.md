#CS591 Project Proposal
####Sean Zhang & Martin Yim


  We are interested in identifying clusters of locations where parking tickets have been issued in Boston in the past. We are hoping to identify relationships between areas prone to being ticketed, and the area's characteristics (vehicle ownership, affluence, etc).
  
  We plan to use the K-Means algorithm to identify areas prone to more parking tickets (based on volume of tickets issued). By then relating these clusters with information such as property values in the area and vehicle tax (as a proxy for vehicle ownership) by zipcode, we hope to find areas where the supply of parking is not meeting the demand (hence, parking tickets). We could then use this information to share with the city of Boston to determine where more parking could be provided to meet citizens' parking needs. 

<<<<<<< HEAD
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

You will need the latest versions of the PROV, DML, and Protoql Python libraries. If you have `pip` installed, the following should install the latest versions automatically:
```
pip install prov --upgrade --no-cache-dir
pip install dml --upgrade --no-cache-dir
pip install protoql --upgrade --no-cache-dir
```
If you are having trouble installing `lxml` in a Windows environment, you could try retrieving it [here](http://www.lfd.uci.edu/~gohlke/pythonlibs/).

Note that you may need to use `python -m pip install <library>` to avoid issues if you have multiple versions of `pip` and Python on your system.

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
To access the contents of the `auth.json` file after you have loaded the `dml` library, use `dml.auth`.

## Running the execution script for a contributed project.

To execute all the algorithms for a particular contributor (e.g., `alice_bob`) in an order that respects their explicitly specified data flow dependencies, you can run the following from the root directory:
```
python execute.py alice_bob
```
=======
  Conversely, we are also hoping to use the data that we've gathered to find new locations in the city that could be an area of dense illegal parking in the future, based on car ownership, property value, and public transportation convenience. These areas may be under-enforced, and could yield revenue for the city, but may also prove to be areas which need consideration for more parking as well.
>>>>>>> caf02f094ba0bc99018114af0d7f09d50d8fe602
