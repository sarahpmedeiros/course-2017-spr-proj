### Questions for Office Hours
#### Provenance
  * When we add cdc data, do I include the full url with the offset and limit parameters for SODA API? 
  * Usage: Do I need to write a specific query as seen in example.py?

```python 
this_script = doc.agent('alg:asambors_maxzm#fetchData', {prov.model.PROV_TYPE:prov.model.PROV['SoftwareAgent'], 'ont:Extension':'py'}) 

```
  * Should this be fetchData (file name / class name) or is it arbitrary?  

#### Transformations
  * Was this supposed to be done on the mongo side entirely?

#### General
  * In each file, do we need to have a prov method? Can we reference the prov method found in the file that fetches all the data?
  * If each file requires a prov method, do we copy and paste or should we just grab the relevant dataset?
  * We didn't need any authentication, should we be concerned by this? 
  * How to clear our mongodb? 
  * How would you like the files arranged? A bit confused on what the architecture overall is supposed to look like beyond having class files