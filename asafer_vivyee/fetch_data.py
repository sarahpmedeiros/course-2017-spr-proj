import urllib.request
import json
import dml
import prov.model
import datetime
import uuid

class FetchData(dml.Algorithm):
    contributor = 'asafer_vivyee'
    reads = []
    writes = ['asafer_vivyee.orchards', 'asafer_vivyee.corner_stores', 'asafer_vivyee.obesity', 'asafer_vivyee.nutrition_prog', 'asafer_vivyee.mbta']
    