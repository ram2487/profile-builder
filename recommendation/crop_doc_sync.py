# -*- coding: utf-8 -*-
"""
Created on Fri Mar 26 10:15:34 2021

@author: Akash Raj
"""

from pymongo import MongoClient
import datetime
import time
from elasticsearch import Elasticsearch
from bson.objectid import ObjectId
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

mongo_client = MongoClient('mongodb://root:pqeBqx8qgV@20.193.137.123:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false')
db = mongo_client.bighaat

import json

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime.datetime):
            return str(o)
        return json.JSONEncoder.default(self, o)

while(1):
    start_time_crop_doc = time.time()    
    col = db.crop_doctor
    cursor = col.find()
    crop_doctor = list(cursor)
#    status=[]
    for node in crop_doctor:
        _id = str(node['_id']).split("'")[0]
        node.pop('_id')
        res=es.index(index='crop_doc_sync',id=_id,body=JSONEncoder().encode(node))
#        status.append(res['_id'])
    print("Crop Doc--- %s seconds ---" % (time.time() - start_time_crop_doc))