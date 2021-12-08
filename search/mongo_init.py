# -*- coding: utf-8 -*-
"""
Created on Fri Mar 12 16:06:24 2021

@author: Akash Raj
"""

from pymongo import MongoClient
from elasticsearch import Elasticsearch
#es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
#es = Elasticsearch([{'host': 'elastic.bighaat.cf'}])
es = Elasticsearch([{'host': 'elastic-helm-elasticsearch-coordinating-only'}])
#es = Elasticsearch("https://elastic:al8tPX2oS900b410YIg9QB2g@52.224.252.20:9200/", verify_certs=False, connection_class=RequestsHttpConnection)
import datetime
import time
mongo_client = MongoClient('mongodb://root:pqeBqx8qgV@20.193.137.123:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false')


import json
from bson import ObjectId

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime.datetime):
            return str(o)
        return json.JSONEncoder.default(self, o)

start_time = time.time()
db = mongo_client.bighaat
col = db.posts
cursor = col.find()
posts = list(cursor)
status=[]
for node in posts:
    _id = str(node['_id']).split("'")[0]
    node.pop('_id')
    res=es.index(index='posts_sync',id=_id,body=JSONEncoder().encode(node))
    status.append(res['_id'])

col = db.users
cursor = col.find()
users = list(cursor)
status=[]
for node in users:
    _id = str(node['_id']).split("'")[0]
    node.pop('_id')
    res=es.index(index='users_sync',id=_id,body=JSONEncoder().encode(node))
    status.append(res['_id'])

col = db.comments
cursor = col.find()
comments = list(cursor)
status=[]
for node in comments:
    _id = str(node['_id']).split("'")[0]
    node.pop('_id')
    res=es.index(index='comments_sync',id=_id,body=JSONEncoder().encode(node))
    status.append(res['_id'])

col = db.crops
cursor = col.find()
crops = list(cursor)
status=[]
for node in crops:
    _id = str(node['_id']).split("'")[0]
    node.pop('_id')
    res=es.index(index='crops_sync',id=_id,body=JSONEncoder().encode(node))
    status.append(res['_id'])
    
col = db.crop_doctor
cursor = col.find()
crops = list(cursor)
status=[]
for node in crops:
    _id = str(node['_id']).split("'")[0]
    node.pop('_id')
    res=es.index(index='crop_doc_sync',id=_id,body=JSONEncoder().encode(node))
    status.append(res['_id'])
print('Done')
print("--- %s seconds ---" % (time.time() - start_time))