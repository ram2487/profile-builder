# -*- coding: utf-8 -*-
"""
Created on Fri Mar 12 11:39:27 2021

@author: Akash Raj
"""
from pymongo import MongoClient
import datetime
#import time
from elasticsearch import Elasticsearch
from bson.objectid import ObjectId
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
from pandas.io.json import json_normalize
import pandas as pd
#import re
import numpy as np

mongo_client = MongoClient('mongodb://root:pqeBqx8qgV@20.193.137.123:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false')


import json

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime.datetime):
            return str(o)
        return json.JSONEncoder.default(self, o)

while(1):
    t=datetime.datetime.utcnow().strftime("%Y:%m:%d:%H:%M").split(":")
    db = mongo_client.bighaat
#    col = db.posts
#    end=datetime.datetime(int(t[0]),int(t[1]),int(t[2]),int(t[3]),int(t[4]))
#    delta=datetime.timedelta(minutes=2)
#    start=end-delta
#    cursor = col.find({'updatedAt':{'$gte': start}})
#    posts = list(cursor)
#    for node in posts:
#        _id = str(node['_id']).split("'")[0]
#        node.pop('_id')
#        es.index(index='posts_sync',id=_id,body=JSONEncoder().encode(node))
#        
#    print("Updated {} {}".format(len(list(cursor)),'posts'))
    
    col = db.comments
    end=datetime.datetime(int(t[0]),int(t[1]),int(t[2]),int(t[3]),int(t[4]))
    delta=datetime.timedelta(minutes=2)
    start=end-delta
    cursor = col.find({'updatedAt':{'$gte': start}})
    comments = list(cursor)
    for node in comments:
        _id = str(node['_id']).split("'")[0]
        node.pop('_id')
        es.index(index='comments_sync',id=_id,body=JSONEncoder().encode(node))
    
    print("Updated {} {}".format(len(list(cursor)),'comments'))
    
#    col = db.users
#    end=datetime.datetime(int(t[0]),int(t[1]),int(t[2]),int(t[3]),int(t[4]))
#    delta=datetime.timedelta(minutes=2)
#    start=end-delta
#    cursor = col.find({'lastOpened':{'$gte': start}})
#    users = list(cursor)
#    for node in users:
#        _id = str(node['_id']).split("'")[0]
#        node.pop('_id')
#        es.index(index='users_sync',id=_id,body=JSONEncoder().encode(node))
#    
#    print("Updated {} {}".format(len(list(cursor)),'users'))
#    
#    col = db.crops
#    end=datetime.datetime(int(t[0]),int(t[1]),int(t[2]),int(t[3]),int(t[4]))
#    delta=datetime.timedelta(minutes=2)
#    start=end-delta
#    cursor = col.find({'updatedAt':{'$gte': start}})
#    crops = list(cursor)
#    for node in crops:
#        _id = str(node['_id']).split("'")[0]
#        node.pop('_id')
#        es.index(index='crops_sync',id=_id,body=JSONEncoder().encode(node))
#    
#    print("Updated {} {}".format(len(list(cursor)),'crops'))
    
    col = db.crop_doctor
    end=datetime.datetime(int(t[0]),int(t[1]),int(t[2]),int(t[3]),int(t[4]))
    delta=datetime.timedelta(minutes=2)
    start=end-delta
    cursor = col.find({'updatedOn':{'$gte': start}})
    crop_doctor = list(cursor)
    for node in crop_doctor:
        _id = str(node['_id']).split("'")[0]
        node.pop('_id')
        es.index(index='crop_doc_sync',id=_id,body=JSONEncoder().encode(node))
    
    print("Updated {} {}".format(len(list(cursor)),'crop_doctor'))    
#    time.sleep(5)

