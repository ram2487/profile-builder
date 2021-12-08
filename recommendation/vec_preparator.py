# -*- coding: utf-8 -*-
"""
Created on Thu Mar 18 21:33:09 2021

@author: Akash Raj
"""

from elasticsearch import Elasticsearch
import time
import numpy as np
from pandas.io.json import json_normalize
import pandas as pd
import re
from es_pandas import es_pandas

while(1):
    es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
    
    res = es.search(index="posts_sync", body={"size":10000,"query":{"match_all":{}}})
    posts = json_normalize(res['hits']['hits'])
    
    res = es.search(index="comments_sync", body={"size":10000,"query":{"match_all":{}}})
    comments = json_normalize(res['hits']['hits'])
    
    res = es.search(index="users_sync", body={"size":10000,"query":{"match_all":{}}})
    users = json_normalize(res['hits']['hits'])
    
    res = es.search(index="crops_sync", body={"size":10000,"query":{"match_all":{}}})
    crops = json_normalize(res['hits']['hits'])
    
    posts.columns
    posts.columns=pd.Series(posts.columns).apply(lambda x:x.split('.')[-1])
    
    posts=posts.rename(columns={'_id':'postId'})
    
    posts['x']=posts['coordinates'].apply(lambda x:x[0])
    posts['y']=posts['coordinates'].apply(lambda x:x[1])
    
    posts=pd.concat([posts,pd.get_dummies(posts['language'])],axis=1)
    
    
    crop_dict=dict(zip(crops['_id'],crops['_source.cropName']))
    
    posts[list(crop_dict.values())]=0
    for row in posts.itertuples():
        pass
        posts.loc[row[0],crop_dict[row[21]]]=1
    
    cols_xy=['x','y']
    cols_xy.extend(posts['language'].unique().tolist())
    cols_xy.extend(list(crop_dict.values()))
    
    cols_non_xy=posts['language'].unique().tolist()
    cols_non_xy.extend(list(crop_dict.values()))
    
    users.columns

    users.columns=pd.Series(users.columns).apply(lambda x:x.split('.')[-1])
    users=users[['_id', 'crops', 'userType','language', 'coordinates']]
    
    users['x']=users['coordinates'].apply(lambda x:x[0])
    users['y']=users['coordinates'].apply(lambda x:x[1])
    users[list(crop_dict.values())]=0
    
    for row in users.itertuples():
        pass
        for c in row[2]:
            if c['cropId']!=None:
                users.loc[row[0],crop_dict[str(c['cropId'])]]=1
    users=pd.concat([users,pd.get_dummies(users['language'])],axis=1)
    
    users[['en', 'hi', 'te']]=users[['en', 'hi', 'te']].replace(1,7)
    users.set_index('_id',inplace=True)
    user_dict=users[cols_non_xy].to_dict(orient='index')
    
    for i in user_dict.keys():
        user_dict[i]=np.array(list(user_dict[i].values()))
    
    cu=0
    miss=[]
    for index,row in posts.iterrows():
        pass
        post_vec=np.array(row[cols_non_xy].tolist())
        user_dict[row['userId']]=user_dict[row['userId']]+post_vec
    
    
        for u in row['upvotes']:
            user_dict[u]= user_dict[u]+post_vec
        
        for u in row['viewedUsers']:
            if str(type(user_dict.get(u)))!="<class 'NoneType'>":
                user_dict[u]= user_dict[u]+post_vec
            else:
                cu=cu+1
                miss.append(u)
    
    post_dict=posts[['postId']+cols_non_xy]
    post_dict.set_index('postId',inplace=True)
    post_dict=post_dict.to_dict(orient='index')
    
    for i in post_dict.keys():
        post_dict[i]=np.array(list(post_dict[i].values()))
    
    comments.columns=pd.Series(comments.columns).apply(lambda x:x.split('.')[-1])
    for index,row in comments.iterrows():
        pass
        if str(type(post_dict.get(row['postId'])))!="<class 'NoneType'>":
            post_vec=post_dict[row['postId']]
            user_dict[row['userId']]=user_dict[row['userId']]+post_vec
        
        for u in row['upvotes']:
            user_dict[u]= user_dict[u]+post_vec
    
    user_df=pd.DataFrame.from_dict(user_dict,orient='index').reset_index()
    cols=['_id']+cols_non_xy
    user_df.columns=cols
    user_df=pd.concat([users[['x','y']].reset_index(drop=True),user_df],axis=1)
    user_df=user_df.set_index('_id')
    
    es_host = 'localhost:9200'
    index = 'users_activity_vec'
    doc_type = 'vec'
    ep = es_pandas(es_host)
    ep.init_es_tmpl(user_df, doc_type)
    ep.to_es(user_df, index, doc_type=doc_type, use_index=True)
    
    posts_vec=posts[['postId']+cols_xy+['isDeleted']].set_index('postId')
    index = 'posts_vec'
    doc_type = 'vec'
    ep = es_pandas(es_host)
    ep.init_es_tmpl(posts_vec, doc_type)
    ep.to_es(posts_vec, index, doc_type=doc_type, use_index=True)
    
    res = es.search(index="crop_doc_sync", body={"size":10000,"query":{"match_all":{}}})
    df = json_normalize(res['hits']['hits'])
    
    df['_id'].nunique()
    df=df[~df['_source.diagnosisDetails'].isnull()]
    df['disease']=df['_source.diagnosisDetails'].apply(lambda x:re.sub('[^A-Za-z0-9]+',' ',x[0]['causeInfo'][0]['name']))
    
    d={}
    for i in df['_source.userId'].tolist():
        d[i]=[]
    
    for i,j in zip(df['_source.userId'].tolist(),df['disease'].tolist()):
        d[i].append(j)
    
    for i in df['_source.userId'].tolist():
        d[i]=list(set(d[i]))
    
    res=es.index(index='crop_doc_disease',body=d)
    print("Done")
#    time.sleep(10)