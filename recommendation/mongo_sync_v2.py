"""local_mongo_sync_beta_app"""
#code='local_mongo_sync_beta_app'
#mongo_string='mongodb://root:pqeBqx8qgV@20.193.137.123:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false'
#es_host = 'localhost:9200'
#from elasticsearch import Elasticsearch
#es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
#auth={'apiKey': '135BE3F9-B1D8-4A14-A999-303286C32275'}

"""local_mongo_sync_prod_app"""
#code='local_mongo_sync_prod_app'
#mongo_string='mongodb://root:eXVB5mbmuZ@20.193.140.88:27017/admin?authSource=admin&readPreference=primary&appname=MongoDB%20Compass%20Community&ssl=false'
#es_host = 'localhost:9200'
#from elasticsearch import Elasticsearch
#es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
#auth={'apiKey': '135BE3F9-B1D8-4A14-A999-303286C32275'}

"""dev_env_mongo_sync_beta_app"""
code='dev_env_mongo_sync_beta_app'
mongo_string='mongodb://root:pqeBqx8qgV@20.193.137.123:27017/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&ssl=false'
es_host = 'elastic-helm-elasticsearch-coordinating-only'
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'elastic-helm-elasticsearch-coordinating-only'}])
auth={'apiKey': '135BE3F9-B1D8-4A14-A999-303286C32275'}
#
"""beta_env_mongo_sync_prod_app"""
#code='beta_env_mongo_sync_prod_app'
#mongo_string='mongodb://root:eXVB5mbmuZ@20.193.140.88:27017/admin?authSource=admin&readPreference=primary&appname=MongoDB%20Compass%20Community&ssl=false'
#es_host = 'elastic-helm-elasticsearch-coordinating-only'
#from elasticsearch import Elasticsearch
#es = Elasticsearch([{'host': 'elastic-helm-elasticsearch-coordinating-only'}])
#auth={'apiKey': '135BE3F9-B1D8-4A14-A999-303286C32275'}

"""prod_env_mongo_sync_prod_app"""
#code='prod_env_mongo_sync_prod_app'
#mongo_string='mongodb://root:eXVB5mbmuZ@bighaat-mongo-mongodb-0.bighaat-mongo-mongodb-headless.kisanvedika.svc.cluster.local:27017,bighaat-mongo-mongodb-1.bighaat-mongo-mongodb-headless.kisanvedika.svc.cluster.local:27017,bighaat-mongo-mongodb-2.bighaat-mongo-mongodb-headless.kisanvedika.svc.cluster.local:27017/admin?authSource=admin&compressors=disabled&gssapiServiceName=mongodb'
#es_host = 'elastic-helm-elasticsearch-coordinating-only'
#from elasticsearch import Elasticsearch
#es = Elasticsearch([{'host': 'elastic-helm-elasticsearch-coordinating-only'}])
#auth={'apiKey': '135BE3F9-B1D8-4A14-A999-303286C32275'}

from pymongo import MongoClient
import datetime
import time
from elasticsearch import Elasticsearch
from bson.objectid import ObjectId
from elasticsearch.helpers import scan

#es = Elasticsearch([{'host': 'elastic-helm-elasticsearch-coordinating-only'}])
#es = Elasticsearch("elastic-helm-elasticsearch-coordinating-only")
#es = Elasticsearch("https://elastic:al8tPX2oS900b410YIg9QB2g@52.224.252.20:9200/", verify_certs=False, connection_class=RequestsHttpConnection)
#from pandas.io.json import json_normalize
import pandas as pd
import re
import numpy as np
from es_pandas import es_pandas
import warnings
warnings.filterwarnings("ignore")

#es_host = 'elastic-helm-elasticsearch-coordinating-only'
mongo_client = MongoClient(mongo_string)
db = mongo_client.bighaat

import json

import requests


def big_read(index):
    es_response = scan(
        es,
        index=index,
        doc_type='_doc',
        query={"query": { "match_all" : {}}}
    )
    return es_response
    
def logger(message):
    message_dict={'code':code,
       'message':message}
    r=requests.post('https://apibeta.bighaat.com/crop/api/logerror/create-error-log?message={}&api-version=1.0'.format(str(message_dict)),headers=auth)
    return r.text  

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime.datetime):
            return str(o)
        return json.JSONEncoder.default(self, o)
#try:
#    start_time = time.time()
#    db = mongo_client.bighaat
#    col = db.posts
#    cursor = col.find()
#    posts = list(cursor)
#    status=[]
#    for node in posts:
#        _id = str(node['_id']).split("'")[0]
#        node.pop('_id')
#        res=es.index(index='posts_sync',id=_id,body=JSONEncoder().encode(node))
#        status.append(res['_id'])
#    print("Posts--- %s seconds ---" % (time.time() - start_time_posts))
#    logger("Posts--- %s seconds ---" % (time.time() - start_time_posts))
#    
#    col = db.comments
#    cursor = col.find()
#    comments = list(cursor)
#    status=[]
#    for node in comments:
#        _id = str(node['_id']).split("'")[0]
#        node.pop('_id')
#        res=es.index(index='comments_sync',id=_id,body=JSONEncoder().encode(node))
#        status.append(res['_id'])
#    
#    col = db.crops
#    cursor = col.find()
#    crops = list(cursor)
#    status=[]
#    for node in crops:
#        _id = str(node['_id']).split("'")[0]
#        node.pop('_id')
#        res=es.index(index='crops_sync',id=_id,body=JSONEncoder().encode(node))
#        status.append(res['_id'])
#        
#    col = db.crop_doctor
#    cursor = col.find()
#    crops = list(cursor)
#    status=[]
#    for node in crops:
#        _id = str(node['_id']).split("'")[0]
#        node.pop('_id')
#        res=es.index(index='crop_doc_sync',id=_id,body=JSONEncoder().encode(node))
#        status.append(res['_id'])
#    print('Done')
#    print("--- %s Mongo Init seconds ---" % (time.time() - start_time))
#    logger("--- %s Mongo Init seconds ---" % (time.time() - start_time)) 
#except Exception as e:
#    logger('Error in Mongo Init '+str(e))
#    print('Error in Mongo Init '+str(e))

while(1):
    try:
        start_time = time.time()
        start_time_posts = time.time()
        col = db.posts
        cursor = col.find()
        posts = list(cursor)
        print(time.time()-start_time_posts)
        
        posts_list=[]
        for node in posts:
            pass
            posts_list.append(json.loads(JSONEncoder().encode(node)))
        
        posts=pd.DataFrame(posts_list)
    
        print("Posts--- %s seconds ---" % (time.time() - start_time_posts))
        message="Posts--- %s seconds ---" % (time.time() - start_time_posts)
        logger(message)
        
        start_time_users = time.time()
        col = db.users
        cursor = col.find()
        users = list(cursor)
        print(time.time()-start_time_users)
        
        users_list=[]
        for node in users:
            pass
            users_list.append(json.loads(JSONEncoder().encode(node)))
        
        users=pd.DataFrame(users_list)
    
        print("Users--- %s seconds ---" % (time.time() - start_time_users))
        message="Users--- %s seconds ---" % (time.time() - start_time_users)
        logger(message)
        
        start_time_comments = time.time()
        col = db.comments
        cursor = col.find()
        comments = list(cursor)
        print(time.time()-start_time_comments)
        
        comments_list=[]
        for node in comments:
            pass
            comments_list.append(json.loads(JSONEncoder().encode(node)))
        
        comments=pd.DataFrame(comments_list)
        
        comments_df=comments.set_index('_id')[['postId','userId','comment','isDeleted']]
        index = 'comments_sync'
        doc_type = 'vec'
        ep = es_pandas(es_host)
        ep.init_es_tmpl(comments_df, doc_type)
        ep.to_es(comments_df, index, doc_type=doc_type, use_index=True)
        
        print("Comments--- %s seconds ---" % (time.time() - start_time_comments))
        message="Comments--- %s seconds ---" % (time.time() - start_time_comments)
        logger(message)
        
        start_time_crop_doctor = time.time()
        col = db.crop_doctor
        cursor = col.find()
        crop_doctor = list(cursor)
        print(time.time()-start_time_crop_doctor)
        
        crop_doctor_list=[]
        for node in crop_doctor:
            pass
            crop_doctor_list.append(json.loads(JSONEncoder().encode(node)))
        
        crop_doctor=pd.DataFrame(crop_doctor_list)
    
        print("Crop_doctor--- %s seconds ---" % (time.time() - start_time_crop_doctor))
        message="Crop_doctor--- %s seconds ---" % (time.time() - start_time_crop_doctor)
        logger(message)
        
        start_time_crops = time.time()
        col = db.crops
        cursor = col.find()
        crops = list(cursor)
        print(time.time()-start_time_crops)
        
        crops_list=[]
        for node in crops:
            pass
            crops_list.append(json.loads(JSONEncoder().encode(node)))
        
        crops=pd.DataFrame(crops_list)
    
        print("Crops--- %s seconds ---" % (time.time() - start_time_crops))
        message="Crops--- %s seconds ---" % (time.time() - start_time_crops)
        logger(message)
        
        print("Total Updates--- %s seconds ---" % (time.time() - start_time))
        logger("Total Updates--- %s seconds ---" % (time.time() - start_time))
        
    #    start_time_crops = time.time()
    #    col = db.crops
    #    cursor = col.find()
    #    crops = list(cursor)
    ##    status=[]
    #    for node in crops:
    #        _id = str(node['_id']).split("'")[0]
    #        node.pop('_id')
    #        res=es.index(index='crops_sync',id=_id,body=JSONEncoder().encode(node))
    ##        status.append(res['_id'])
    #    print("Crops--- %s seconds ---" % (time.time() - start_time_crops))
    #    logger("Crops--- %s seconds ---" % (time.time() - start_time_crops))
    #    
    #    t=datetime.datetime.utcnow().strftime("%Y:%m:%d:%H:%M").split(":")
    #    db = mongo_client.bighaat
    #    
    #    start_time_updates = time.time()
    #    col = db.crop_doctor
    #    end=datetime.datetime(int(t[0]),int(t[1]),int(t[2]),int(t[3]),int(t[4]))
    #    delta=datetime.timedelta(minutes=2)
    #    start=end-delta
    #    cursor = col.find({'updatedOn':{'$gte': start}})
    #    crop_doctor = list(cursor)
    #    for node in crop_doctor:
    #        _id = str(node['_id']).split("'")[0]
    #        node.pop('_id')
    #        es.index(index='crop_doc_sync',id=_id,body=JSONEncoder().encode(node))
    #    
    #    print("Updated {} {}".format(len(crop_doctor),'crop_doctor')) 
    #    logger("Updated {} {}".format(len(crop_doctor),'crop_doctor')) 
    #    
    #    col = db.comments
    #    end=datetime.datetime(int(t[0]),int(t[1]),int(t[2]),int(t[3]),int(t[4]))
    #    delta=datetime.timedelta(minutes=2)
    #    start=end-delta
    #    cursor = col.find({'actionUpdatedAt':{'$gte': start}})
    #    comments = list(cursor)
    #    for node in comments:
    #        _id = str(node['_id']).split("'")[0]
    #        node.pop('_id')
    #        es.index(index='comments_sync',id=_id,body=JSONEncoder().encode(node))
    #    
    #    print("Updated {} {}".format(len(comments),'comments'))
    #    logger("Updated {} {}".format(len(comments),'comments'))
    #    
    #    print("Updates--- %s seconds ---" % (time.time() - start_time_updates))
    #    logger("Updates--- %s seconds ---" % (time.time() - start_time_updates))
        
    
    #    
    #    res=big_read('users_sync')
    #    users = json_normalize(list(res))
    #    users=users.drop('sort',axis=1)
    #    
    #    res=big_read('posts_sync')
    #    posts = json_normalize(list(res))
    #    posts=posts.drop('sort',axis=1)
    #    
    #    st=time.time()
    #    res=big_read('comments_sync')
    #    print("ES read--- %s seconds ---" % (time.time() - st))
    #    comments = json_normalize(list(res))
    #    print("ES read--- %s seconds ---" % (time.time() - st))
    #    comments=comments.drop('sort',axis=1)
    #    print("ES read--- %s seconds ---" % (time.time() - st))
    #    
    #    res=big_read('crops_sync')
    #    crops = json_normalize(list(res))
    #    crops=crops.drop('sort',axis=1)
    #    print("ES read--- %s seconds ---" % (time.time() - st))
        
        start_time_vectorization = time.time()
    
        crops=crops[crops['isDeleted']==False]
        posts.columns
    #    posts.columns=pd.Series(posts.columns).apply(lambda x:x.split('.')[-1])
    #    users.columns=pd.Series(users.columns).apply(lambda x:x.split('.')[-1])
    #    
        posts['x']=posts['locale'].apply(lambda x:x['location']['coordinates'][0])
        posts['y']=posts['locale'].apply(lambda x:x['location']['coordinates'][1])
        posts['language']=posts['locale'].apply(lambda x:x['language'])
        posts['cropId']=posts.postContext.apply(lambda x:x['cropId'])
        posts=posts[~posts['language'].isna()]
        posts_search=posts.copy()
        crop_dict=dict(zip(crops['_id'],crops['cropName']))
        posts_search['cropName']=posts['cropId'].map(crop_dict)
        username_dict=dict(zip(users['_id'],users['firstName']))
        posts_search['userName']=posts['userId'].map(username_dict)
        posts_search=posts_search.set_index('_id')
        posts_search=posts_search[['x','y','title','description','actionUpdatedAt','isDeleted','cropName', 'userName','language','userId']]
        
        index = 'posts_search_sync'
        doc_type = 'vec'
        ep = es_pandas(es_host)
        ep.init_es_tmpl(posts_search, doc_type)
        ep.to_es(posts_search, index, doc_type=doc_type, use_index=True)
        
        posts=posts.rename(columns={'_id':'postId'})
        
        
        
        posts=pd.concat([posts,pd.get_dummies(posts['language'])],axis=1)
        
        
        
        posts[list(crop_dict.values())]=0
        
        for row in posts.itertuples():
            pass
            posts.loc[row[0],crop_dict[row[24]]]=1
        
        cols_xy=['x','y']
        cols_xy.extend(posts['language'].unique().tolist())
        cols_xy.extend(list(crop_dict.values()))
        
        language=posts['language'].unique().tolist()
        crops_names=list(crop_dict.values())
        
        res=es.index(index='cols',id='aabab',body={'language':",".join(language),'crop_names':",".join(crops_names)})
        
        cols_non_xy=posts['language'].unique().tolist()
        cols_non_xy.extend(list(crop_dict.values()))
        posts['language'].value_counts()
        users.columns
    
        
        users=users[['_id', 'crops', 'userType','language', 'location']]
        
        users['x']=users['location'].apply(lambda x:x['coordinates'][0])
        users['y']=users['location'].apply(lambda x:x['coordinates'][1])
        users[list(crop_dict.values())]=0
        
        st=time.time()
        for row in users.itertuples():
            pass
            for c in row[2]:
                if c['cropId']!=None:
                    users.loc[row[0],crop_dict[str(c['cropId'])]]=1
        users=pd.concat([users,pd.get_dummies(users['language'])],axis=1)
        print(time.time()-st) 
    #    users[['en', 'hi', 'te']]=users[['en', 'hi', 'te']].replace(1,7)
        users.set_index('_id',inplace=True)
        user_dict=users[cols_non_xy].to_dict(orient='index')
        
    #    for i in user_dict.keys():
    #        user_dict[i]=np.array(list(user_dict[i].values()))
        for i in user_dict.keys():
            user_dict[i]=np.zeros((len(list(user_dict[i].values()))))
            
        user_df_profile=users[cols_xy]
    #    es_host = 'https://elastic:al8tPX2oS900b410YIg9QB2g@52.224.252.20:9200/'
        index = 'users_profile_vec'
        doc_type = 'vec'
        ep = es_pandas(es_host)
        ep.init_es_tmpl(user_df_profile, doc_type)
        ep.to_es(user_df_profile, index, doc_type=doc_type, use_index=True)
        
        st=time.time()
        c_up=0
        c_v=0
        c_uid=0
        miss_up=[]
        miss_v=[]
        miss_uid=[]
        for index,row in posts.iterrows():
            pass
            post_vec=np.array(row[cols_non_xy].tolist())
            
            if str(type(user_dict.get(row['userId'])))!="<class 'NoneType'>":
                    user_dict[row['userId']]=user_dict[row['userId']]+post_vec
            else:
                c_uid=c_uid+1
                miss_uid.append(row['userId'])
        
        
        
            for u in row['upvotes']:
                if str(type(user_dict.get(u)))!="<class 'NoneType'>":
                    user_dict[u]= user_dict[u]+post_vec
                else:
                    c_up=c_up+1
                    miss_up.append(u)
            
            for u in row['viewedUsers']:
                if str(type(user_dict.get(u)))!="<class 'NoneType'>":
                    user_dict[u]= user_dict[u]+post_vec
                else:
                    c_v=c_v+1
                    miss_v.append(u)
        print(time.time()-st)
        
        post_dict=posts[['postId']+cols_non_xy]
        post_dict.set_index('postId',inplace=True)
        post_dict=post_dict.to_dict(orient='index')
        
        for i in post_dict.keys():
            post_dict[i]=np.array(list(post_dict[i].values()))
        
        comments.columns=pd.Series(comments.columns).apply(lambda x:x.split('.')[-1])
        st=time.time()
        for index,row in comments.iterrows():
            pass
            if str(type(post_dict.get(row['postId'])))!="<class 'NoneType'>":
                post_vec=post_dict[row['postId']]
                user_dict[row['userId']]=user_dict[row['userId']]+post_vec
            
            for u in row['upvotes']:
                user_dict[u]= user_dict[u]+post_vec
        print(time.time()-st)
        
        user_df=pd.DataFrame.from_dict(user_dict,orient='index').reset_index()
        cols=['_id']+cols_non_xy
        user_df.columns=cols
        user_df=pd.concat([users[['x','y']].reset_index(drop=True),user_df],axis=1)
        user_df=user_df.set_index('_id')
        
        
        
    #    es_host = 'localhost:9200'
        index = 'users_activity_vec'
        doc_type = 'vec'
        ep = es_pandas(es_host)
        ep.init_es_tmpl(user_df, doc_type)
        ep.to_es(user_df, index, doc_type=doc_type, use_index=True)
        
        posts_vec=posts[['postId']+cols_xy+['isDeleted','actionUpdatedAt']].set_index('postId')
        index = 'posts_vec'
        doc_type = 'vec'
        ep = es_pandas(es_host)
        ep.init_es_tmpl(posts_vec, doc_type)
        ep.to_es(posts_vec, index, doc_type=doc_type, use_index=True)
        
        print("Vectorization--- %s seconds ---" % (time.time() - start_time_vectorization))
        
        start_time_cropdoc_vectorization = time.time()
    #        res = es.search(index="crop_doc_sync", body={"size":10000,"query":{"match_all":{}}})
    #        df = json_normalize(res['hits']['hits'])
    #    st=time.time()
    #    res=big_read('crop_doc_sync')
    #    print(time.time()-st)
    #    df = json_normalize(list(res))
    #    df=df.drop('sort',axis=1)
    #    print(time.time()-st)
        df=crop_doctor
        df['_id'].nunique()
        df=df[~df['diagnosisDetails'].isnull()]
        df['disease']=df['diagnosisDetails'].apply(lambda x:re.sub('[^A-Za-z0-9]+',' ',x[0]['causeInfo'][0]['name']))
        
        st=time.time()
        d={}
        for i in user_df.index.tolist():
            d[i]=[]
        
        for i,j in zip(df['userId'].tolist(),df['disease'].tolist()):
            d[i].append(j)
        
        for i in df['userId'].tolist():
            d[i]=[",".join(list(set(d[i])))]
        crop_doc_df=pd.DataFrame.from_dict(d,orient='index')
        print(time.time()-st)
    #        res=es.index(index='crop_doc_disease',id='oopp',body=d)
        index = 'crop_doc_disease'
        doc_type = 'vec'
        ep = es_pandas(es_host)
        ep.init_es_tmpl(crop_doc_df, doc_type)
        ep.to_es(crop_doc_df, index, doc_type=doc_type, use_index=True)
        print("CropDoc Vectorization--- %s seconds ---" % (time.time() - start_time_cropdoc_vectorization))
        print("Done")
        print("--- %s Total seconds ---" % (time.time() - start_time))
        
        logger("CropDoc Vectorization--- %s seconds ---" % (time.time() - start_time_cropdoc_vectorization))
        logger("Done")
        logger("--- %s Total seconds ---" % (time.time() - start_time))

    except Exception as e:
        logger(str(e))
        print(str(e))