# -*- coding: utf-8 -*-
"""
Created on Wed Mar  3 19:03:25 2021

@author: Akash Raj
"""

from flask import Flask
app = Flask(__name__)
from pandas.io.json import json_normalize
from elasticsearch import Elasticsearch
import numpy as np
import pandas as pd
from nltk.corpus import stopwords
import random
STOP_WORDS=stopwords.words('english')
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
from flask import request
import warnings
warnings.filterwarnings("ignore")

def cropDocRecommend(u,start,fraction_size):
    userId=u
    b={
    'query':{
        'match':{"_id": "oopp"}
        }
    }
    res = es.search(index="crop_doc_disease", body=b)
    q=res['hits']['hits'][0]['_source'][userId]
    """3 latest disease searched by user in crop doc"""
    q=" ".join(q[-3:])
    #q='Vkasetu'
    if q=='':
        return {'postIds':[]}
    q1=[w for w in q.split() if not w in STOP_WORDS]
    should1=[]
    for i in q1:
        should1.append({ 
              "fuzzy":{"title": {"value":i,"fuzziness":10}}          
            })
    b1={
      "query": {
        "bool": {
          "should": should1,
          "must":{'match':{'isDeleted':False }}
        }
      }
    }
    
    post_dict_list=[]
    res1= es.search(index='posts_sync',body=b1)
    print("Keyword Search:\n")
    for hit in res1['hits']['hits']:
        print(str(hit['_score']) + "\t" + hit['_source']['title'])
        post_dict_list.append({'score_post':hit['_score'],'postId':hit['_id'],'postTitle_post':hit['_source']['title']})
        
    q2=[w for w in q.split() if not w in STOP_WORDS]
    should2=[]
    for i in q2:
        should2.append({ 
              "fuzzy":{"comment": {"value":i,"fuzziness":10}}          
            })
    b2={
      "query": {
        "bool": {
          "should": should2,
          "must":{'match':{'isDeleted':False }}
        }
      }
    }
    
    comment_dict_list=[]=[]
    res2= es.search(index='comments_sync',body=b2)
    for hit in res2['hits']['hits']:
        print(str(hit['_score']) + "\t" + hit['_source']['postId']+"\t" + hit['_source']['comment'] )
        b={
        'query':{
            'match':{"_id":hit['_source']['postId'] }
            }
        }
        try:
            p=es.search(index='posts_sync',body=b)['hits']['hits'][0]['_source']['title']
            comment_dict_list.append({'score_comment':hit['_score'],'postId':hit['_source']['postId'],'postTitle_comment':p,'comment':hit['_source']['comment']})
        except:
            print("Comment's post is deleted")
    
    q3=[w for w in q.split() if not w in STOP_WORDS]
    should3=[]
    for i in q3:
        should3.append({ 
              "fuzzy":{"description": {"value":i,"fuzziness":10}}          
            })
    b3={
      "query": {
        "bool": {
          "should": should3,
          "must":{'match':{'isDeleted':False }}
        }
      }
    }
    
    description_dict_list=[]
    res3= es.search(index='posts_sync',body=b3)
    print("Keyword Search:\n")
    for hit in res3['hits']['hits']:
        print(str(hit['_score']) + "\t" + hit['_source']['title'])
        description_dict_list.append({'score_description':hit['_score'],'postId':hit['_id'],'postTitle_description':hit['_source']['title'],'description':hit['_source']['description']})
    
    if len(post_dict_list)!=0:
        df1=pd.DataFrame(post_dict_list)
    else:
        df1=pd.DataFrame(columns=['score_post', 'postId', 'postTitle_post'])
        
    if len(description_dict_list)!=0:
        df2=pd.DataFrame(description_dict_list)
    else:
        df2=pd.DataFrame(columns=['score_description', 'postId', 'postTitle_description','description'])
    
    if len(comment_dict_list)!=0:
        df3=pd.DataFrame(comment_dict_list)
    else:
        df3=pd.DataFrame(columns=['score_comment', 'postId', 'postTitle_comment', 'comment'])
    
    
    combo_pcd=df1.merge(df2,how='inner',on='postId').merge(df3,how='inner',on='postId')
    combo_pd=df1.merge(df2,how='inner',on='postId')
    combo_pc=df1.merge(df3,how='inner',on='postId')
    combo_cd=df2.merge(df3,how='inner',on='postId')
    
    id_top=list(combo_pcd['postId'])+list(combo_pd['postId'])+list(combo_pc['postId'])+list(combo_cd['postId'])
    
    combo=pd.concat([combo_pcd,combo_pd,combo_pc,combo_cd])
    
    df1=df1[df1['postId'].apply(lambda x: x  not in id_top)]
    df2=df2[df2['postId'].apply(lambda x: x  not in id_top)]
    df3=df3[df3['postId'].apply(lambda x: x  not in id_top)]
    
    final=pd.concat([combo,df1,df2,df3])
    final=final.iloc[start:start+fraction_size,:]
    print(final['postId'].tolist(),'cropdoc')
    return {'postIds':final['postId'].tolist()}


def user_profile(u,start,fraction_size):
    from scipy.spatial import distance
    user_id=u
    
    b={
    'query':{
        'match':{"_id": user_id}
        }
    }
    
    vec_user=es.search(body=b,index='users_profile_vec')['hits']['hits'][0]['_source']
    
    b={
    'query':{
        'match':{"_id": "aabab"}
        }
    }
    
    col_names=es.search(index="cols", body=b)['hits']['hits'][0]['_source']
    language=col_names['language'].split(",")
    crop_names=col_names['crop_names'].split(",")
    for i in vec_user:
        pass
        vec_user[i]=[vec_user[i]]
        
    lan=pd.DataFrame.from_dict(vec_user)[language]
    lan=lan.div(lan.sum(axis=1),axis=0)
    crops=pd.DataFrame.from_dict(vec_user)[crop_names]
    crops=crops.div(crops.sum(axis=1),axis=0)
    
    vec_user=pd.concat([pd.DataFrame.from_dict(vec_user)[['x','y']],lan,crops],axis=1)
    res = es.search(index="posts_vec", body={"size":10000,"query":{"match_all":{}}})
    posts_vec = json_normalize(res['hits']['hits'])
    posts_vec.columns=pd.Series(posts_vec.columns).apply(lambda x:x.split('.')[-1])
    posts_vec=posts_vec[posts_vec['isDeleted']==False]
    posts_vec=posts_vec.sort_values(by='updatedAt',ascending=False)
    vector_posts=posts_vec.drop(['_index','_type','_id','_score','isDeleted'],axis=1)
    

    score=[]
    for i in range(len(vector_posts)):
        pass
        dst = distance.euclidean(np.array(vector_posts[language+crop_names].iloc[i,:]), np.array(list(vec_user[language+crop_names].iloc[0,:])))
        score.append(dst)
    post_res=posts_vec[['_id','x','y']]
    post_res['score']=score
    post_res.sort_values(by='score',inplace=True)

    post_res=post_res.iloc[start:start+fraction_size,:]
    
#    vector_posts_xy=post_res.iloc[start:start+fraction_size,:][['x','y']]
#    user_xy=np.array(list(vec_user.iloc[0,:])[:2])
#    
#    score_xy=[]
#    for i in range(len(vector_posts_xy)):
#        dst = distance.euclidean(vector_posts_xy.iloc[i,:], user_xy)
#        score_xy.append(dst)
#    
#    post_res['score_xy']=score_xy
#    post_res.sort_values(by='score_xy',inplace=True)
    print(post_res['_id'].tolist(),'profile')
    return {'postIds':post_res['_id'].tolist()}

def user_activity(u,start,fraction_size):
    from scipy.spatial import distance
    user_id=u
    
    b={
    'query':{
        'match':{"_id": user_id}
        }
    }
    
    vec_user=es.search(body=b,index='users_activity_vec')['hits']['hits'][0]['_source']
    
    b={
    'query':{
        'match':{"_id": "aabab"}
        }
    }
    
    col_names=es.search(index="cols", body=b)['hits']['hits'][0]['_source']
    language=col_names['language'].split(",")
    crop_names=col_names['crop_names'].split(",")
    for i in vec_user:
        pass
        vec_user[i]=[vec_user[i]]
        
    lan=pd.DataFrame.from_dict(vec_user)[language]
    lan=lan.div(lan.sum(axis=1),axis=0)
    crops=pd.DataFrame.from_dict(vec_user)[crop_names]
    crops=crops.div(crops.sum(axis=1),axis=0)
    
    vec_user=pd.concat([pd.DataFrame.from_dict(vec_user)[['x','y']],lan,crops],axis=1)
    res = es.search(index="posts_vec", body={"size":10000,"query":{"match_all":{}}})
    posts_vec = json_normalize(res['hits']['hits'])
    posts_vec.columns=pd.Series(posts_vec.columns).apply(lambda x:x.split('.')[-1])
    posts_vec=posts_vec[posts_vec['isDeleted']==False]
    posts_vec=posts_vec.sort_values(by='updatedAt',ascending=False)
    vector_posts=posts_vec.drop(['_index','_type','_id','_score','isDeleted'],axis=1)
    

    score=[]
    for i in range(len(vector_posts)):
        pass
        dst = distance.euclidean(np.array(vector_posts[language+crop_names].iloc[i,:]), np.array(list(vec_user[language+crop_names].iloc[0,:])))
        score.append(dst)
    post_res=posts_vec[['_id','x','y']]
    post_res['score']=score
    post_res.sort_values(by='score',inplace=True)

    post_res=post_res.iloc[start:start+fraction_size,:]
    print(post_res['_id'].tolist(),'activity')
    return {'postIds':post_res['_id'].tolist()}

def user_location(u,start,fraction_size):
    from scipy.spatial import distance
    user_id=u
    
    b={
    'query':{
        'match':{"_id": user_id}
        }
    }
    
    vec_user=es.search(body=b,index='users_activity_vec')['hits']['hits'][0]['_source']
    
    b={
    'query':{
        'match':{"_id": "aabab"}
        }
    }
    
    col_names=es.search(index="cols", body=b)['hits']['hits'][0]['_source']
    language=col_names['language'].split(",")
    crop_names=col_names['crop_names'].split(",")
    for i in vec_user:
        pass
        vec_user[i]=[vec_user[i]]
        
    lan=pd.DataFrame.from_dict(vec_user)[language]
    lan=lan.div(lan.sum(axis=1),axis=0)
    crops=pd.DataFrame.from_dict(vec_user)[crop_names]
    crops=crops.div(crops.sum(axis=1),axis=0)
    
    vec_user=pd.concat([pd.DataFrame.from_dict(vec_user)[['x','y']],lan,crops],axis=1)
    res = es.search(index="posts_vec", body={"size":10000,"query":{"match_all":{}}})
    posts_vec = json_normalize(res['hits']['hits'])
    posts_vec.columns=pd.Series(posts_vec.columns).apply(lambda x:x.split('.')[-1])
    posts_vec=posts_vec[posts_vec['isDeleted']==False]
    posts_vec=posts_vec.sort_values(by='updatedAt',ascending=False)
    vector_posts=posts_vec.drop(['_index','_type','_id','_score','isDeleted'],axis=1)
    

    score=[]
    for i in range(len(vector_posts)):
        pass
        dst = distance.euclidean(np.array(vector_posts[['x','y']].iloc[i,:]), np.array(list(vec_user[['x','y']].iloc[0,:])))
        score.append(dst)
    post_res=posts_vec[['_id','x','y']]
    post_res['score']=score
    post_res.sort_values(by='score',inplace=True)

    post_res=post_res.iloc[start:start+fraction_size,:]
    print(post_res['_id'].tolist(),'location')
    return {'postIds':post_res['_id'].tolist()}

def latest_posts(u,start,fraction_size):
    from scipy.spatial import distance
    user_id=u
    
    b={
    'query':{
        'match':{"_id": user_id}
        }
    }
    
    vec_user=es.search(body=b,index='users_activity_vec')['hits']['hits'][0]['_source']
    
    b={
    'query':{
        'match':{"_id": "aabab"}
        }
    }
    
    col_names=es.search(index="cols", body=b)['hits']['hits'][0]['_source']
    language=col_names['language'].split(",")
    crop_names=col_names['crop_names'].split(",")
    for i in vec_user:
        pass
        vec_user[i]=[vec_user[i]]
        
    lan=pd.DataFrame.from_dict(vec_user)[language]
    lan=lan.div(lan.sum(axis=1),axis=0)
    crops=pd.DataFrame.from_dict(vec_user)[crop_names]
    crops=crops.div(crops.sum(axis=1),axis=0)
    
    vec_user=pd.concat([pd.DataFrame.from_dict(vec_user)[['x','y']],lan,crops],axis=1)
    res = es.search(index="posts_vec", body={"size":10000,"query":{"match_all":{}}})
    posts_vec = json_normalize(res['hits']['hits'])
    posts_vec.columns=pd.Series(posts_vec.columns).apply(lambda x:x.split('.')[-1])
    posts_vec=posts_vec[posts_vec['isDeleted']==False]
    posts_vec=posts_vec.sort_values(by='updatedAt',ascending=False)
#    vector_posts=posts_vec.drop(['_index','_type','_id','_score','isDeleted'],axis=1)
#    
#
#    score=[]
#    for i in range(len(vector_posts)):
#        pass
#        dst = distance.euclidean(np.array(vector_posts[['x','y']].iloc[i,:]), np.array(list(vec_user[['x','y']].iloc[0,:])))
#        score.append(dst)
#    post_res=posts_vec[['_id','x','y']]
#    post_res['score']=score
#    post_res.sort_values(by='score',inplace=True)

    post_res=posts_vec.iloc[start:start+fraction_size,:]
    print(post_res['_id'].tolist(),'latest')
    return {'postIds':post_res['_id'].tolist()}

@app.route('/recommend/', methods=['GET'])
def recommend():
    user_id=request.args.get('user')
    size=int(request.args.get('size'))
    page=int(request.args.get('page'))
    fraction_cropDoc=float(request.args.get('cropdoc'))
    fraction_activity=float(request.args.get('activity'))
    fraction_profile=float(request.args.get('profile'))
    fraction_location=float(request.args.get('location'))
    fraction_latest=float(request.args.get('latest'))
    
    cd=cropDocRecommend(user_id,int(size*(page-1)*fraction_cropDoc),int(size*page*fraction_cropDoc))['postIds']
    if cd==[]:
        fraction_activity=fraction_activity+fraction_cropDoc
    act=user_activity(user_id,int(size*(page-1)*fraction_activity),int(size*page*fraction_activity))['postIds']
    profile=user_profile(user_id,int(size*(page-1)*fraction_profile),int(size*page*fraction_profile))['postIds']
    location=user_location(user_id,int(size*(page-1)*fraction_location),int(size*page*fraction_location))['postIds']
    latest=latest_posts(user_id,int(size*(page-1)*fraction_latest),int(size*page*fraction_latest))['postIds']
    
#    act=user_profile_activity(user_id,size*(page-1),size*page)['postIds']
#    cd.extend(act)
#    cd=list(set(cd))
#    rt=cd[size*(page-1):size*page]
    rt=cd+act+profile+location+latest
    random.shuffle(rt)
    return {'postIds':rt}
#p=recommend('5f732fa0b6a715000181f528')
if __name__ == '__main__':
    app.run(host= '0.0.0.0',port=5600)