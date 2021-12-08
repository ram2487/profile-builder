# -*- coding: utf-8 -*-
"""
Created on Thu Feb  4 19:19:48 2021

@author: Akash Raj
"""
from flask import Flask
app = Flask(__name__)
import pandas as pd
import re
from elasticsearch import Elasticsearch
from nltk.corpus import stopwords
STOP_WORDS=stopwords.words('english')
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

res = es.search(index="crop_doc_sync", body={"size":10000,"query":{"match_all":{}}})
from pandas.io.json import json_normalize
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


@app.route('/cropdoc/<u>')
def cropDocRecommend(u):
    userId=u
    res = es.search(index="crop_doc_disease", body={"size":10000,"query":{"match_all":{}}})
    q=res['hits']['hits'][0]['_source'][userId]
    """3 latest disease searched by user in crop doc"""
    q=" ".join(q[-3:])
    #q='Vkasetu'
    
    q1=[w for w in q.split() if not w in STOP_WORDS]
    should1=[]
    for i in q1:
        should1.append({ 
              "fuzzy":{"title": {"value":i,"fuzziness":10}}          
            })
    b1={
      "query": {
        "bool": {
          "should": should1
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
          "should": should2
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
        p=es.search(index='posts_sync',body=b)['hits']['hits'][0]['_source']['title']
        comment_dict_list.append({'score_comment':hit['_score'],'postId':hit['_source']['postId'],'postTitle_comment':p,'comment':hit['_source']['comment']})
    
    
    q3=[w for w in q.split() if not w in STOP_WORDS]
    should3=[]
    for i in q3:
        should3.append({ 
              "fuzzy":{"description": {"value":i,"fuzziness":10}}          
            })
    b3={
      "query": {
        "bool": {
          "should": should3
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
    
    return {'postIds':final['postId'].tolist()}

if __name__ == '__main__':

    app.run(host= '0.0.0.0',port=5050)