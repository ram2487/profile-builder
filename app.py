"""dev_env_for_beta_app"""
code='dev_env_for_beta_app'
from elasticsearch import Elasticsearch
es = Elasticsearch([{'host': 'elastic-helm-elasticsearch-coordinating-only'}])
sample_user_id='607077a405164b0001e72f69'
log='https://apibeta.bighaat.com/crop/api/logerror/create-recommendation-error-log?message={}&api-version=1.0'

"""local_beta_app"""
#code='local_app_beta'
#from elasticsearch import Elasticsearch
#es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
#sample_user_id='607077a405164b0001e72f69'
#log='https://apibeta.bighaat.com/crop/api/logerror/create-recommendation-error-log?message={}&api-version=1.0'

"""local_prod_app"""
#code='local_app_prod'
#from elasticsearch import Elasticsearch
#es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
#sample_user_id='5fb0ced5cd15bf00013f68d0'
#log='https://api.bighaat.com/crop/api/logerror/create-recommendation-error-log?message={}&api-version=1.0'

"""beta_env_for_prod_app"""
#code='beta_env_for_prod_app'
#from elasticsearch import Elasticsearch
#es = Elasticsearch([{'host': 'elastic-helm-elasticsearch-coordinating-only'}])
#sample_user_id='5fb0ced5cd15bf00013f68d0'
#log='https://api.bighaat.com/crop/api/logerror/create-recommendation-error-log?message={}&api-version=1.0'

"""prod_env_for_prod_app"""
#code='prod_env_for_prod_app'
#from elasticsearch import Elasticsearch
#es = Elasticsearch([{'host': 'elastic-helm-elasticsearch-coordinating-only'}])
#sample_user_id='5fb0ced5cd15bf00013f68d0'
#log='https://api.bighaat.com/crop/api/logerror/create-recommendation-error-log?message={}&api-version=1.0'

auth={'apiKey': '135BE3F9-B1D8-4A14-A999-303286C32275'}

from flask import Flask
app = Flask(__name__)
from pandas.io.json import json_normalize
#import numpy as np
import pandas as pd
from nltk.corpus import stopwords
#import random
import time
#from datetime import datetime,timedelta
STOP_WORDS=stopwords.words('english')

#es = Elasticsearch('https://prodelastic.bighaat.cf/')

#es = Elasticsearch([{'host': 'elastic-helm-elasticsearch-coordinating-only'}])
#es = Elasticsearch("https://elastic:al8tPX2oS900b410YIg9QB2g@52.224.252.20:9200/", verify_certs=False, connection_class=RequestsHttpConnection)
from flask import request
import warnings
warnings.filterwarnings("ignore")
from elasticsearch.helpers import scan
import requests
import datetime


def logger(message):
    message_dict={
       'code':code,
       'message':message}
    r=requests.post(log.format(str(message_dict)),headers=auth)
    return r.text  
def big_read(index):
    es_response = scan(
        es,
        index=index,
        doc_type='_doc',
        query={"query": { "match_all" : {}}}
    )
    return es_response

#global_user={}
 
#res = es.search(index="users_sync", body={"size":10000,"query":{"match_all":{}}})
#users = json_normalize(res['hits']['hits'])

def cropDocRecommend(u):
    try:
        logger("CropDoc started for user:"+u)
        userId=u
        b={
        'query':{
            'match':{"_id": userId}
            }
        }
        res = es.search(index="crop_doc_disease", body=b)
        q=res['hits']['hits'][0]['_source']['0']
        if q==None:
            logger("Exit CropDoc for user:"+u+"with 0 results")
            return {'total_cd':[]}
        """3 latest disease searched by user in crop doc"""
#        q=",".join(q[-3:])
        q=q.split(",")
        if ' ' in q:
            q.remove(' ')
        q=q[-3:]
        #q='Vkasetu'
            
        q1=[w for w in q if not w in STOP_WORDS]
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
        res1= es.search(index='posts_search_sync',body=b1)
        print("Keyword Search:\n")
        for hit in res1['hits']['hits']:
    #        print(str(hit['_score']) + "\t" + hit['_source']['title'])
            post_dict_list.append({'score_post':hit['_score'],'postId':hit['_id'],'postTitle_post':hit['_source']['title']})
            
        q2=[w for w in q if not w in STOP_WORDS]
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
    #        print(str(hit['_score']) + "\t" + hit['_source']['postId']+"\t" + hit['_source']['comment'] )
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
        
        q3=[w for w in q if not w in STOP_WORDS]
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
        res3= es.search(index='posts_search_sync',body=b3)
    #    print("Keyword Search:\n")
        for hit in res3['hits']['hits']:
    #        print(str(hit['_score']) + "\t" + hit['_source']['title'])
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
    
    
    #    final=final[start:fraction_size]
    #    print(final['postId'].tolist(),'cropdoc')
        logger("Exit CropDoc for user:"+u+"with"+str(len(final['postId']))+" "+str(final['postId']))
        return {'total_cd':final['postId'].tolist()}
    except Exception as e:
        logger("Some error in corpdoc "+str(e))

def user_profile_activity(posts_vec,u):
    try:
        logger("Started Profile_activity recom for usser"+u)      
        user_id=u
    #    logger(str(total_cd))
        b={
        'query':{
            'match':{"_id": user_id}
            }
        }
        
        vec_user=es.search(body=b,index='users_profile_vec')['hits']['hits'][0]['_source']
        vec_user_activity=es.search(body=b,index='users_activity_vec')['hits']['hits'][0]['_source']
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
        if lan.sum().sum()!=0:
            lan=lan.div(lan.sum(axis=1),axis=0)
        crops=pd.DataFrame.from_dict(vec_user)[crop_names]
        if crops.sum().sum()!=0:
            crops=crops.div(crops.sum(axis=1),axis=0)
        
        vec_user=pd.concat([pd.DataFrame.from_dict(vec_user)[['x','y']],lan,crops],axis=1)
        
        for i in vec_user_activity:
            pass
            vec_user_activity[i]=[vec_user_activity[i]]
            
        lan=pd.DataFrame.from_dict(vec_user_activity)[language]
        
        if lan.sum().sum()!=0:
            lan=lan.div(lan.sum(axis=1),axis=0)
        crops=pd.DataFrame.from_dict(vec_user_activity)[crop_names]
        if crops.sum().sum()!=0:
            crops=crops.div(crops.sum(axis=1),axis=0)
        vec_user_activity=pd.concat([pd.DataFrame.from_dict(vec_user_activity)[['x','y']],lan,crops],axis=1)
        
        
        #    res = es.search(index="posts_search_sync", body={"size":10000,"query":{"match_all":{}}})
        #    posts_vec = json_normalize(res['hits']['hits'])
        #    posts_vec.columns=pd.Series(posts_vec.columns).apply(lambda x:x.split('.')[-1])
        #    posts_vec=posts_vec[posts_vec['isDeleted']==False]
        
        vec_user[language+crop_names]=vec_user[language+crop_names]+vec_user_activity[language+crop_names]
       
            
    #        l=vec_user[language].sample(1,weights=vec_user[language].values[0],axis=1).columns[0]
    #        c=vec_user[crop_names].sample(1,weights=vec_user[crop_names].values[0],axis=1).columns[0]
        
        crop_list=vec_user[crop_names].T.sort_values(by=0,ascending=False)
        crop_list=crop_list[crop_list!=0].dropna().index.tolist()
        language_list=vec_user[language].T.sort_values(by=0,ascending=False)
        language_list=language_list[language_list!=0].dropna().index.tolist()
        pv=posts_vec.copy()
        pv=pv[pv['userId']!=u]
    #    l2_list=[]
    #    for l in language_list:
    #        l1_list=[]
    #        for c in crop_list:
    #            p=pv[(pv['language']==l) & (pv['cropName']==c)]
    #            p=p.sort_values(by='actionUpdatedAt',ascending=False)
    #            l1_list.append(p['_id'].tolist())
    #        l2_list.append(l1_list)
    #            if len(p)!=0:
    #                try:
    #                    post_list.extend(p['_id'].tolist())
    #                except:
    #                    pass
    #    user_own_post=posts_vec.copy()
    #    user_own_post=user_own_post[user_own_post['userId']==u]
    #    user_own_post['actionUpdatedAt']=pd.to_datetime(user_own_post['actionUpdatedAt'])
    #    user_own_post=user_own_post[user_own_post['actionUpdatedAt']-datetime.utcnow()<timedelta(days=3)]
    #    user_own_post_list=user_own_post['_id'].tolist()[:5]
        p=pd.DataFrame()
        for l in language_list:
    
            for c in crop_list:
                pt=pv[(pv['language']==l) & (pv['cropName']==c)]
                p=pd.concat([p,pt])
        p=p.sort_values(by='actionUpdatedAt',ascending=False)
        
        
        posts=p
        
    #    for l1 in l2_list:
    #        len_list=[]
    #        for i in l1:
    #            len_list.append(len(i))
    #            m=max(len_list)
    #        for i in range(m):
    #            for j in l1:
    #                if i<len(j):
    #                    post_list.append(j[i])
        
        if crop_list==[]:
            posts=pv[pv.language.isin(language_list)].sort_values(by='actionUpdatedAt',ascending=False)
    
    #    print(total_cd)
    #    post_list=[x for x in post_list if x not in total_cd+global_user[u]]
    #    for post in post_list[start:]:
    #        if post not in total_cd+global_user[u]:
    #            final.append(post)
    #        if len(final)==fraction_size-start:
    #            break
    #    print(final)
    #    post_list=user_own_post_list+post_list
        posts_crop_doc_list=cropDocRecommend(u)['total_cd']
        posts_crop_doc=pv[pv['_id'].isin(posts_crop_doc_list)]
        posts_act_doc=pd.concat([posts,posts_crop_doc])
        posts_act_doc=posts_act_doc.drop_duplicates()
        posts_act_doc=posts_act_doc.sort_values(by='actionUpdatedAt',ascending=False)
        
        all_posts=pv.sort_values(by='actionUpdatedAt',ascending=False)
        posts_language=pv[pv.language.isin(language_list)].sort_values(by='actionUpdatedAt',ascending=False)
        pre_final=pd.concat([posts_act_doc,posts_language])
        pre_final=pre_final.drop_duplicates()
        
        final_df=pd.concat([posts_act_doc,posts_language,all_posts])
        final_df=final_df.drop_duplicates()
        logger("Ended Profile_activity recom for user "+u+" with "+str(len(posts_act_doc))+" posts")
        return {'pre_final':pre_final,'postIds':final_df['_id'].tolist(),'language_list':language_list,'crop_list':crop_list}
    except Exception as e:
        logger("Some error in activity "+str(e))
        print(e)
#res=big_read('users_profile_vec')
#users = json_normalize(list(res))
#users=users.drop('sort',axis=1)
#for i in users['_id'].tolist():
#    post_dict={}
#    global_user[i]=[]
res=big_read('posts_search_sync')
posts_vec = json_normalize(list(res))
posts_vec=posts_vec.drop('sort',axis=1)
posts_vec.columns=pd.Series(posts_vec.columns).apply(lambda x:x.split('.')[-1])
posts_vec=posts_vec[posts_vec['isDeleted']==False].reset_index(drop=True)



logger("Sample for user 5fb0ced5cd15bf00013f68d0")
post_sample_dict={}

up=user_profile_activity(posts_vec,sample_user_id)
post_sample_dict['postIds']=",".join(up['postIds'])
post_sample_dict['time']=time.time()
post_sample_dict['crop_list']=",".join(up['crop_list'])
post_sample_dict['language_list']=",".join(up['language_list'])
res=es.index(index='user_posts_list',id=sample_user_id,body=post_sample_dict)

@app.route('/recommend/', methods=['GET'])
def recommend():
    try:
        
    #        global global_user
        user_id=request.args.get('user')
        size=int(request.args.get('size'))
        page=int(request.args.get('page'))
#        disease_count=int(request.args.get('disease_count'))
#        fraction_cropDoc=float(request.args.get('cropdoc'))
        logger("started recom for user "+user_id+" for page "+str(page))
        
        res=big_read('posts_search_sync')
        posts_vec = json_normalize(list(res))
        posts_vec=posts_vec.drop('sort',axis=1)
        posts_vec.columns=pd.Series(posts_vec.columns).apply(lambda x:x.split('.')[-1])
        posts_vec=posts_vec[posts_vec['isDeleted']==False].reset_index(drop=True)
        
        b={
        'query':{
            'match':{"_id": user_id}
            }
        }
        
        user_posts_list=es.search(body=b,index='user_posts_list')['hits']['hits']
        if len(user_posts_list)!=0:
                
            if time.time()-user_posts_list[0]['_source']['time']<=300:
                up=user_posts_list[0]['_source']
                f=up['postIds'].split(",")
                c=up['crop_list'].split(",")
                l=up['language_list'].split(",")
                logger("Updated Data in es exits, so using it "+user_id)
                print("Updated Data in es exits, so using it "+user_id)
            else:
                logger("Data in es exits, but old so creating for user "+user_id)
                print("Data in es exits, but old so creating for user "+user_id)
                post_sample_dict={}
                up=user_profile_activity(posts_vec,user_id)
                f=up['postIds']
                c=up['crop_list']
                l=up['language_list']
                post_sample_dict['postIds']=",".join(up['postIds'])
                post_sample_dict['time']=time.time()
                post_sample_dict['crop_list']=",".join(up['crop_list'])
                post_sample_dict['language_list']=",".join(up['language_list'])
                res=es.index(index='user_posts_list',id=user_id,body=post_sample_dict)

        else:
            logger("No data in es exits, creating for user "+user_id)  
            print("No data in es exits, creating for user "+user_id)
            post_sample_dict={}
            up=user_profile_activity(posts_vec,user_id)
            f=up['postIds']
#            f=up['postIds']
            c=up['crop_list']
            l=up['language_list']
            post_sample_dict['postIds']=",".join(up['postIds'])
            post_sample_dict['time']=time.time()
            post_sample_dict['crop_list']=",".join(up['crop_list'])
            post_sample_dict['language_list']=",".join(up['language_list'])
            res=es.index(index='user_posts_list',id=user_id,body=post_sample_dict)
#        if(page==1):
#            global_user[user_id]=[]
#            logger("Set the glober user as len "+str(len(global_user[user_id]))+" as page is 1"+" for user "+user_id)
#    #    fraction_activity=float(request.args.get('activity'))
#    #    fraction_profile=float(request.args.get('profile'))
#    #    fraction_location=float(request.args.get('location'))
#    #    fraction_latest=float(request.args.get('latest'))
#        rt=[]

##        cd=cropDocRecommend(user_id)
#        for i in cd['total_cd']:
#            if i not in global_user[user_id]:
#                rt.append(i)
#                logger("size of crop_doc results is "+str(len(rt))+" for user "+user_id)
#            if len(rt)>=int(size*fraction_cropDoc):
#                break
#        prev=[]
#        for i in rt:
#            prev.append(i)
#        logger("Addded crop doc results of length "+str(len(rt))+" for user "+user_id)
#        if len(rt)==0:
#            fraction_activity=1
#        else:
#            fraction_activity=1-fraction_cropDoc
        
        
#        res = es.search(index="posts_search_sync", body={"size":10000,"query":{"match_all":{}}})
#        posts_vec = json_normalize(res['hits']['hits'])
        
        
        logger("Post df created"+" for user "+user_id)
#        act=user_profile_activity(posts_vec,user_id)
        
#        for i in act['postIds']:
#            if i not in global_user[user_id]+prev:
#                rt.append(i)
#                logger("size of profile_activity results is "+str(len(rt))+" for user "+user_id)
#            if len(rt)==10:
#                break
        crop_dict=dict(zip(posts_vec['_id'],posts_vec['cropName']))
        user_dict=dict(zip(posts_vec['_id'],posts_vec['userName']))
        language_dict=dict(zip(posts_vec['_id'],posts_vec['language']))
        date_dict=dict(zip(posts_vec['_id'],posts_vec['actionUpdatedAt']))
#        m=max(len(cd['total_cd'])
#        final=[]
#        for i in range(len(act['postIds'])):
#            if i<len(cd['total_cd']):
#                final.append(cd['total_cd'][i])
#            final.append(act['postIds'][i])
#        final=list(set(final))
#        ret=act['postIds'][size*(page-1):size*page]
    #    print(rt)
        ret=f[size*(page-1):size*page]
        posts_comb=[]
        for i in ret:
            posts_comb.append((crop_dict[i],user_dict[i],language_dict[i],date_dict[i]))
        crop_name=[]
        for i in ret:
            crop_name.append(crop_dict[i])
     
        
        user_name=[]
        for i in ret:
            user_name.append(user_dict[i])
        
        language=[]
        for i in ret:
            language.append(language_dict[i])
        date=[]
        for i in ret:
            date.append(date_dict[i])
#        global_user[user_id].extend(rt)
        
#        print(len(set(rt)),len(set(global_user[user_id])),rt,global_user[user_id])
#        cd_count=len(cd['total_cd'])
#        cd_unique=len(set(cd['total_cd']))
#        act_pro=len(act['postIds'])
#        act_pro_unique=len(set(act['postIds']))
#        meta1=[cd_count,cd_unique,act_pro,act_pro_unique]
#        meta2=[len(set(rt)),len(set(global_user[user_id])),rt,global_user[user_id]]
#        
#        logger("Meta info 1 is"+str(meta1)+" for user "+user_id)
#        logger("Meta info 2 is"+str(meta1)+" for user "+user_id)
        logger("Ending Recommendation for user "+user_id)
        
        """Crop Doc Disease"""
        b={
        'query':{
            'match':{"_id": user_id}
            }
        }
        res = es.search(index="crop_doc_disease", body=b)
        q=res['hits']['hits'][0]['_source']['0']
        if q==None:
            cd_disease=[]
        """10 latest disease searched by user in crop doc. It will be same for every page"""
        q=q.split(",")
        if ' ' in q:
            q.remove(' ')
        cd_disease=q[-10:]
        return {'cd_disease':cd_disease,'cropName':crop_name,'user':user_name,'language':language,'date':date,'postIds':ret,'version':6,'len':len(list(set(ret))),'x_lang':l,'x_crop':c}
    except Exception as e:
        logger('Error in recommendation '+str(e))
        print('Error in recommendation '+str(e))
        
@app.route('/refresh/', methods=['GET'])
def refresh():
    user_id=request.args.get('user')
    from_date=request.args.get('from_date')
    to_date=request.args.get('to_date')
#    print(from_date)
#    return {'user':1}
#    from_date='2020-10-25 14:43:41.457000'
#    to_date='2020-11-25 14:43:41.457000'
    from_date=datetime.datetime.strptime(from_date, '%Y-%m-%d %H:%M:%S.%f')
    to_date=datetime.datetime.strptime(to_date, '%Y-%m-%d %H:%M:%S.%f')
    res=big_read('posts_search_sync')
    posts_vec = json_normalize(list(res))
    posts_vec=posts_vec.drop('sort',axis=1)
    posts_vec.columns=pd.Series(posts_vec.columns).apply(lambda x:x.split('.')[-1])
    posts_vec=posts_vec[posts_vec['isDeleted']==False].reset_index(drop=True)
    data=user_profile_activity(posts_vec,user_id)['pre_final']
    data=data[(pd.to_datetime(data['actionUpdatedAt'])>from_date) & (pd.to_datetime(data['actionUpdatedAt'])<to_date)]
    b={
        'query':{
            'match':{"_id": user_id}
            }
    }
        
    user_posts_list=es.search(body=b,index='user_posts_list')['hits']['hits']
    if time.time()-user_posts_list[0]['_source']['time']<=300:
        logger('refresh-api '+str({'posts_no':0,'userid':user_id}))
        return {'posts_no':0}
    else:
        logger('refresh-api '+str({'posts_no':len(data),'userid':user_id}))
        return {'posts_no':len(data)}
"""
user_id='5f4f300071a0b4478c550d55'
user_id='607077a405164b0001e72f69'
size=10
page=1
fraction_cropDoc=0.2
'meta2':meta2,'meta1':meta1,
'cropDoc':len(cd['total_cd']),
"""
#p=recommend('5f4f300071a0b4478c550d55')
#cropDocRecommend('607077a405164b0001e72f69')
if __name__ == '__main__':
    app.run(host= '0.0.0.0',port=5600)