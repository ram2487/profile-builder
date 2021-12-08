# -*- coding: utf-8 -*-
"""
Created on Fri Mar 12 20:40:48 2021

@author: Akash Raj
"""
from flask import Flask
app = Flask(__name__)
from elasticsearch import Elasticsearch
#es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
es = Elasticsearch([{'host': 'elastic-helm-elasticsearch-coordinating-only'}])
#es = Elasticsearch("https://elastic:al8tPX2oS900b410YIg9QB2g@52.224.252.20:9200/", verify_certs=False, connection_class=RequestsHttpConnection)
from nltk.corpus import stopwords
STOP_WORDS=stopwords.words('english')
from flask import request

@app.route('/search/', methods=['GET'])
def search():
    q= request.args.get('query')
    q=q.lower()
    size=int(request.args.get('size'))
    page=int(request.args.get('page'))
    q=[w for w in q.split() if not w in STOP_WORDS]
    should=[]
    for i in q:
        should.append({ 
              "fuzzy":{"title":  i  }          
            })
    for i in q:
        should.append({ 
              "fuzzy":{"cropName":  i  }          
            })
    for i in q:
        should.append({ 
              "fuzzy":{"userName":  i  }          
            })
    b={"size":100,
      "query": {
        "bool": {
          "should": should,
          "must":{'match':{'isDeleted':False }}
        }
      }
    }
    res= es.search(index='posts_search_sync',body=b)
    print("Keyword Search:\n")
    postIds=[]
    for hit in res['hits']['hits']:
        print(str(hit['_score']) + "\t" + hit['_source']['title'] +"\t"+hit['_source']['userName']+"\t" + hit['_source']['cropName'])
        postIds.append(hit['_id'])
    
    return {'postIds':postIds[size*(page-1):size*page]}
if __name__ == '__main__':

    app.run(host= '0.0.0.0',port=5670)