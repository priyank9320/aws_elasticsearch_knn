import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
from pprint import pprint
import os

def hello(event, context):

    access_key=os.environ['access_key']
    secret_key=os.environ['secret_key']
    region= 'eu-west-2' # us-east-1
    service = 'es' # 



    proxy_endpoint = 'search-company-sim-4k7ovt44tpdnhmpudn73oyf5cu.eu-west-2.es.amazonaws.com/' # For example, foo.execute-api.us-east-1.amazonaws.com/prod
    endpoint_parts = proxy_endpoint.split('/')
    host = endpoint_parts[0] # For example, foo.execute-api.us-east-1.amazonaws.com
    url_prefix = endpoint_parts[1]


    #credentials = boto3.Session().get_credentials()
    awsauth = AWS4Auth(access_key, secret_key, region, service)

    es = Elasticsearch(
        hosts = [{'host': host, 'url_prefix': url_prefix, 'port': 443}],
        http_auth = awsauth,
        use_ssl = True,
        verify_certs = True,
        connection_class = RequestsHttpConnection
    )

    reg=event["queryStringParameters"]["reg"] # extract the reg number from the  'event' that is received

    # ENTER THE REG NUMBER AND IT FINDS THE VECTOR FOR THAT COMPANY
    doc = {
        'size' : 10,
        'query': {
            'match' : {
                'reg': reg
            }
        }
    }

    res=es.search(index='cmp-sim', body=doc)
    serach_vec=res['hits']['hits'][0]['_source']['vectors'] # extract the vector that was received for the reg number

    # RUNNING KNN ON 5K RECORDS

    #GET
    # THIS WORKS
    query={
    "size": 10,
    "query": {
        "knn": {
        "vectors": {
            "vector": serach_vec,
            "k": 10
        }
        }
    }
    }


    res=es.search(index='cmp-sim' ,body=query)
    # prints the name of the companies in the results returned

    output=[]
    for r in res['hits']['hits']:
        output_elmnt={}
        output_elmnt['name']=r['_source']['name']
        output_elmnt['link']=r['_source']['link']
        output_elmnt['reg']=r['_source']['reg']
        output_elmnt['match_score']=r['_score']
        output.append(output_elmnt)

    output


    body = { 
        "message": "Go Serverless v1.0! Your function executed successfully!",
        "output": output
    }

    response = {
        "statusCode": 200,
        "body": json.dumps(body)
    }

    return response

    # Use this code if you don't use the http event with the LAMBDA-PROXY
    # integration
    # """
    # return {
    #     "message": "Go Serverless v1.0! Your function executed successfully!",
    #     "event": event
    # }
    # """
