import json
import urllib.parse
import boto3
from urllib.parse import unquote_plus

s3 = boto3.client('s3')



def lambda_handler(event, context):
    print(type(event))
    print("Received event: " + json.dumps(event, indent=2))
   
    
    bucket = event["detail"]["bucket"]["name"]
    key = event["detail"]["object"]["key"]
    request_id = event["detail"]["request-id"]
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        body = response['Body'].read().decode('utf-8')
        body = json.loads(body)
        print("BODY IS:")
        print(body)
        print(type(body))
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}.'.format(key, bucket))
        raise e
    
    #Determines if file upload has geographic and demographic data and is from public API
    has_demographic_data = True
    has_geographic_data = True
    is_public_API = True 

    #Ensures we have demographic_columns, geographic_columns, and if data is from public API
    try: 
        if body.get("demographic_file_name") is None:
            has_demographic_data = False
        if body.get("geographic_file_name") is None:
            has_geographic_data = False
        if body.get("is_public_API") is None:
            is_public_API = False
        acs_data_year = body.get("acs_data_year")

    except Exception as e:
        print(e)
        raise e
        
    
    equity_calc_dict_structure = {
        "Records":[
            {
                "s3":{
                    "bucket":{
                        "name":bucket
                    },
                    "object":{
                        "key":key
                    }
                },
                "responseElements":{
                    "x-amz-request-id":request_id
                }
            }
        ],
        "has_demographic_data":has_demographic_data,
        "has_geographic_data":has_geographic_data, 
        "is_public_API":is_public_API,
        "acs_data_year":acs_data_year
        
    }
     
    return equity_calc_dict_structure
      