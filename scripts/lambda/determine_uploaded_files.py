
import json
import boto3
import botocore 

s3 = boto3.client("s3")

print('Loading function')


def lambda_handler(event, context):
    
    #load in json from previous lambda
    
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = event["Records"][0]["s3"]["object"]["key"]
    is_public_API = event["is_public_API"]

    print(event)
    
    #Build list of the files to wait on based on previous lambda's data:
    if is_public_API:
        files_to_check = ["_resource"]
    else:
        files_to_check = [""]
    
    if event["has_demographic_data"]:
        files_to_check.append("_demographic")
    if event["has_geographic_data"]:
        files_to_check.append("_geographic")
    
    print("Files to check are:", files_to_check)
    
    #Check if the files to wait on have been delivered to S3: 
    for file_suffix in files_to_check:
        unique_id = key.split("/")[1].split(".")[0]
        data_path = "input-data/" + unique_id + file_suffix + ".csv"
        
        try:
            obj = s3.head_object(Bucket = bucket, Key = data_path)
            print(obj)
        
        except botocore.exceptions.ClientError as e:
            print("on the except side")
            #Data not yet available: 
            if e.response["Error"]["Code"] == "404":
                print("We don't yet have {} dataset".format(file_suffix))
                
                event["all_files_uploaded"] = False
                
                return event
            
            #There is an error
            else:
                raise e  
                
    event["all_files_uploaded"] = True

    return event