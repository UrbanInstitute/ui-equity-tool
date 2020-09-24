import json
import datetime
import time
import random
import os
import io
import boto3
import csv
from botocore.exceptions import ClientError


api_bucket = os.environ["API_BUCKET"]
data_bucket_region = os.environ["DATA_BUCKET_REGION"]


def formatter(data, event, statuscode):
    """
    Format the response so it conforms to API Gateway's requirements.
    Args:
        data (dict): Data to be returned from request as a json dict.
        event (dict): AWS API Gateway event data passed to AWS Lambda.
        statuscode: status code to return to API Gateway
    Returns:
        Dictionary of formatted response data
    """

    return {
        "isBase64Encoded": False,
        "statusCode": statuscode,
        "headers": event["headers"],
        "body": json.dumps(
            # [
            {"next": None, "previous": None, "results": data,}
            # ]
        ),
    }


def handler(event, context):
    if event["httpMethod"] == "GET":
        if event["resource"].startswith("/getfile"):
            pathparams = event["pathParameters"]
            fileid = pathparams["fileid"]
            print(fileid)

            s3 = boto3.client("s3")

            try:
                # If object exists, get file as json
                x = s3.get_object(
                    Bucket=api_bucket,
                    Key="output-data/api-response/" + str(fileid) + ".json",
                )
                output_json = x["Body"].read().decode()
                file = json.loads(output_json)
                status_code = 200
                file_exists = True

            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    # The object does not exist.
                    status_code = 200
                    file = []
                    file_exists = False
                else:
                    # Something else has gone wrong.
                    print("Internal AWS Error")
                    status_code = 500
                    file = []
                    file_exists = None
                    raise ValueError
            except:
                # The object does exist.
                status_code = 500
                file = []
                file_exists = None

            # Format output json
            output = {
                "result": file,
                "fileid": fileid,
                "file_exists": file_exists,
            }

        elif event["resource"].startswith("/getstatus"):
            print(event["pathParameters"])
            pathparams = event["pathParameters"]
            fileid = pathparams["fileid"]

            # Get update json using the fileid

            s3 = boto3.client("s3")

            try:
                # If object exists, get file as json
                x = s3.get_object(
                    Bucket=api_bucket,
                    Key="update-data/" + str(fileid) + ".json",
                )
                output_json = x["Body"].read().decode()
                file = json.loads(output_json)
                status_code = 200
                file_exists = True
            except ClientError as e:
                if e.response["Error"]["Code"] == "NoSuchKey":
                    # The object does not exist.
                    status_code = 200
                    file = []
                    file_exists = False
                else:
                    # Something else has gone wrong.
                    print("Other Uncaught Error")
                    status_code = 500
                    file = []
                    file_exists = None
                    raise ValueError
            except:
                # The object does exist.
                status_code = 500
                file = []
                file_exists = None

            output = {
                "formdata": file,
                "fileid": fileid,
                "file_exists": file_exists,
            }

        else:
            output = {"status_code": 200, "errorcode": "Resource Not found"}
            status_code = 200

        return formatter(output, event, status_code)

