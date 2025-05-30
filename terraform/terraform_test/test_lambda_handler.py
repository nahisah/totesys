import boto3 
import os
import time
import string
import random 

def lambda_handler(event,context):
    s3 = boto3.client("s3")

    bucket_name = os.environ["BUCKET_NAME"] ## reference to terraform/lambda environment variable
    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    file_name = "hello-world.txt"
    file_content = "Hello world!" + random_string

    s3.put_object(Bucket=bucket_name, Key=file_name,Body=file_content)

    return {
        "statusCode": 200,
        "body": "uploaded file"

    }