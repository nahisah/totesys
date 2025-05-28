import boto3 
import os 

def lambda_handler(event,context):
    s3 = boto3.client("s3")

    bucket_name = os.environ["BUCKET_NAME"] ## reference to terraform/lambda environment variable
    file_name = "hello-world.txt"
    file_content = "Hello world!"

    s3.put_object(Bucket=bucket_name, Key=file_name,Body=file_content)

    return {
        "statusCode": 200,
        "body": "uploaded file"

    }