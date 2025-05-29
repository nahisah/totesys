import boto3

s3 = boto3.client("s3")

bucket_name = "bucket-two-processed20250528084929258500000002"
file_path = "src/test.txt"
s3_key = "my_file.txt"

s3.upload_file(file_path,bucket_name,s3_key)

print("file is loaded.")