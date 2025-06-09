import pytest
import os
from moto import mock_aws
import boto3
from src.transform.transform_lambda import lambda_handler
import json


@pytest.fixture 
def test_mock_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "mock_access_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "aws_secret_key"
    os.environ["AWS_SESSION_TOKEN"] = "0123"
    os.environ["INGESTION_BUCKET_NAME"] = "ingestion-bucket"
    os.environ["TRANSFORM_BUCKET_NAME"] = "processed-bucket"

@pytest.fixture
def client(test_mock_credentials):
    with mock_aws():
        yield boto3.client("s3")

@pytest.mark.it('function returns correct message on success')
def test_uploads_data(client):
    def file_uploader(client, key):
        with open(f"data/test_data/{key}", "r") as f:
            body = f.read()
        
        client.put_object(
            Body=body,
            Bucket='ingestion-bucket',
            Key=key,
        )
    
    client.create_bucket(
        Bucket="processed-bucket",
        CreateBucketConfiguration={"LocationConstraint":"eu-west-2"}
    )
    client.create_bucket(
        Bucket="ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint":"eu-west-2"}
    )
    
    keys = [
        "address-20250605T134757Z.json",
        "counterparty-20250605T134757Z.json",
        "currency-20250605T134850Z.json",
        "department-20250605T134851Z.json",
        "design-20250605T134756Z.json",
        "sales_order-20250604T102926Z.json",
        "staff-20250605T134758Z.json"
    ]
    for key in keys:
        file_uploader(client, key)
    
    assert lambda_handler({}, {}) == {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data successfully transformed'
            })
        }


@pytest.mark.it('function uploads all required files in parquet to the right bucket')
def test_uploads_files(client):
    def file_uploader(client, key):
        with open(f"data/test_data/{key}", "r") as f:
            body = f.read()
        
        client.put_object(
            Body=body,
            Bucket='ingestion-bucket',
            Key=key,
        )
    
    client.create_bucket(
        Bucket="processed-bucket",
        CreateBucketConfiguration={"LocationConstraint":"eu-west-2"}
    )
    client.create_bucket(
        Bucket="ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint":"eu-west-2"}
    )
    
    keys = [
        "address-20250605T134757Z.json",
        "counterparty-20250605T134757Z.json",
        "currency-20250605T134850Z.json",
        "department-20250605T134851Z.json",
        "design-20250605T134756Z.json",
        "sales_order-20250604T102926Z.json",
        "staff-20250605T134758Z.json"
    ]
    for key in keys:
        file_uploader(client, key)
        
    lambda_handler({}, {})
    
    files_in_bucket = client.list_objects_v2(
        Bucket="processed-bucket"
    )["Contents"]
    
    table_name = (
        "fact_sales_order",
        "dim_design",
        "dim_currency",
        "dim_location",
        "dim_date",
        "dim_staff",
        "dim_counterparty"
    )
    
    for file in files_in_bucket:
        assert file["Key"].endswith(".parquet")
        assert file["Key"].startswith(table_name)
    
@pytest.mark.it('function returns correct error message on failure')
def test_error_message(client):
    def file_uploader(client, key):
        with open(f"data/test_data/{key}", "r") as f:
            body = f.read()
        
        client.put_object(
            Body=body,
            Bucket='ingestion-bucket',
            Key=key,
        )
    
    client.create_bucket(
        Bucket="wrong_bucket_name",
        CreateBucketConfiguration={"LocationConstraint":"eu-west-2"}
    )
    client.create_bucket(
        Bucket="ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint":"eu-west-2"}
    )
    
    keys = [
        "address-20250605T134757Z.json",
        "counterparty-20250605T134757Z.json",
        "currency-20250605T134850Z.json",
        "department-20250605T134851Z.json",
        "design-20250605T134756Z.json",
        "sales_order-20250604T102926Z.json",
        "staff-20250605T134758Z.json"
    ]
    for key in keys:
        file_uploader(client, key)
    
    response = lambda_handler({}, {})
    
    assert response["statusCode"] == 500
    assert json.loads(response["body"])["message"] == "Error!"