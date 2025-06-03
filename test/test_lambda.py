import pytest
from moto import mock_aws
import boto3
from src.ingestion.lambda_for_secrets import lambda_handler
import os
from unittest.mock import patch
import json
import dotenv


@pytest.fixture(autouse=True)
def db_credentials():
    dotenv.load_dotenv()

@pytest.fixture 
def test_mock_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "mock_access_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "aws_secret_key"
    os.environ["AWS_SESSION_TOKEN"] = "0123"
    os.environ["BUCKET_NAME"] = "ingestion-bucket"

@pytest.fixture
def client(test_mock_credentials):
    with mock_aws():
        yield boto3.client("s3")


@patch("src.ingestion.lambda_for_secrets.requests")
def test_successful_request_returns_status_code_200(mock_request,client):
    client.create_bucket(
        Bucket="ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint":"eu-west-2"}
    )
    mock_request.get().status_code = 200
    mock_body = {"SecretString":
                        json.dumps({"user": os.environ["DBUSER"],
                                   "database": os.environ["DBNAME"],
                                   "password":os.environ["DBPASSWORD"],
                                   "port":os.environ["PORT"],
                                   "host":os.environ["HOST"]}) }
    mock_text = json.dumps(mock_body)
    mock_request.get().text =mock_text
    assert lambda_handler({},{}) == {"statusCode": 200}

@patch("src.ingestion.lambda_for_secrets.requests")
def test_all_data_successfully_put_inside_bucket(mock_request,client):
    client.create_bucket(
        Bucket="ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint":"eu-west-2"}
    )
    mock_request.get().status_code = 200
    mock_body = {"SecretString":
                        json.dumps({"user": os.environ["DBUSER"],
                                   "database": os.environ["DBNAME"],
                                   "password":os.environ["DBPASSWORD"],
                                   "port":os.environ["PORT"],
                                   "host":os.environ["HOST"]}) }
    mock_text = json.dumps(mock_body)
    mock_request.get().text =mock_text
    lambda_handler({},{})
    contents = client.list_objects_v2(Bucket="ingestion-bucket")
    
    assert len(contents["Contents"]) == 7 #Number should match amount of table names

@patch("src.ingestion.lambda_for_secrets.requests")
def test_status_code_500_for_wrong_request(mock_request,client):
    mock_request.get().status_code = 200
    mock_body = {"SecretString":
                        json.dumps({"user": os.environ["DBUSER"],
                                   "database": os.environ["DBNAME"],
                                   "password":os.environ["DBPASSWORD"],
                                   "port":os.environ["PORT"],
                                   "host":os.environ["HOST"]}) }
    mock_text = json.dumps(mock_body)
    mock_request.get().text =mock_text
    assert lambda_handler({},{})["statusCode"] == 500 