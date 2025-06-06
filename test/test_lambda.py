import json
import os
from unittest.mock import patch

import boto3
import dotenv
import pytest
from moto import mock_aws

from src.ingestion.lambda_for_secrets import lambda_handler


@pytest.fixture(autouse=True)
def db_credentials():
    dotenv.load_dotenv()


@pytest.fixture
def test_mock_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "mock_access_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "aws_secret_key"
    os.environ["AWS_SESSION_TOKEN"] = "0123"
    os.environ["AWS_REGION"] = "eu-west-2"
    os.environ["INGESTION_BUCKET_NAME"] = "ingestion-bucket"
    os.environ["STEP_MACHINE_ARN"] = "arn:aws:states:eu-west-2:123456789012:stateMachine:step-machine"


@pytest.fixture
def client(test_mock_credentials):
    with mock_aws():
        yield boto3.client("s3")

@pytest.fixture
def step_client(test_mock_credentials):
    with mock_aws():
        yield boto3.client("stepfunctions",region_name="eu-west-2")        


@patch("src.ingestion.lambda_for_secrets.requests")

def test_successful_request_returns_status_code_200(mock_request,client,step_client):
    step_client.create_state_machine(
        name = "step-machine",
        definition="{}",
        roleArn="arn:aws:iam::123456789012:role/DummyRole"
    )
    

    client.create_bucket(
        Bucket="ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    mock_request.get().status_code = 200
    mock_body = {
        "SecretString": json.dumps(
            {
                "user": os.environ["DBUSER"],
                "database": os.environ["DBNAME"],
                "password": os.environ["DBPASSWORD"],
                "port": os.environ["PORT"],
                "host": os.environ["HOST"],
            }
        )
    }
    mock_text = json.dumps(mock_body)
    mock_request.get().text = mock_text
    assert lambda_handler({}, {}) == {"statusCode": 200}


@patch("src.ingestion.lambda_for_secrets.requests")

def test_all_data_successfully_put_inside_bucket(mock_request,client,step_client):
    step_client.create_state_machine(
        name = "step-machine",
        definition="{}",
        roleArn="arn:aws:iam::123456789012:role/DummyRole"
    )

    client.create_bucket(
        Bucket="ingestion-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    mock_request.get().status_code = 200
    mock_body = {
        "SecretString": json.dumps(
            {
                "user": os.environ["DBUSER"],
                "database": os.environ["DBNAME"],
                "password": os.environ["DBPASSWORD"],
                "port": os.environ["PORT"],
                "host": os.environ["HOST"],
            }
        )
    }
    mock_text = json.dumps(mock_body)
    mock_request.get().text = mock_text
    lambda_handler({}, {})
    contents = client.list_objects_v2(Bucket="ingestion-bucket")

    assert len(contents["Contents"]) == 7  # Number should match amount of table names


@patch("src.ingestion.lambda_for_secrets.requests")

def test_status_code_500_for_wrong_request(mock_request,client,step_client):
    step_client.create_state_machine(
        name = "step-machine",
        definition="{}",
        roleArn="arn:aws:iam::123456789012:role/DummyRole"
    )

    mock_request.get().status_code = 200
    mock_body = {
        "SecretString": json.dumps(
            {
                "user": os.environ["DBUSER"],
                "database": os.environ["DBNAME"],
                "password": os.environ["DBPASSWORD"],
                "port": os.environ["PORT"],
                "host": os.environ["HOST"],
            }
        )
    }
    mock_text = json.dumps(mock_body)

    mock_request.get().text =mock_text
    assert lambda_handler({},{})["statusCode"] == 500 

