import pytest
import os
from moto import mock_aws
import boto3
from src.load.load_lambda import lambda_handler
import json
import datetime
from datetime import timezone
from unittest.mock import patch
import dotenv
from utils.db_connection import create_conn, close_conn


@pytest.fixture(autouse=True)
def test_mock_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "mock_access_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "aws_secret_key"
    os.environ["AWS_SESSION_TOKEN"] = "0123"
    os.environ["INGESTION_BUCKET_NAME"] = "ingestion-bucket"
    os.environ["TRANSFORM_BUCKET_NAME"] = "processed-bucket"
    os.environ["STEP_MACHINE_ARN"] = (
        "arn:aws:states:eu-west-2:123456789012:stateMachine:step-machine"
    )


@pytest.fixture(autouse=True)
def db_credentials():
    dotenv.load_dotenv()


@pytest.fixture
def client(test_mock_credentials):
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture
def step_client(test_mock_credentials):
    with mock_aws():
        yield boto3.client("stepfunctions", region_name="eu-west-2")


def get_rows_from_table(table_name):
    query = f"SELECT * FROM {table_name};"

    conn = create_conn()
    if conn:
        try:
            response = conn.run(query)
        finally:
            close_conn(conn)

    return response


@patch("src.load.load_lambda.requests")
@pytest.mark.it("function returns correct message on success")
def test_uploads_data(mock_request, client, step_client):
    def file_uploader(client, filename):
        table_name = filename.split("-")[0]
        key = f"{table_name}/2025/01/01/{table_name}-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"

        client.upload_file(
            Filename=f"data/test_data/{filename}",
            Bucket="processed-bucket",
            Key=f"{key}",
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

    step_client.create_state_machine(
        name="step-machine",
        definition="{}",
        roleArn="arn:aws:iam::123456789012:role/DummyRole",
    )

    client.create_bucket(
        Bucket="processed-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    filenames = [
        "dim_counterparty-20250609T133849Z.parquet",
        "dim_currency-20250609T105450Z.parquet",
        "dim_date-20250609T105450Z.parquet",
        "dim_design-20250609T105450Z.parquet",
        "dim_location-20250609T105450Z.parquet",
        "dim_staff-20250609T105450Z.parquet",
        "fact_sales_order-20250609T105449Z.parquet",
    ]
    for filename in filenames:
        file_uploader(client, filename)

    assert lambda_handler({}, {}) == {
        "statusCode": 200,
        "body": json.dumps({"message": "Data successfully loaded"}),
    }


@patch("src.load.load_lambda.requests")
@pytest.mark.it("function uploads all required data to the warehouse")
def test_uploads_files(mock_request, client, step_client):
    def file_uploader(client, filename):
        table_name = filename.split("-")[0]
        key = f"{table_name}/2025/01/01/{table_name}-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"

        client.upload_file(
            Filename=f"data/test_data/{filename}",
            Bucket="processed-bucket",
            Key=f"{key}",
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

    step_client.create_state_machine(
        name="step-machine",
        definition="{}",
        roleArn="arn:aws:iam::123456789012:role/DummyRole",
    )

    client.create_bucket(
        Bucket="processed-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    filenames = [
        "dim_counterparty-20250609T133849Z.parquet",
        "dim_currency-20250609T105450Z.parquet",
        "dim_date-20250609T105450Z.parquet",
        "dim_design-20250609T105450Z.parquet",
        "dim_location-20250609T105450Z.parquet",
        "dim_staff-20250609T105450Z.parquet",
        "fact_sales_order-20250609T105449Z.parquet",
    ]
    for filename in filenames:
        file_uploader(client, filename)

    lambda_handler({}, {})

    assert len(get_rows_from_table("dim_date")) == 954


@patch("src.load.load_lambda.requests")
@pytest.mark.it("function returns correct error message on failure")
def test_error_message(mock_request, client, step_client):
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

    step_client.create_state_machine(
        name="step-machine",
        definition="{}",
        roleArn="arn:aws:iam::123456789012:role/DummyRole",
    )

    client.create_bucket(
        Bucket="processed-bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

    response = lambda_handler({}, {})

    assert response["statusCode"] == 500
    assert json.loads(response["body"])["message"] == "Error!"
