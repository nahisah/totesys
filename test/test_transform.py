import json
import os
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from src.transform.transform_lambda import (
    get_all_table_data_from_ingest_bucket,
    get_table_data_from_ingest_bucket,
)


@pytest.fixture
def test_mock_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "mock_access_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "aws_secret_key"
    os.environ["AWS_SESSION_TOKEN"] = "0123"
    os.environ["INGESTION_BUCKET_NAME"] = "mock_bucket"


@pytest.fixture
def client(test_mock_credentials):
    with mock_aws():
        yield boto3.client("s3")


class TestGetTableDataFromIngestBucket:

    def test_returns_dictionary_with_correct_keys(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        key = f"currency/2025/01/01/currency-{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"
        client.put_object(
            Body=json.dumps(
                {
                    "currency_id": 1,
                    "currency_code": "GBP",
                    "created_at": 2025,
                    "last_updated": 1999,
                }
            ),
            Bucket=bucket_name,
            Key=key,
        )
        column_names = ["currency_id", "currency_code", "created_at", "last_updated"]
        response = get_table_data_from_ingest_bucket("currency", bucket_name)
        assert isinstance(response, dict)
        for column_name in column_names:
            assert column_name in response

    def test_raises_exception_on_failure_to_retrieve_data(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        with pytest.raises(RuntimeError):
            get_table_data_from_ingest_bucket("currency", bucket_name)


class TestGetAllTableDataFromIngestBucket:

    def test_returns_dictionary_with_all_tables_as_keys(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        table_names = [
            "sales_order",
            "design",
            "address",
            "counterparty",
            "staff",
            "currency",
            "department",
        ]
        for table_name in table_names:
            key = f"{table_name}/2025/01/01/{table_name}-{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"
            client.put_object(Body=json.dumps({}), Bucket=bucket_name, Key=key)
        response = get_all_table_data_from_ingest_bucket()
        assert isinstance(response, dict)
        for table_name in table_names:
            assert table_name in response
