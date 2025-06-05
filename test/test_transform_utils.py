from src.transform.transform_utils import get_table_data_from_ingest_bucket, get_all_table_data_from_ingest_bucket, transform_fact_sales_order
import pytest
import os
from moto import mock_aws
import boto3
from datetime import datetime, timezone
import json
import pandas as pd


@pytest.fixture
def test_mock_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "mock_access_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "aws_secret_key"
    os.environ["AWS_SESSION_TOKEN"] = "0123"
    os.environ["BUCKET_NAME"] = "mock_bucket"

@pytest.fixture
def client(test_mock_credentials):
    with mock_aws():
        yield boto3.client("s3")


class TestGetTableDataFromIngestBucket:
    
    def test_returns_dictionary_with_correct_keys(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        key = f"currency/2025/01/01/currency-{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"

        client.put_object(
            Body=json.dumps([{
                "currency_id": 1,
                "currency_code": "GBP",
                "created_at": 2025,
                "last_updated": 1999
            },{
                "currency_id": 2,
                "currency_code": "USD",
                "created_at": 2025,
                "last_updated": 1999
            }]),
            Bucket=bucket_name,
            Key=key
        )
        column_names = ["currency_id", "currency_code", "created_at", "last_updated"]
        response = get_table_data_from_ingest_bucket("currency", bucket_name)
        assert isinstance(response, list)
        for column_name in column_names:
            assert column_name in response[0]
    
    def test_raises_exception_on_failure_to_retrieve_data(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        with pytest.raises(RuntimeError):
            get_table_data_from_ingest_bucket("currency", bucket_name)

class TestGetAllTableDataFromIngestBucket:
    
    def test_returns_dictionary_with_all_tables_as_keys(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
        table_names = ["sales_order",
                    "design",
                    "address",
                    "counterparty",
                    "staff",
                    "currency",
                    "department"
        ]
        for table_name in table_names:
            key = f"{table_name}/2025/01/01/{table_name}-{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"
            client.put_object(
                Body=json.dumps({}),
                Bucket=bucket_name,
                Key=key
            )
        response = get_all_table_data_from_ingest_bucket()
        assert isinstance(response, dict)
        for table_name in table_names:
            assert table_name in response

class TestTransformTables:

    @pytest.mark.it('transform_fact_sales_order returns a dataframe with columns as specified in the warehourse design')
    def test_fact_sales_order(self):
        # arrange
        with open("data/test_data/sales_order-20250604T102926Z.json", 'r') as file:
            data_sales_order = json.load(file)

        # act
        actual = transform_fact_sales_order(data_sales_order)

        # assert
        assert isinstance(actual, pd.DataFrame)

        assert actual.columns.tolist() == ['sales_record_id', 'sales_order_id', 'created_date',         'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']


    @pytest.mark.it('transform_fact_sales_order splits the datestamped columns into the right formats')
    def test_datetime_split(self):
        # arrange
        with open("data/test_data/sales_order-20250604T102926Z.json", 'r') as file:
            data_sales_order = json.load(file)

        # act
        actual = transform_fact_sales_order(data_sales_order)

        # assert

        assert isinstance(actual.at[0, 'created_date'], dict)

