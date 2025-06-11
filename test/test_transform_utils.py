import datetime
import json
import os
from datetime import timezone

import awswrangler as wr
import boto3
import pandas as pd
import pytest
from moto import mock_aws

from src.transform.transform_utils import (
    get_all_table_data_from_ingest_bucket,
    get_table_data_from_ingest_bucket,
    transform_dim_counterparty,
    transform_dim_currency,
    transform_dim_date,
    transform_dim_design,
    transform_dim_location,
    transform_dim_staff,
    transform_fact_sales_order,
    upload_to_s3,
)


@pytest.fixture
def test_mock_credentials(autouse=True):
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
        key = f"currency/2025/01/01/currency-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"
        client.put_object(
            Body=json.dumps(
                [
                    {
                        "currency_id": 1,
                        "currency_code": "GBP",
                        "created_at": 2025,
                        "last_updated": 1999,
                    },
                    {
                        "currency_id": 2,
                        "currency_code": "USD",
                        "created_at": 2025,
                        "last_updated": 1999,
                    },
                ]
            ),
            Bucket=bucket_name,
            Key=key,
        )
        column_names = ["currency_id", "currency_code", "created_at", "last_updated"]
        response = get_table_data_from_ingest_bucket("currency", bucket_name)
        assert isinstance(response, list)
        for column_name in column_names:
            assert column_name in response[0]

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
            key = f"{table_name}/2025/01/01/{table_name}-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"
            client.put_object(Body=json.dumps({}), Bucket=bucket_name, Key=key)
        response = get_all_table_data_from_ingest_bucket()
        assert isinstance(response, dict)
        for table_name in table_names:
            assert table_name in response


class TestTransformTables:

    @pytest.mark.it(
        "transform_fact_sales_order returns a dataframe with columns as specified in the warehouse design"
    )
    def test_fact_sales_order(self):
        with open("data/test_data/sales_order-20250604T102926Z.json", "r") as file:
            data_sales_order = json.load(file)

        actual = transform_fact_sales_order(data_sales_order)

        assert isinstance(actual, pd.DataFrame)

        assert actual.columns.tolist() == [
            "sales_record_id",
            "sales_order_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
            "sales_staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "design_id",
            "agreed_payment_date",
            "agreed_delivery_date",
            "agreed_delivery_location_id",
        ]

    @pytest.mark.it(
        "transform_fact_sales_order splits the datestamped columns into the right formats"
    )
    def test_datetime_split(self):
        with open("data/test_data/sales_order-20250604T102926Z.json", "r") as file:
            data_sales_order = json.load(file)

        actual = transform_fact_sales_order(data_sales_order)

        assert isinstance(actual["created_date"][0], datetime.date)
        assert isinstance(actual["last_updated_date"][0], datetime.date)
        assert isinstance(actual["agreed_payment_date"][0], datetime.date)
        assert isinstance(actual["agreed_delivery_date"][0], datetime.date)

    @pytest.mark.it(
        "transform_dim_design returns a dataframe with columns as specified in the warehouse design"
    )
    def test_dim_design(self):
        with open("data/test_data/design-20250605T134756Z.json", "r") as file:
            data = json.load(file)

        actual = transform_dim_design(data)

        assert isinstance(actual, pd.DataFrame)

        assert actual.columns.tolist() == [
            "design_id",
            "design_name",
            "file_location",
            "file_name",
        ]

    @pytest.mark.it(
        "transform_dim_currency returns a dataframe with columns as specified in the warehouse design"
    )
    def test_dim_currency(self):
        with open("data/test_data/currency-20250605T134850Z.json", "r") as file:
            data = json.load(file)

        actual = transform_dim_currency(data)

        assert isinstance(actual, pd.DataFrame)
        assert actual.columns.tolist() == [
            "currency_id",
            "currency_code",
            "currency_name",
        ]

    @pytest.mark.it(
        "transform_dim_location returns a dataframe with columns as specified in the warehouse design"
    )
    def test_dim_location(self):
        with open("data/test_data/address-20250605T134757Z.json", "r") as file:
            data = json.load(file)

        actual = transform_dim_location(data)

        assert isinstance(actual, pd.DataFrame)
        assert actual.columns.tolist() == [
            "location_id",
            "address_line_1",
            "address_line_2",
            "district",
            "city",
            "postal_code",
            "country",
            "phone",
        ]

    @pytest.mark.it(
        "transform_dim_dates returns a dataframe with columns as specified in the warehouse design"
    )
    def test_dim_dates(self):
        with open("data/test_data/sales_order-20250604T102926Z.json", "r") as file:
            data_sales_order = json.load(file)
        transformed_data = transform_fact_sales_order(data_sales_order)

        actual = transform_dim_date(transformed_data)

        assert isinstance(actual, pd.DataFrame)
        assert actual.columns.tolist() == [
            "date_id",
            "year",
            "month",
            "day",
            "day_of_week",
            "day_name",
            "month_name",
            "quarter",
        ]

    @pytest.mark.it(
        "transform_dim_staff returns a dataframe with columns as specified in the warehouse design"
    )
    def test_dim_staff(self):
        with open("data/test_data/staff-20250605T134758Z.json", "r") as f1:
            data_staff = json.load(f1)
        with open("data/test_data/department-20250605T134851Z.json", "r") as f2:
            data_department = json.load(f2)

        actual = transform_dim_staff(data_staff, data_department)
        assert isinstance(actual, pd.DataFrame)
        assert actual.columns.tolist() == [
            "staff_id",
            "first_name",
            "last_name",
            "department_name",
            "location",
            "email_address",
        ]

    @pytest.mark.it(
        "transform_dim_staff returns a dataframe with columns as specified in the warehouse design"
    )
    def test_dim_counterparty(self):
        with open("data/test_data/counterparty-20250605T134757Z.json", "r") as f1:
            data_counterparty = json.load(f1)
        with open("data/test_data/address-20250605T134757Z.json", "r") as f2:
            data_address = json.load(f2)

        actual = transform_dim_counterparty(data_counterparty, data_address)
        assert isinstance(actual, pd.DataFrame)
        assert actual.columns.tolist() == [
            "counterparty_id",
            "counterparty_legal_name",
            "counterparty_legal_address_line_1",
            "counterparty_legal_address_line_2",
            "counterparty_legal_district",
            "counterparty_legal_city",
            "counterparty_legal_postal_code",
            "counterparty_legal_country",
            "counterparty_legal_phone",
        ]


class TestUploadToS3:

    @pytest.mark.it(
        "upload_to_s3 uploads a file to s3 with a correct key and in a correct format"
    )
    def test_upload_to_s3(self, client):
        bucket_name = "mock_bucket"
        table_name = "fact_sales_order"
        client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )

        with open("data/test_data/sales_order-20250604T102926Z.json", "r") as file:
            data_sales_order = json.load(file)

        df = transform_fact_sales_order(data_sales_order)

        path = upload_to_s3(df, bucket_name, table_name)

        response = wr.s3.read_parquet([path])

        assert response.columns.tolist() == [
            "sales_record_id",
            "sales_order_id",
            "created_date",
            "created_time",
            "last_updated_date",
            "last_updated_time",
            "sales_staff_id",
            "counterparty_id",
            "units_sold",
            "unit_price",
            "currency_id",
            "design_id",
            "agreed_payment_date",
            "agreed_delivery_date",
            "agreed_delivery_location_id",
        ]
