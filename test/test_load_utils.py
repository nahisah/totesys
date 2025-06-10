import os
import pytest
from moto import mock_aws
import boto3
from src.load.load_utils import accessing_files_from_processed_bucket, load_dim_dates_into_warehouse, load_dim_staff_into_warehouse, load_dim_location_into_warehouse, load_dim_currency_into_warehouse, load_dim_design_into_warehouse
from utils.db_connection import create_conn, close_conn
import datetime
from datetime import timezone
import pandas as pd


@pytest.fixture(autouse=True)
def test_mock_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "mock_access_key"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "aws_secret_key"
    os.environ["AWS_SESSION_TOKEN"] = "0123"
    os.environ["INGESTION_BUCKET_NAME"] = "mock_bucket"


@pytest.fixture
def client(test_mock_credentials):
    with mock_aws():
        yield boto3.client("s3")


def get_rows_from_table(table_name):
    query = f"SELECT * FROM {table_name};"
    
    conn = create_conn()
    if conn:
        try:
            response = conn.run(query)
        finally:
            close_conn(conn)
    
    return response

class TestAccesingFileFromProcessedBucket:

    def test_resturns_a_pandas_data_frame(self,client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name,CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})
        key = f"dim_currency/2025/01/01/dim_currency-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"

        client.upload_file("./data/test_data/dim_currency-20250609T105450Z.parquet",bucket_name,key)
        response = accessing_files_from_processed_bucket("dim_currency",bucket_name)
        assert isinstance(response,pd.DataFrame)
    
    def test_rasies_exception_on_failure_to_retrieve_file(self,client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name,CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})
        with pytest.raises(RuntimeError):
            accessing_files_from_processed_bucket("dim_currency",bucket_name)


class TestLoadDataFramesIntoWarehouse:
    
    def test_dim_dates_uploads_data_to_warehouse(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})
        key = f"dim_date/2025/01/01/dim_date-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"
        client.upload_file("./data/test_data/dim_date-20250609T105450Z.parquet", bucket_name, key)
        
        df = accessing_files_from_processed_bucket("dim_date", bucket_name)
        
        load_dim_dates_into_warehouse(df)
        
        assert len(get_rows_from_table("dim_date")) == 954

    def test_dim_dates_raises_runtime_error_on_exception(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})
        key = f"dim_date/2025/01/01/dim_date-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"
        client.upload_file("data/test_data/dim_counterparty-20250609T133849Z.parquet", bucket_name, key)
        
        df = accessing_files_from_processed_bucket("dim_date", bucket_name)
        
        with pytest.raises(RuntimeError):
            load_dim_dates_into_warehouse(df)
    
    def test_dim_staff_uploads_data_to_warehouse(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})
        key = f"dim_staff/2025/01/01/dim_staff-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"
        client.upload_file("data/test_data/dim_staff-20250609T105450Z.parquet", bucket_name, key)
        
        df = accessing_files_from_processed_bucket("dim_staff", bucket_name)
        
        load_dim_staff_into_warehouse(df)
        
        assert len(get_rows_from_table("dim_staff")) == 20

    def test_dim_staff_raises_runtime_error_on_exception(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})
        key = f"dim_staff/2025/01/01/dim_staff-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"

        client.upload_file("data/test_data/dim_counterparty-20250609T133849Z.parquet", bucket_name, key)
        
        df = accessing_files_from_processed_bucket("dim_staff", bucket_name)
        
        with pytest.raises(RuntimeError):
            load_dim_staff_into_warehouse(df)

    def test_dim_location_uploads_data_to_warehouse(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})
        key = f"dim_location/2025/01/01/dim_location-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"
        client.upload_file("data/test_data/dim_location-20250609T105450Z.parquet", bucket_name, key)
        
        df = accessing_files_from_processed_bucket("dim_location", bucket_name)
        
        load_dim_location_into_warehouse(df)
        
        assert len(get_rows_from_table("dim_location")) == 30

    def test_dim_location_raises_runtime_error_on_exception(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})
        key = f"dim_location/2025/01/01/dim_location-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"

        client.upload_file("data/test_data/dim_counterparty-20250609T133849Z.parquet", bucket_name, key)
        
        df = accessing_files_from_processed_bucket("dim_location", bucket_name)
        
        with pytest.raises(RuntimeError):
            load_dim_location_into_warehouse(df)

    def test_dim_currency_uploads_data_to_warehouse(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})
        key = f"dim_currency/2025/01/01/dim_currency-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"
        client.upload_file("data/test_data/dim_currency-20250609T105450Z.parquet", bucket_name, key)
        
        df = accessing_files_from_processed_bucket("dim_currency", bucket_name)
        
        load_dim_currency_into_warehouse(df)
        
        assert len(get_rows_from_table("dim_currency")) == 3

    def test_dim_currency_raises_runtime_error_on_exception(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})
        key = f"dim_currency/2025/01/01/dim_currency-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"

        client.upload_file("data/test_data/dim_counterparty-20250609T133849Z.parquet", bucket_name, key)
        
        df = accessing_files_from_processed_bucket("dim_currency", bucket_name)
        
        with pytest.raises(RuntimeError):
            load_dim_currency_into_warehouse(df)

    def test_dim_design_uploads_data_to_warehouse(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})
        key = f"dim_design/2025/01/01/dim_design-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"
        client.upload_file("data/test_data/dim_design-20250609T105450Z.parquet", bucket_name, key)
        
        df = accessing_files_from_processed_bucket("dim_design", bucket_name)
        
        load_dim_design_into_warehouse(df)
        
        assert len(get_rows_from_table("dim_design")) == 603

    def test_dim_design_raises_runtime_error_on_exception(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})
        key = f"dim_design/2025/01/01/dim_design-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"

        client.upload_file("data/test_data/dim_counterparty-20250609T133849Z.parquet", bucket_name, key)
        
        df = accessing_files_from_processed_bucket("dim_design", bucket_name)
        
        with pytest.raises(RuntimeError):
            load_dim_design_into_warehouse(df)


