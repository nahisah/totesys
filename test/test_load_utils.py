import os
import pytest
from moto import mock_aws
import boto3
from src.load.load_utils import accessing_files_from_processed_bucket, load_data_frames_into_datawarehouse
import datetime
from datetime import timezone
import pandas as pd


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
    
    def test_sample(self, client):
        bucket_name = "mock_bucket"
        client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint":"eu-west-2"})
        key = f"dim_date/2025/01/01/dim_date-{datetime.datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}"

        client.upload_file("./data/test_data/dim_date-20250609T105450Z.parquet", bucket_name, key)
        df = accessing_files_from_processed_bucket("dim_date", bucket_name)
        
        load_data_frames_into_datawarehouse(df)
