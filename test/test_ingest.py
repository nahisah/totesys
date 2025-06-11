import datetime
import json
from datetime import timezone

import boto3
import pytest
from moto import mock_aws

from src.ingestion.ingest_utils import (
    convert_to_json,
    extract_data,
    ingest,
    upload_to_s3,
)
from utils.db_connection import close_conn, create_conn
from utils.normalise_datetime import normalise_datetimes


@pytest.fixture(scope="module")
def db():
    """Create a database connection and closes it at the end"""
    db = create_conn()
    yield db
    close_conn(db)

@pytest.fixture(scope="module")
def mock_client():
    with mock_aws():
        client = boto3.client("s3")
        yield client


@pytest.mark.it(
    "extract_data returns all the information in a given table in list of dictionaries format"
)
def test_extract_data_full_table(db):
    table_name = "currency"
    expected = [
        {
            "currency_id": 2,
            "currency_code": "USD",
            "created_at": "2022-11-03 14:20:49.962",
            "last_updated": "2022-11-03 14:20:49.962",
        },
        {
            "currency_id": 3,
            "currency_code": "EUR",
            "created_at": "2022-11-03 14:20:49.962",
            "last_updated": "2022-11-03 14:20:49.962",
        },
    ]
    actual = normalise_datetimes(extract_data(table_name))
    assert actual == expected


@pytest.mark.it(
    "extract_data raises a RuntimeErroer in the event of failure"
)
def test_extract_data_error():
    table_name = "restaurants"
    with pytest.raises(RuntimeError):
        extract_data(table_name)


@pytest.mark.it("convert_to_json converts a list of dictionaries into a .json file")
def test_converts_to_json():
    input_data = [
        {
            "currency_id": 2,
            "currency_code": "USD",
            "created_at": "2022-11-03 14:20:49.962",
            "last_updated": "2022-11-03 14:20:49.962",
        },
        {
            "currency_id": 3,
            "currency_code": "EUR",
            "created_at": "2022-11-03 14:20:49.962",
            "last_updated": "2022-11-03 14:20:49.962",
        },
    ]

    converted = convert_to_json(input_data)

    def is_it_json(input):
        try:
            json.loads(input)
            return True
        except (json.JSONDecodeError, ValueError):
            return False

    assert is_it_json(converted)


@pytest.mark.it("upload_to_s3 uploads json to s3 with a correct key")
def test_correct_upload(mock_client):
    bucket_name = "mock_bucket"
    mock_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    input_data = [
        {
            "currency_id": 2,
            "currency_code": "USD",
            "created_at": "2022-11-03 14:20:49.962",
            "last_updated": "2022-11-03 14:20:49.962",
        },
        {
            "currency_id": 3,
            "currency_code": "EUR",
            "created_at": "2022-11-03 14:20:49.962",
            "last_updated": "2022-11-03 14:20:49.962",
        },
    ]
    input_json = json.dumps(input_data)
    table_name = "currency"
    now = datetime.datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    timestamp_2 = (now - datetime.timedelta(seconds=1)).strftime("%Y%m%dT%H%M%SZ")
    timestamp_3 = (now + datetime.timedelta(seconds=1)).strftime("%Y%m%dT%H%M%SZ")
    expected_key = f"{table_name}/{date_path}/{table_name}-{timestamp}.json"
    expected_key_2 = f"{table_name}/{date_path}/{table_name}-{timestamp_2}.json"
    expected_key_3 = f"{table_name}/{date_path}/{table_name}-{timestamp_3}.json"

    upload_response = upload_to_s3(input_json, bucket_name, table_name)

    response = mock_client.list_objects(Bucket=bucket_name)

    actual_key = response["Contents"][0]["Key"]

    assert (
        actual_key == expected_key
        or actual_key == expected_key_2
        or actual_key == expected_key_3
    )

    assert upload_response in {
        f"Uploaded to s3://{bucket_name}/{expected_key}",
        f"Uploaded to s3://{bucket_name}/{expected_key_2}",
        f"Uploaded to s3://{bucket_name}/{expected_key_3}",
    }


@pytest.mark.it(
    "upload_to_s3 raises a RuntimeError in the event of failure"
)
def test_upload_to_s3_error():
    bucket_name = "mock_bucket_2"
    input_data = [
        {
            "currency_id": 2,
            "currency_code": "USD",
            "created_at": "2022-11-03 14:20:49.962",
            "last_updated": "2022-11-03 14:20:49.962",
        },
        {
            "currency_id": 3,
            "currency_code": "EUR",
            "created_at": "2022-11-03 14:20:49.962",
            "last_updated": "2022-11-03 14:20:49.962",
        },
    ]
    input_json = json.dumps(input_data)
    table_name = "currency"
    
    with pytest.raises(RuntimeError):
        upload_to_s3(input_json, bucket_name, table_name)


@pytest.mark.it(
    "the ingest function extracts the data from the currency table, which contains datetime objects, changes them to json and saves in the given bucket"
)
def test_ingestion_works(mock_client):
    table_name = "currency"
    bucket_name = "mock_bucket_3"
    mock_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    now = datetime.datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    timestamp_2 = (now - datetime.timedelta(seconds=1)).strftime("%Y%m%dT%H%M%SZ")
    timestamp_3 = (now + datetime.timedelta(seconds=1)).strftime("%Y%m%dT%H%M%SZ")
    expected_key = f"{table_name}/{date_path}/{table_name}-{timestamp}.json"
    expected_key_2 = f"{table_name}/{date_path}/{table_name}-{timestamp_2}.json"
    expected_key_3 = f"{table_name}/{date_path}/{table_name}-{timestamp_3}.json"

    ingest(table_name, bucket_name)

    response = mock_client.list_objects(Bucket=bucket_name)

    actual_key = response["Contents"][0]["Key"]

    assert (
        actual_key == expected_key
        or actual_key == expected_key_2
        or actual_key == expected_key_3
    )


@pytest.mark.it(
    "the ingest function extracts the data from the sales_order table, which contains decimal objects, changes them to json and saves in the given bucket"
)
def test_sales_order_ingestion(mock_client):
    table_name = "sales_order"
    bucket_name = "mock_bucket_3"
    now = datetime.datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    timestamp_2 = (now - datetime.timedelta(seconds=1)).strftime("%Y%m%dT%H%M%SZ")
    timestamp_3 = (now + datetime.timedelta(seconds=1)).strftime("%Y%m%dT%H%M%SZ")
    expected_key = f"{table_name}/{date_path}/{table_name}-{timestamp}.json"
    expected_key_2 = f"{table_name}/{date_path}/{table_name}-{timestamp_2}.json"
    expected_key_3 = f"{table_name}/{date_path}/{table_name}-{timestamp_3}.json"

    ingest(table_name, bucket_name)

    response = mock_client.list_objects(Bucket=bucket_name)
    actual_key = response["Contents"][1]["Key"]
    assert (
        actual_key == expected_key
        or actual_key == expected_key_2
        or actual_key == expected_key_3
    )
