import pytest
import json
import datetime
import boto3
from moto import mock_aws
from datetime import timezone

from utils.db_connection import create_conn, close_conn
from utils.normalise_datetime import normalise_datetimes
from totesys.src.ingestion.ingest import extract_data, convert_to_json, upload_to_s3, ingest


# fixture for connecting to database
@pytest.fixture(scope="module")
def db():
    """Create a database connection and closes it at the end"""
    db = create_conn()
    yield db
    close_conn(db)


@pytest.mark.it('extract_data gives us all the information in a given table in list of dictionaries format')
def test_extract_data_full_table(db):
    # arrange
    table_name = 'currency'
    expected = [{'currency_id': 2, 'currency_code': 'USD', 'created_at': '2022-11-03 14:20:49.962', 'last_updated': '2022-11-03 14:20:49.962'}, {'currency_id': 3, 'currency_code': 'EUR', 'created_at': '2022-11-03 14:20:49.962', 'last_updated': '2022-11-03 14:20:49.962'}]
    #act
    actual = normalise_datetimes(extract_data(table_name))
    #assert
    assert actual == expected


@pytest.mark.it('extract_data gives us an informative error message in the event of failure')
def test_informative_error_message():
    # arrange
    table_name = 'restaurants'
    # act

    # assert
    with pytest.raises(RuntimeError):
        extract_data(table_name)




@pytest.mark.it('convert_to_json converts a list of dictionaries into a .json file')
def test_converts_to_json():
    # arrange
    input_data = [{'currency_id': 2, 'currency_code': 'USD', 'created_at': '2022-11-03 14:20:49.962', 'last_updated': '2022-11-03 14:20:49.962'}, {'currency_id': 3, 'currency_code': 'EUR', 'created_at': '2022-11-03 14:20:49.962', 'last_updated': '2022-11-03 14:20:49.962'}]

    #act
    converted = convert_to_json(input_data)

    def is_it_json(input):
        try:
            json.loads(input)
            return True
        except (json.JSONDecodeError, ValueError):
            return False
        
    assert is_it_json(converted) == True



# fixture mocking an s3 client with s3 bucket in it
@pytest.fixture(scope="module")
def mock_client():
    with mock_aws():
        client = boto3.client('s3')
        yield client


@pytest.mark.it('upload_to_s3 uploads json to s3 with a correct key')
def test_correct_upload(mock_client):
    # arrange
    bucket_name = 'mock_bucket'
    mock_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
    input_data = [{'currency_id': 2, 'currency_code': 'USD', 'created_at': '2022-11-03 14:20:49.962', 'last_updated': '2022-11-03 14:20:49.962'}, {'currency_id': 3, 'currency_code': 'EUR', 'created_at': '2022-11-03 14:20:49.962', 'last_updated': '2022-11-03 14:20:49.962'}]
    input_json = json.dumps(input_data)
    table_name = 'currency'
    now = datetime.datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    expected_key = f"{table_name}/{date_path}/{table_name}-{timestamp}.json"

    # act
    upload_response = upload_to_s3(input_json, bucket_name, table_name)

    # assert
    response = mock_client.list_objects(Bucket=bucket_name)

    actual_key = response['Contents'][0]['Key']

    assert actual_key == expected_key

    assert upload_response == f"Uploaded to s3://{bucket_name}/{expected_key}"






@pytest.mark.it('upload_to_s3 gives us an informative error message in the event of failure')
def test_informative_error_message_upload(mock_client):
    # arrange
    bucket_name = 'mock_bucket_2'
    input_data = [{'currency_id': 2, 'currency_code': 'USD', 'created_at': '2022-11-03 14:20:49.962', 'last_updated': '2022-11-03 14:20:49.962'}, {'currency_id': 3, 'currency_code': 'EUR', 'created_at': '2022-11-03 14:20:49.962', 'last_updated': '2022-11-03 14:20:49.962'}]
    input_json = json.dumps(input_data)
    table_name = 'currency'

    # act

    # assert
    with pytest.raises(RuntimeError):
        upload_to_s3(input_json, bucket_name, table_name)


@pytest.mark.it('the ingest function extracts the data, changes it to json and saves in the given bucket')
def test_ingestion_works(mock_client):
    # arrange
    table_name = 'currency'
    bucket_name = 'mock_bucket_3'
    mock_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": "eu-west-2"})
    now = datetime.datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    expected_key = f"{table_name}/{date_path}/{table_name}-{timestamp}.json"

    # act
    ingest(table_name, bucket_name)
    
    # assert
    response = mock_client.list_objects(Bucket=bucket_name)

    actual_key = response['Contents'][0]['Key']

    assert actual_key == expected_key






