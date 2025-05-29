import pytest
from unittest.mock import patch, MagicMock
import json
import datetime
import boto3
from moto import mock_aws

from utils.db_connection import create_conn, close_conn
from utils.normalise_datetime import normalise_datetimes
from src.ingest import extract_data, upload_to_s3


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
    with pytest.raises(Exception):
        extract_data(table_name)





# # upload to s3: let's still mock the s3 bucket
# # upload to s3: use moto to simulate the s3 bucket / aws infrastructure
# # upload to s3: check that a file was uploaded to our s3 bucket
# # check that the key is right
# # check that the format is right (json)
# # if the upload is unsuccessful, we get an informative error message



@pytest.fixture(scope="module")
def mock_bucket():
    with mock_aws():
        client = boto3.client('s3')
        region = 'eu-west-2'
        bucket_name = 'mock_bucket'





# # Test upload_to_s3
# @patch("src.ingestion.boto3.client")
# @patch("src.ingestion.datetime")
# def test_upload_to_s3(mock_datetime, mock_boto_client):
#     # Prepare time and mocks
#     mock_now = datetime.datetime(2025, 5, 28, 9, 12, 0)
#     mock_datetime.datetime.utcnow.return_value = mock_now
#     mock_datetime.datetime.strftime = datetime.datetime.strftime

#     mock_s3 = MagicMock()
#     mock_boto_client.return_value = mock_s3

#     data = [{"id": 1, "name": "Alice"}]
#     bucket = "test-bucket"
#     table = "sales_order"
#     expected_key = "sales_order/2025/05/28/sales_order-20250528T091200Z.json"

#     key = upload_to_s3(data, bucket, table)

#     assert key == expected_key
#     mock_s3.put_object.assert_called_once_with(
#         Bucket=bucket,
#         Key=expected_key,
#         Body=json.dumps(data),
#         ContentType="application/json"
#     )