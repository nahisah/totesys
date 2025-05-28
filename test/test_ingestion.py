import pytest
from unittest.mock import patch, MagicMock
from src.ingestion import extract_data, upload_to_s3
import json
import datetime


#  Fixture for db_config
@pytest.fixture
def mock_db_config():
    return {"user": "test", "password": "test", "host": "localhost", "database": "totesys"}


#  Test extract_data
@patch("src.ingestion.pg8000.connect")
def test_extract_data(mock_connect, mock_db_config):
    # Setup fake connection, cursor, and rows
    mock_cursor = MagicMock()
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    result = extract_data("staff", mock_db_config)

    assert result == [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    mock_cursor.execute.assert_called_once()
    mock_conn.close.assert_called_once()


# Test upload_to_s3
@patch("src.ingestion.boto3.client")
@patch("src.ingestion.datetime")
def test_upload_to_s3(mock_datetime, mock_boto_client):
    # Prepare time and mocks
    mock_now = datetime.datetime(2025, 5, 28, 9, 12, 0)
    mock_datetime.datetime.utcnow.return_value = mock_now
    mock_datetime.datetime.strftime = datetime.datetime.strftime

    mock_s3 = MagicMock()
    mock_boto_client.return_value = mock_s3

    data = [{"id": 1, "name": "Alice"}]
    bucket = "test-bucket"
    table = "sales_order"
    expected_key = "sales_order/2025/05/28/sales_order-20250528T091200Z.json"

    key = upload_to_s3(data, bucket, table)

    assert key == expected_key
    mock_s3.put_object.assert_called_once_with(
        Bucket=bucket,
        Key=expected_key,
        Body=json.dumps(data),
        ContentType="application/json"
    )