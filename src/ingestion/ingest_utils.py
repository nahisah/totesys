"""
Contains the utility functions for the ingestion lambda_handler.
"""

import datetime
import json
from datetime import timezone

import boto3
from pg8000.native import identifier

from src.utils.db_connection import close_conn, create_conn
from src.utils.default_serialiser import default_serialiser


def extract_data(table_name):
    """
    This function connects to the database whose credentials are stored as environment variables and selects all the information in the given table.

    # Arguments:
        table_name: a string representing the name of the table in the database that we want to extract.

    # Returns:
        A list of dictionaries where each dictionary represents a single row in the given table and the keys are the column names in the given table.

    # Raises:
        RuntimeError: An error occurred during data extraction.
    """

    query = f"SELECT * FROM {identifier(table_name)}"

    conn = create_conn()

    if conn:
        try:
            data = conn.run(query)
            columns = [column["name"] for column in conn.columns]
            result = [dict(zip(columns, row)) for row in data]
            return result
        except Exception as e:
            raise RuntimeError(f"Database query failed: {e}")
        finally:
            close_conn(conn)


def convert_to_json(data):
    """
    This function converts a list of dictionaries that the extract_data function returns into a json object. It is functionally identical to the json.dumps method, with a specified function for the default argument.

    # Arguments:
        data: a list of dictionaries.

    # Returns:
        A json object.
    """

    return json.dumps(data, default=default_serialiser)


def upload_to_s3(data, bucket_name, table_name):
    """
    This function takes a json object and uploads it to a given bucket with a key that includes table name and datestamp.

    # Arguments:
        data: a json object containing the data for a table in the database.
        bucket_name: a string representing the name of the s3 bucket to upload to.
        table_name: a string representing the table whose data we are uploading.

    # Returns:
        A message confirming successful upload and showing the full location.
    """

    s3 = boto3.client("s3")

    now = datetime.datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    key = f"{table_name}/{date_path}/{table_name}-{timestamp}.json"

    try:
        s3.put_object(
            Bucket=bucket_name, Key=key, Body=data, ContentType="application/json"
        )

        message = f"Uploaded to s3://{bucket_name}/{key}"
        print(message)
        return message

    except Exception as e:
        raise RuntimeError(f"Database query failed: {e}")


def ingest(table_name, bucket_name):
    """
    This function calls extract_data, and converts the data to json through the convert_to_json function. It then uploads the data into the given s3 bucket.

    # Arguments:
        table_name: a string representing the name of the table the data is from.
        bucket_name: a string representing the name of the s3 bucket that is being uploaded to.

    # Return:
        A string indicating successful extraction of the data.

    # Raises:
        RuntimeError: An error occurred during data extraction.
    """

    try:
        extracted_data = extract_data(table_name)

        converted_data = convert_to_json(extracted_data)

        upload_to_s3(converted_data, bucket_name, table_name)

        return "Ingestion successful"

    except Exception as e:
        raise RuntimeError(f"Ingestion failed: {e}")
