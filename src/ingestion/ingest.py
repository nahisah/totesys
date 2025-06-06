import datetime
import json
from datetime import timezone

import boto3
from pg8000.native import identifier

from utils.db_connection import close_conn, create_conn
from utils.default_serialiser import default_serialiser


def extract_data(table_name):
    """
    This function connects to the linked database and selects all the information in the given table.

    Arguments:
        table_name: string, the name of the table in the database that we want to extract

    Returns:
        A list of dictionaries where each dictionary represents a single row in the given table
        and the keys are the column names in the given table

    Raises:
        RuntimeError: Raises an exception
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
    This function converts an object (which is supposed to be the list of dictionaries
    that the extract_data function returns) into a json object.

    Arguments:
        data: list of dictionaries

    Returns:
        A json object
    """

    return json.dumps(data, default=default_serialiser)


def upload_to_s3(data, bucket_name, table_name):
    """
    This function takes a json object and uploads it to a given bucket with a key that includes table name and datestamp

    Arguments:
        data: a json object
        bucket_name: a string representing the name of s3 bucket
        table_name: a string respresenting the table the data from which we are uploading

    Returns:
        A message confirming successful upload and showing the full key
    """

    s3 = boto3.client("s3")

    now = datetime.datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%dT%H%MZ")

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

    This function calls extraction function, and transforms the data through converted_data function.
    It then uploads the data into the s3 bucket. If sucessful it returns successful, else it raises an error.

    Arguments:
        table_name: string representing the name of the table the data is from
        bucket_name: string representing the name of the s3 bucket that is being uploaded to

    Return:
        A string indicating: 'Ingestion successful'

    Raises:
        RuntimeError: Raises an exception

    """

    try:
        extracted_data = extract_data(table_name)

        converted_data = convert_to_json(extracted_data)

        upload_to_s3(converted_data, bucket_name, table_name)

        return "Ingestion successful"

    except Exception as e:
        raise RuntimeError(f"Ingestion failed: {e}")
