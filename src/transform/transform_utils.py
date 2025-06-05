import boto3
import json
import os

def get_table_data_from_ingest_bucket(table_name, bucket_name):
    """
    Connects to the ingestion s3 bucket and retrieves the most recent data for the given table.

    Arguments: 
        table_name: the name of the table in the bucket to retrieve data from
        bucket_name: the name of the s3 bucket, which should be the ingestion bucket

    Returns:
        A dictionary whose keys are column names from the ingested table and whose values are the most recent entires for that table

    Raises: 
        RuntimeError on error
    """
    # TODO: check what happens if multiple objects get uploaded 
    # to bucket by ingestion process
    try:
        client = boto3.client("s3")
        listing_response = client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=table_name
        )
        object_key = listing_response["Contents"][-1]["Key"]
        retrieval_response = client.get_object(
            Bucket=bucket_name,
            Key=object_key
        )
        body = retrieval_response["Body"].read().decode("utf-8")
        return json.loads(body)
    
    except Exception as e:
        raise RuntimeError(f"Retrieval of data from ingest bucket failed: {e}")


def get_all_table_data_from_ingest_bucket():
    """
    Retrieves the most recent data for each table stored in the ingestion s3 bucket.

    Returns:
        A dictionary whose keys are table names and whose values are dictionaries containing that table's data
    """
    table_names = [
        "sales_order",
        "design",
        "address",
        "counterparty",
        "staff",
        "currency",
        "department"
    ]
    ingested_data = {}
    for table_name in table_names:
        ingested_data[table_name] = get_table_data_from_ingest_bucket(table_name, os.environ["BUCKET_NAME"])
    
    return ingested_data


def transform_fact_sales_order(ingested_data):
    pass