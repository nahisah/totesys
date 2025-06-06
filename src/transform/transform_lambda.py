import boto3
import json
import os

def lambda_handler(event, context):
    ingested_data = get_all_table_data_from_ingest_bucket()
    
    # Build the dataframes from ingested_data, a dict whose keys are table names
    # and whose values are dicts with the data from that table


def get_table_data_from_ingest_bucket(table_name, bucket_name):
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
    table_names = ["sales_order",
                    "design",
                    "address",
                    "counterparty",
                    "staff",
                    "currency",
                    "department"
    ]
    ingested_data = {}
    for table_name in table_names:
        ingested_data[table_name] = get_table_data_from_ingest_bucket(table_name, os.environ["INGESTION_BUCKET_NAME"])
    return ingested_data
