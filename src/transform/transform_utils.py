import boto3
import json
import os
import pandas as pd

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


def transform_fact_sales_order(data_sales_order):

    """
    Transforms data from the sales_order table into the format required for fact_sales_order

    Arguments:
    data in a form of a list of dictionaries representing the contents of the sales_order table


    Returns:
    a dataframe in the required format

    
    """



    df = pd.DataFrame(data_sales_order)

    df[["created_date","created_time"]] = df['created_at'].str.split('T', expand = True)
    df[["last_updated_date","last_updated_time"]] = df['last_updated'].str.split('T', expand = True)

    pd.to_datetime(df['created_date'], format="%Y-%m-%d")
    pd.to_datetime(df['last_updated_date'], format="%Y-%m-%d")
    pd.to_datetime(df['created_time'], format="mixed")
    pd.to_datetime(df['last_updated_time'], format="mixed")


    # delete unnecessary columns
    del df['created_at']
    del df['last_updated']

    # add sales_record_id column as index
    df.insert(0, 'sales_record_id', df.index)

    df = df.rename(columns={'staff_id': 'sales_staff_id'})

    columns_in_order = ['sales_record_id', 'sales_order_id', 'created_date',         'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']


    df = df[columns_in_order]

    return df



    


    