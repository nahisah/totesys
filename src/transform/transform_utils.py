import boto3
import json
import os
import pandas as pd
from datetime import datetime, timezone
import s3fs
from currency_codes import get_currency_by_code

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

    df["created_date"] = pd.to_datetime(df['created_date'], format="%Y-%m-%d").dt.date
    df["last_updated_date"] = pd.to_datetime(df['last_updated_date'], format="%Y-%m-%d").dt.date
    df["created_time"] = pd.to_datetime(df['created_time'], format="mixed")
    df["last_updated_time"] = pd.to_datetime(df['last_updated_time'], format="mixed")
    df["agreed_payment_date"] = pd.to_datetime(df['agreed_payment_date'], format="%Y-%m-%d").dt.date
    df["agreed_delivery_date"] = pd.to_datetime(df['agreed_delivery_date'], format="%Y-%m-%d").dt.date

    # delete unnecessary columns
    del df['created_at']
    del df['last_updated']

    # add sales_record_id column as index
    df.insert(0, 'sales_record_id', df.index)

    df = df.rename(columns={'staff_id': 'sales_staff_id'})

    columns_in_order = ['sales_record_id', 'sales_order_id', 'created_date',         'created_time', 'last_updated_date', 'last_updated_time', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'design_id', 'agreed_payment_date', 'agreed_delivery_date', 'agreed_delivery_location_id']

    df = df[columns_in_order]

    return df

def transform_dim_design(design_data):

    df = pd.DataFrame(design_data)
    del df["created_at"]
    del df["last_updated"]
    
    return df

def transform_dim_currency(currency_data):
    df = pd.DataFrame(currency_data)
    del df["created_at"]
    del df["last_updated"]
    
    df["currency_name"] = df['currency_code'].apply(get_currency_by_code).apply(lambda x: x.name)
    
    return df

def transform_dim_location(location_data):
    df = pd.DataFrame(location_data)
    del df["created_at"]
    del df["last_updated"]
    
    df = df.rename(columns={'address_id': 'location_id'})
    
    return df

def transform_dim_date(transformed_fact_sales_data):
    """
    Function requires the dataframe from transform_fact_sales_order as an argument.
    """
    old_df = transformed_fact_sales_data
    dates_1 = old_df["created_date"]
    dates_2 = old_df["last_updated_date"]
    dates_3 = old_df["agreed_payment_date"]
    dates_4 = old_df["agreed_delivery_date"]
    all_dates = pd.concat([dates_1, dates_2, dates_3, dates_4]).drop_duplicates(ignore_index=True)
    
    df = all_dates.to_frame(name="date_id")
    df["year"] = pd.to_datetime(df["date_id"]).dt.year
    df["month"] = pd.to_datetime(df["date_id"]).dt.month
    df["day"] = pd.to_datetime(df["date_id"]).dt.day
    df["day_of_week"] = pd.to_datetime(df["date_id"]).dt.day_of_week
    df["day_name"] = pd.to_datetime(df["date_id"]).dt.day_name()
    df["month_name"] = pd.to_datetime(df["date_id"]).dt.month_name()
    df["quarter"] = pd.to_datetime(df["date_id"]).dt.quarter
    
    return df
    



def convert_to_parquet(table_df=pd.DataFrame):
    
    parquet_file = table_df.to_parquet(path=None, index=False, engine='pyarrow')

    return parquet_file



def upload_to_s3(dataframe, bucket_name, table_name):
    """
    This function takes a json object and uploads it to a given bucket with a key that includes table name and datestamp
    
    Arguments:
        data: a bytes object, suitable for storage as parquet
        bucket_name: a string representing the name of s3 bucket
        table_name: a string representing the table in the warehouse which we are uploading to

    Returns:
        A message confirming successful upload and showing the full key    
    """
    # s3 = s3fs.S3FileSystem()

    # s3 = boto3.client('s3')
    now = datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    key = f"{table_name}/{date_path}/{table_name}-{timestamp}.parquet"
    
    s3_url = f's3://{bucket_name}/{key}'
    try:
        dataframe.to_parquet(s3_url)
        # with s3.open(f"{bucket_name}/{key}", "wb") as f:
        #     dataframe.to_parquet(f)
        # s3.put_object(
        #     Bucket=bucket_name,
        #     Key=key,
        #     Body=data
        # )

        message = f"s3://{bucket_name}/{key}"
        print(message)
        return message
    
    except Exception as e:
        raise RuntimeError(f"Database query failed: {e}")