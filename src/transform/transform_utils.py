import boto3
import json
import os
import pandas as pd
from datetime import datetime, timezone
import awswrangler as wr
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
    print(df)
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

def transform_dim_staff(staff, department):
    staff_df = pd.DataFrame(staff)
    department_df = pd.DataFrame(department)
    
    del staff_df["created_at"]
    del staff_df["last_updated"]
    del department_df["created_at"]
    del department_df["last_updated"]
    del department_df["manager"]
    
    df = pd.merge(staff_df, department_df, on="department_id", how="left")
    
    del df["department_id"]
    columns_in_order = [
        "staff_id",
        "first_name",
        "last_name",
        "department_name", 
        "location", 
        "email_address"
    ]
    df = df[columns_in_order]
    return df

def transform_dim_counterparty(counterparty_data, address_data):
    counterparty_df = pd.DataFrame(counterparty_data)
    address_df = pd.DataFrame(address_data)
    counterparty_df = counterparty_df.rename(columns={'legal_address_id': 'address_id'})
    df = pd.merge(counterparty_df, address_df, on="address_id", how="left")
    
    df = df[[
        "counterparty_id", 
        "counterparty_legal_name", 
        "address_line_1",
        "address_line_2",
        "district",
        "city",
        "postal_code",
        "country",
        "phone"
    ]]
    df = df.rename(columns={
        'address_line_1': 'counterparty_legal_address_line_1',
        "address_line_2": "counterparty_legal_address_line_2",
        "district": "counterparty_legal_district",
        "city": "counterparty_legal_city",
        "postal_code": "counterparty_legal_postal_code",
        "country": "counterparty_legal_country",
        "phone": "counterparty_legal_phone"
    })

    return df
    
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
    now = datetime.now(timezone.utc)
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    key = f"{table_name}/{date_path}/{table_name}-{timestamp}.parquet"
    
    s3_url = f's3://{bucket_name}/{key}'
    try:
        wr.s3.to_parquet(
            df=dataframe,
            path=s3_url,
        )

        message = f"s3://{bucket_name}/{key}"
        print(message)
        return message
    
    except Exception as e:
        raise RuntimeError(f"Database query failed: {e}")