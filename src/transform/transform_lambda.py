from src.transform.transform_utils import get_all_table_data_from_ingest_bucket, transform_fact_sales_order, upload_to_s3, transform_dim_design, transform_dim_currency, transform_dim_location, transform_dim_date, transform_dim_staff, transform_dim_counterparty
import os

def lambda_handler(event, context):
    ingested_data = get_all_table_data_from_ingest_bucket()

      
    fact_sales_order = transform_fact_sales_order(ingested_data["sales_order"])
    dim_design = transform_dim_design(ingested_data["design"])
    dim_currency = transform_dim_currency(ingested_data["currency"])
    dim_location = transform_dim_location(ingested_data["address"])
    dim_date = transform_dim_date(fact_sales_order)
    dim_staff = transform_dim_staff(ingested_data["staff"], ingested_data["department"])
    dim_counterparty = transform_dim_counterparty(ingested_data["counterparty"], ingested_data["address"])
    
    table_names = {
        "fact_sales_order": fact_sales_order,
        "dim_design": dim_design,
        "dim_currency": dim_currency,
        "dim_location": dim_location,
        "dim_date": dim_date,
        "dim_staff": dim_staff,
        "dim_counterparty": dim_counterparty
    }
    
    for k, v in table_names.items():
        upload_to_s3(v, os.environ["TRANSFORM_BUCKET_NAME"], k)
    
    return "Success"
    
    #TODO:
    # Create proper error message with try/except
    # Change success message to something sensible
    # Check correct environment variable for the bucket name
    # Testing
    # Docstring


