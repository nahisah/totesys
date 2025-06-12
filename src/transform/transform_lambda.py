"""
Contains the main function used by the Transform Lambda AWS resource.
"""

import json
import logging
import os

from src.transform.transform_utils import (
    get_all_table_data_from_ingest_bucket,
    transform_dim_counterparty,
    transform_dim_currency,
    transform_dim_date,
    transform_dim_design,
    transform_dim_location,
    transform_dim_staff,
    transform_fact_sales_order,
    upload_to_s3,
)


def lambda_handler(event, context):
    """
    This function will run the transform function on all tables in the ingestion bucket and upload them as parquet to the processed bucket.

    # Returns:
        A message with status code 200 on successful input into the processed bucket.
        A message with status code 500 on an unsuccessful attempt.

    """

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        
        ingested_data = get_all_table_data_from_ingest_bucket()
        logger.info("Extracted data from ingestion bucket.")

        fact_sales_order = transform_fact_sales_order(ingested_data["sales_order"])
        dim_design = transform_dim_design(ingested_data["design"])
        dim_currency = transform_dim_currency(ingested_data["currency"])
        dim_location = transform_dim_location(ingested_data["address"])
        dim_date = transform_dim_date(fact_sales_order)
        dim_staff = transform_dim_staff(
            ingested_data["staff"], ingested_data["department"]
        )
        dim_counterparty = transform_dim_counterparty(
            ingested_data["counterparty"], ingested_data["address"]
        )
        logger.info("Transformation of ingested data complete.")

        table_names = {
            "fact_sales_order": fact_sales_order,
            "dim_design": dim_design,
            "dim_currency": dim_currency,
            "dim_location": dim_location,
            "dim_date": dim_date,
            "dim_staff": dim_staff,
            "dim_counterparty": dim_counterparty,
        }

        for k, v in table_names.items():
            upload_to_s3(v, os.environ["TRANSFORM_BUCKET_NAME"], k)
            logger.info(f"Uploaded transformed data to S3 for table {k}.")
            

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Data successfully transformed"}),
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Error!", "error": str(e)}),
        }
