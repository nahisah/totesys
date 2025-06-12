"""
Contains the main function used by the Load Lambda AWS resource.
"""

import json
import os
import logging
import requests

from src.load.load_utils import (
    access_files_from_processed_bucket,
    load_dim_counterparty_into_warehouse,
    load_dim_currency_into_warehouse,
    load_dim_dates_into_warehouse,
    load_dim_design_into_warehouse,
    load_dim_location_into_warehouse,
    load_dim_staff_into_warehouse,
    load_fact_sales_order_into_warehouse,
)


def lambda_handler(event, context):
    """
    This function will get warehouse credentials from AWS Secrets Manager, import the most recent transformed data from s3 and load it into the warehouse.

    # Returns:
        A message with status code 200 on successful loading into the warehouse.
        A message with status code 500 on unsuccessful loading of the data.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        secret_name = (
            "arn:aws:secretsmanager:eu-west-2:389125938424:secret:datawarehouse-zhlI93"
        )

        secrets_extension_endpoint = (
            f"http://localhost:2773/secretsmanager/get?secretId={secret_name}"
        )
        headers = {
            "X-Aws-Parameters-Secrets-Token": os.environ.get("AWS_SESSION_TOKEN")
        }

        response = requests.get(secrets_extension_endpoint, headers=headers)
        logger.info(f"Response status code: {response.status_code}")

        secret = json.loads(response.text)["SecretString"]
        secret = json.loads(secret)

        os.environ["DBUSER"] = secret["user"]
        os.environ["DBNAME"] = secret["database"]
        os.environ["DBPASSWORD"] = secret["password"]
        os.environ["PORT"] = secret["port"]
        os.environ["HOST"] = secret["host"]

        # Only 7 out of 11 tables included to match mock database
        # To extract ALL tables include missing table names
        table_names = {
            "dim_design": load_dim_design_into_warehouse,
            "dim_currency": load_dim_currency_into_warehouse,
            "dim_location": load_dim_location_into_warehouse,
            "dim_date": load_dim_dates_into_warehouse,
            "dim_staff": load_dim_staff_into_warehouse,
            "dim_counterparty": load_dim_counterparty_into_warehouse,
            "fact_sales_order": load_fact_sales_order_into_warehouse,
        }

        for table_name in table_names:
            df = access_files_from_processed_bucket(
                table_name, os.environ["TRANSFORM_BUCKET_NAME"]
            )
            logger.info(f"Extracted data from processed bucket for table {table_name}.")
            table_names[table_name](df)
            logger.info(f"Loaded {table_name} to the warehouse.")
        
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Data successfully loaded"}),
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Error!", "error": str(e)}),
        }
