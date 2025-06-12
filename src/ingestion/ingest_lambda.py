"""
Contains the main function used by the Ingestion Lambda AWS resource.
"""

import json
import os
import logging

import boto3
import requests

from src.ingestion.ingest_utils import ingest


def lambda_handler(event, context):
    """
    This function will get database credentials from AWS Secrets Manager and store the data in the ingestion s3 bucket in json format for all tables in the database.

    # Returns:
        A message with status code 200 on successful extraction of the data from the database into the s3 bucket.
        A message with status code 500 on an unsuccessful attempt.
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    try:
        secret_name = "arn:aws:secretsmanager:eu-west-2:389125938424:secret:Totesys_DB_Credentials-4f8nsr"

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

        table_names = [
            "sales_order",
            "design",
            "address",
            "counterparty",
            "staff",
            "currency",
            "department",
        ]
        # Only 7 out of 11 tables included to match mock database
        # To extract ALL tables include missing table names
        for table in table_names:
            logger.info(f"Ingesting {table} table.")
            ingest(table, os.environ["INGESTION_BUCKET_NAME"])

        step_function = os.environ["STEP_MACHINE_ARN"]
        client = boto3.client("stepfunctions", region_name="eu-west-2")
        sf_running = client.list_executions(
            stateMachineArn=os.environ["STEP_MACHINE_ARN"], statusFilter="RUNNING"
        )
        sf_running_check = sf_running.get("executions", [])
        if not sf_running_check:
            client.start_execution(stateMachineArn=step_function)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Data successfully extracted"}),
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Error!", "error": str(e)}),
        }
