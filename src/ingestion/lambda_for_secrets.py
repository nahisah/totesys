import json
import os

import requests

from src.ingestion.ingest import ingest
import boto3


def lambda_handler(event, context):
    """
    This function will get database credentials from AWS Secrets Manager and run the
    ingest function (inputs data into s3 bucket)for all tables in the database.

    Returns:
    A status code(200) signifying a successful input into the S3 bucket
    OR
    A status code(500) signifying an unsuccessful attempt

    """

    step_function = os.environ["STEP_MACHINE_ARN"]
    client = boto3.client("stepfunctions",region_name="eu-west-2")
    response = client.start_execution(
        stateMachineArn=step_function
    )
    

    try:
        secret_name = "arn:aws:secretsmanager:eu-west-2:389125938424:secret:Totesys_DB_Credentials-4f8nsr"

        secrets_extension_endpoint = (
            f"http://localhost:2773/secretsmanager/get?secretId={secret_name}"
        )
        headers = {
            "X-Aws-Parameters-Secrets-Token": os.environ.get("AWS_SESSION_TOKEN")
        }

        response = requests.get(secrets_extension_endpoint, headers=headers)
        print(f"Response status code: {response.status_code}")

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
            ingest(table, os.environ["INGESTION_BUCKET_NAME"])

        return {"statusCode": response.status_code}

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Error!", "error": str(e)}),
        }
