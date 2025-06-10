import os
import json
import requests



def conn_to_warehouse():
    try:
        secret_name = "arn:aws:secretsmanager:eu-west-2:389125938424:secret:datawarehouse-zhlI93"

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

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps({"message": "Error!", "error": str(e)}),
        }
    print(os.environ["DBUSER"])
conn_to_warehouse()