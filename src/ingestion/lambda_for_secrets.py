import json
import os
import requests
from src.ingestion.ingest import ingest

def lambda_handler(event, context):
    try:
        secret_name = "arn:aws:secretsmanager:eu-west-2:389125938424:secret:Totesys_DB_Credentials-4f8nsr"
        
        secrets_extension_endpoint = f"http://localhost:2773/secretsmanager/get?secretId={secret_name}"
        headers = {"X-Aws-Parameters-Secrets-Token": os.environ.get('AWS_SESSION_TOKEN')}
        
        response = requests.get(secrets_extension_endpoint, headers=headers)
        print(f"Response status code: {response.status_code}")
        
        secret = json.loads(response.text)["SecretString"]
        print(f"Retrieved secret: {secret}")
        secret = json.loads(secret)
        os.environ["DBUSER"] = secret["user"]
        os.environ["DBNAME"] = secret["database"]
        os.environ["DBPASSWORD"] = secret["password"]
        os.environ["PORT"] =secret["port"]
        os.environ["HOST"] = secret["host"]
        
        ingest("sales_order",os.environ["BUCKET_NAME"])
       
        return {
            'statusCode': response.status_code,
            'body': json.dumps({
                'message': 'Successfully retrieved secret',
                'secretRetrieved': True
            })
        }
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Error retrieving secret',
                'error': str(e)
            })
        }