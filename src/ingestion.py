# a function to connect to database that would use parameterized queries with pg8000 to avoid SQL INJECTION
# a function that would convert to json and upload  to the s3 bucket
# the data would be saved under the keys that were agreed on

# a function to connect to database that would use parameterized queries with pg8000 to avoid SQL INJECTION
import pg8000
import json
import boto3
from botocore.exceptions import ClientError
import datetime


def extract_data(table_name, db_config):
    conn = pg8000.connect(**db_config)
    cursor = conn.cursor()
    
    query = pg8000.dbapi.SQL("SELECT * FROM {}").format(pg8000.dbapi.Identifier(table_name))
    cursor.execute(query)
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()
    conn.close()

    result = [dict(zip(columns, row)) for row in rows]
    return result

# a function that would convert to json and upload  to the s3 bucket
# the data would be saved under the keys that were agreed on

def upload_to_s3(data, bucket_name, table_name):
    s3 = boto3.client("s3")

    # Current UTC timestamp
    now = datetime.datetime.utcnow()
    date_path = now.strftime("%Y/%m/%d")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")

    # Format: table_name/YYYY/MM/DD/table_name-YYYYMMDDTHHMMSSZ.json
    key_name = f"{table_name}/{date_path}/{table_name}-{timestamp}.json"

    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=key_name,
            Body=json.dumps(data),
            ContentType="application/json"
        )
        print(f"Uploaded to s3://{bucket_name}/{key_name}")
        return key_name
    except ClientError as e:
        print(f"S3 upload failed: {e}")
        raise


