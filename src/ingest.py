# a function to connect to database that would use parameterized queries with pg8000 to avoid SQL INJECTION
# a function that would convert to json and upload  to the s3 bucket
# the data would be saved under the keys that were agreed on

# a function to connect to database that would use parameterized queries with pg8000 to avoid SQL INJECTION
import pg8000
import json
import boto3
from botocore.exceptions import ClientError
import datetime


from utils.db_connection import create_conn, close_conn



# datestamp change to a supported function
# do a big function that runs everything with all the table names
# look at the main.py file and decide what we need from it
# write documentation - at least docstrings



# tasks for the future:
# PROTECT FROM SQL INJECTION
# update this function to only consider UPDATES/NEW INFORMATION IN THE TOTESYS DATABASE
# update this function and db_connection.py to connect to real database
# make sure that it works with our real bucket too



def extract_data(table_name):
    """
    This function will connect to the linked database and select all the information in the given table.

    Arguments:
    table_name - string, the name of the table in the database that we want to extract

    Returns:
    a list of dictionaries where each dictionary represents a single row in the given table and the keys are the column names in the given table
    """

    query = f"SELECT * FROM {table_name};"

    conn = create_conn()

    if conn:
        try:
            data = conn.run(query)
            columns = [column['name'] for column in conn.columns]
            result = [dict(zip(columns,row)) for row in data]
            return result
        except pg8000.exceptions.DatabaseError as e:
            raise RuntimeError(f"Database query failed: {e}")
        finally:
            close_conn(conn)



# a function that would convert to json and upload  to the s3 bucket
# the data would be saved under the keys that were agreed on

def upload_to_s3(data, bucket_name, table_name):
    pass

#     s3 = boto3.client("s3")

#     # Current UTC timestamp
#     now = datetime.datetime.utcnow()
#     date_path = now.strftime("%Y/%m/%d")
#     timestamp = now.strftime("%Y%m%dT%H%M%SZ")

#     # Format: table_name/YYYY/MM/DD/table_name-YYYYMMDDTHHMMSSZ.json
#     key_name = f"{table_name}/{date_path}/{table_name}-{timestamp}.json"

#     try:
#         s3.put_object(
#             Bucket=bucket_name,
#             Key=key_name,
#             Body=json.dumps(data),
#             ContentType="application/json"
#         )
#         print(f"Uploaded to s3://{bucket_name}/{key_name}")
#         return key_name
#     except ClientError as e:
#         print(f"S3 upload failed: {e}")
#         raise


