import boto3
import awswrangler as wr
from utils.db_connection import create_conn,close_conn



def accessing_files_from_processed_bucket(table_name,bucket_name):
    try:
            client = boto3.client("s3")
            listing_response = client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=table_name
            )
            object_key = listing_response["Contents"][-1]["Key"]
            path = f"s3://{bucket_name}/{object_key}"
            response = wr.s3.read_parquet([path])
            return response
        
    except Exception as e:
        raise RuntimeError(f"Retrieval of data from processed bucket failed: {e}")
    
def load_data_frames_into_datawarehouse(df):
     
     conn = create_conn()
     query = ""
     if conn:
          conn.run()
          