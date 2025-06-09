import awswrangler as wr
import os

os.environ["TRANSFORM_BUCKET_NAME"] = "processed-bucket"
def get_parquet_data_from_processed_bucket(table_name):

    s3_path = f"s3://{os.environ["TRANSFORM_BUCKET_NAME"]}/{table_name}/"

    try:
        df = wr.s3.read_parquet(path=s3_path)
        print (df)
    except Exception as e:
        raise RuntimeError(f"Error reading parquet from S3 for {table_name}: {e}")
    

print(get_parquet_data_from_processed_bucket("dim_design"))