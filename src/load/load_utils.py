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
    
def load_dim_dates_into_warehouse(df):
    conn = create_conn()
    if conn:
        try:
            query = f"""
            INSERT INTO dim_date
            VALUES 
            """

            for _, row in df.iterrows():
                query += f"('{row["date_id"]}', {row["year"]}, {row["month"]}, {row["day"]}, {row["day_of_week"]}, '{row["day_name"]}', '{row["month_name"]}', {row["quarter"]}), "
            query = query[:-2]
            query += "ON CONFLICT (date_id) DO NOTHING;"
            
            conn.run(query)
        
        except Exception as e:
            raise RuntimeError(f"Database query failed: {e}")
        
        finally:
            close_conn(conn)

def load_dim_staff_into_warehouse(df):
    conn = create_conn()
    if conn:
        try:
            query = f"""
            INSERT INTO dim_staff
            VALUES 
            """

            for _, row in df.iterrows():
                query += f"({row["staff_id"]}, $${row["first_name"]}$$, $${row["last_name"]}$$, '{row["department_name"]}', '{row["location"]}', $${row["email_address"]}$$), "
            query = query[:-2]
            query += " ON CONFLICT (staff_id) DO NOTHING;"
            
            conn.run(query)
        
        except Exception as e:
            raise RuntimeError(f"Database query failed: {e}")
        
        finally:
            close_conn(conn)
          