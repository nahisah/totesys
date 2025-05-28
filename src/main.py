from ingestion import extract_data, upload_to_s3
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("table", help="Name of the table to extract")
    args = parser.parse_args()

    DB_CONFIG = {
        "user": "your_user",
        "password": "your_password",
        "host": "your_host",
        "database": "totesys"
    }

    BUCKET_NAME = "your-s3-bucket-name"
    TABLE_NAME = args.table

    data = extract_data(TABLE_NAME, DB_CONFIG)
    upload_to_s3(data, BUCKET_NAME, TABLE_NAME)