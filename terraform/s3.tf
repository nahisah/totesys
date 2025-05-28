resource "aws_s3_bucket" "ingestion-bucket" {
  bucket_prefix = "bucket-one-ingestion"

  tags = {
    BucketUsage = "Bucket for data ingestion"}
}

resource "aws_s3_bucket" "processed-bucket" {
  
  bucket_prefix = "bucket-two-processed"
  tags = {
    BucketUsage = "Bucket for processed/transformed data"
  }
}