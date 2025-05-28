resource "aws_s3_bucket" "ingestion-bucket" {
  bucket_prefix = "bucket-one-ingestion"
}

resource "aws_s3_bucket" "processed-bucket" {
  
  bucket_prefix = "bucket-two-processed"
}