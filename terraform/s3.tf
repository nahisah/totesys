resource "aws_s3_bucket" "ingestion-bucket" {
  bucket_prefix       = "bucket-one-ingestion"
  object_lock_enabled = true
  tags = {
  BucketUsage = "Bucket for data ingestion" }
}

resource "aws_s3_bucket_versioning" "ingestion_versioning" {
  bucket = aws_s3_bucket.ingestion-bucket.id
  versioning_configuration {

    status = "Enabled"
  }
}

resource "aws_s3_bucket_object_lock_configuration" "ingestion_bucket_lock" {
  bucket = aws_s3_bucket.ingestion-bucket.id
  rule {
    default_retention {

      mode = "GOVERNANCE"
      days = 5
    }
  }
}

resource "aws_s3_bucket" "processed-bucket" {

  bucket_prefix       = "bucket-two-processed"
  object_lock_enabled = true
  tags = {
    BucketUsage = "Bucket for processed/transformed data"
  }
}

resource "aws_s3_bucket_versioning" "processed_versioning" {
  bucket = aws_s3_bucket.processed-bucket.id
  versioning_configuration {

    status = "Enabled"
  }
}

resource "aws_s3_bucket_object_lock_configuration" "processed_bucket_lock" {
  bucket = aws_s3_bucket.processed-bucket.id
  rule {
    default_retention {

      mode = "GOVERNANCE"
      days = 5
    }
  }
}


resource "aws_s3_bucket" "code-bucket" {
  bucket_prefix = "code-bucket"
  tags = {
    BucketUsage = "Bucket to store code for lambda functions"
  }
}




