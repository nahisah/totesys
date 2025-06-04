resource "aws_s3_bucket" "code-bucket" {
  bucket_prefix = "code-bucket"
  tags = {
    BucketUsage = "Bucket to store code for lambda functions"
  }
}