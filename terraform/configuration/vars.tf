variable "region_name" {
  default = "eu-west-2"

}

# variable "code-bucket" {
#   default = module.code-bucket.code-bucket
# }

resource "aws_s3_bucket" "code-bucket" {
  bucket = module.code-bucket.code-bucket.bucket
  
}