resource "aws_lambda_function" "ingestion_lambda" {

  function_name = "ingestion_lambda"
  #   filename      = data.archive_file.ingestion_zip.output_path ## reference to zipped ingestion lambda
  s3_bucket = "code-bucket-totesys-project"
  s3_key    = "lambda-funtion.zip"
  role      = aws_iam_role.ingestion_lambda_role.arn
  handler   = "secrets.lambda_handler"
  layers    = ["arn:aws:lambda:eu-west-2:133256977650:layer:AWS-Parameters-and-Secrets-Lambda-Extension:17"]

  runtime = "python3.9"

  environment {
    variables = {
      BUCKET_NAME = aws_s3_bucket.ingestion-bucket.bucket ##referenced as os.environ["BUCKET_NAME"] in python
    }
  }
}

# data "archive_file" "ingestion_zip" { ## Zipping ingestion lambda function
#   type        = "zip"
#   source_file = "${path.module}/../terraform/terraform_test/test_lambda_handler.py" ##local file path to be zipped
#   output_path = "${path.module}/../terraform/terraform_test/ingestion_function.zip" ## where zipped function will be stored
# }



