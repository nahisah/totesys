resource "aws_lambda_function" "ingestion_lambda" {

  count = var.deploy_lambda_bool ? 1 : 0
  function_name = "ingestion_lambda"
  #   filename      = data.archive_file.ingestion_zip.output_path ## reference to zipped ingestion lambda
  s3_bucket = aws_s3_bucket.code-bucket.bucket
  s3_key    = "ingestion.zip"
  role      = aws_iam_role.ingestion_lambda_role.arn
  handler   = "secrets.lambda_handler"
  layers    = ["arn:aws:lambda:eu-west-2:133256977650:layer:AWS-Parameters-and-Secrets-Lambda-Extension:17"]
  timeout   = 20
  runtime   = "python3.9"

  environment {
    variables = {
      BUCKET_NAME = aws_s3_bucket.ingestion-bucket.bucket ##referenced as os.environ["BUCKET_NAME"] in python
    }
  }
}




