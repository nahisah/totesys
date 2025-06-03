resource "aws_lambda_function" "ingestion_lambda" {

  function_name = "ingestion_lambda"
  s3_bucket     = "code-bucket-totesys-project"
  s3_key        = "lambda-function.zip"
  role          = aws_iam_role.ingestion_lambda_role.arn
  handler       = "lambda_for_secrets.lambda_handler"
  layers        = ["arn:aws:lambda:eu-west-2:133256977650:layer:AWS-Parameters-and-Secrets-Lambda-Extension:17"]

  runtime = "python3.9"

  environment {
    variables = {
      BUCKET_NAME = aws_s3_bucket.ingestion-bucket.bucket
    }
  }
}





