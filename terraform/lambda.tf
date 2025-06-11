resource "aws_lambda_function" "ingestion_lambda" {

  count = var.deploy_lambda_bool ? 1 : 0
  function_name = "ingestion_lambda"
  s3_bucket = aws_s3_bucket.code-bucket.bucket
  s3_key    = "ingestion.zip"
  role      = aws_iam_role.ingestion_lambda_role.arn
  handler   = "ingest_lambda.lambda_handler"
  layers    = ["arn:aws:lambda:eu-west-2:133256977650:layer:AWS-Parameters-and-Secrets-Lambda-Extension:17"]
  timeout   = 30
  runtime   = "python3.13"

  environment {
    variables = {
      INGESTION_BUCKET_NAME = aws_s3_bucket.ingestion-bucket.bucket
      STEP_MACHINE_ARN = aws_sfn_state_machine.totesys_state_machine.arn
    }
  }
}


resource "aws_lambda_function" "transform_lambda" {

  count         = var.deploy_lambda_bool ? 1 : 0
  function_name = "transform_lambda"
  s3_bucket     = aws_s3_bucket.code-bucket.bucket
  s3_key        = "transform-lambda.zip"
  role          = aws_iam_role.transform_lambda_role.arn
  handler       = "transform_lambda.lambda_handler"
  layers    = ["arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python313:2"]
  timeout = 30
  runtime = "python3.13"
  memory_size = 512

  environment {
    variables = {
      TRANSFORM_BUCKET_NAME = aws_s3_bucket.processed-bucket.bucket
      INGESTION_BUCKET_NAME = aws_s3_bucket.ingestion-bucket.bucket
    }
  }
}

resource "aws_lambda_function" "load_lambda" {

  count         = var.deploy_lambda_bool ? 1 : 0
  function_name = "load_lambda"
  s3_bucket     = aws_s3_bucket.code-bucket.bucket
  s3_key        = "load-lambda.zip"
  role          = aws_iam_role.load_lambda_role.arn
  handler       = "load_lambda.lambda_handler"
  layers    = ["arn:aws:lambda:eu-west-2:133256977650:layer:AWS-Parameters-and-Secrets-Lambda-Extension:17", "arn:aws:lambda:eu-west-2:336392948345:layer:AWSSDKPandas-Python313:2"]
  timeout = 30
  runtime = "python3.13"
  memory_size = 512

  environment {
    variables = {
      TRANSFORM_BUCKET_NAME = aws_s3_bucket.processed-bucket.bucket
    }
  }
}