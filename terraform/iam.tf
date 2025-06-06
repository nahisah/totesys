resource "aws_iam_role" "ingestion_lambda_role" { ##Blank role for ingestion lambda
  name               = "lambda_role"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

data "aws_iam_policy_document" "ingestion_s3_document" { ## Set permissions for lambda to read from code bucket and to put inside ingestion bucket
  statement {

    actions = ["s3:PutObject"]

    resources = [
      "${aws_s3_bucket.ingestion-bucket.arn}/*"

    ]
  }
  statement {
    actions = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.code-bucket.arn}/*" ## permission to read code from code bucket
    ]
  }

  statement {

    # "Effect":= "Allow",
    actions   = ["secretsmanager:GetSecretValue"]
    resources = ["arn:aws:secretsmanager:eu-west-2:389125938424:secret:Totesys_DB_Credentials-4f8nsr"]
  }
}

data "aws_iam_policy_document" "ingestion_cw_document" {
  statement {
    actions = ["logs:CreateLogGroup"] #permission to create log group

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
  statement {

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"] # permission to create log stream and put logs in LogGroup

    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/ingestion_lambda:*"
    ]
  }
}

resource "aws_iam_policy" "ingestion_s3_policy" { ## Attach those permissions to a policy
  name   = "ingestion_s3_policy_lambda_role"
  policy = data.aws_iam_policy_document.ingestion_s3_document.json

}

resource "aws_iam_policy" "ingestion_cw_policy" { ## Attach cw permissions to a policy
  name   = "ingestion_cw_policy_role"
  policy = data.aws_iam_policy_document.ingestion_cw_document.json
}

resource "aws_iam_role_policy_attachment" "ingestion_lambda_s3_policy_attachment" { ## Attach that policy to our ingestion lambda role
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.ingestion_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingestion_lambda_cw_policy_attachment" { #Attach cw policy to our ingestion lambda role
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.ingestion_cw_policy.arn
}

resource "aws_iam_role" "transform_lambda_role" {
  name               = "transform_lambda_role"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "lambda.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

data "aws_iam_policy_document" "transform_s3_document" {
  statement {
    actions = ["s3:PutObject"]
    resources = [
      "${aws_s3_bucket.processed-bucket.arn}/*"

    ]
  }
  statement {
    actions = ["s3:GetObject"]
    resources = [
      "${aws_s3_bucket.code-bucket.arn}/*",
      "${aws_s3_bucket.ingestion-bucket.arn}/*"
    ]
  }
}

data "aws_iam_policy_document" "transform_cw_document" {
  statement {
    actions = ["logs:CreateLogGroup"]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"
    ]
  }
  statement {
    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = [
      "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/ingestion_lambda:*"
    ]
  }
}

resource "aws_iam_policy" "transform_s3_policy" {
  name   = "transform_s3_policy_lambda_role"
  policy = data.aws_iam_policy_document.transform_s3_document.json

}

resource "aws_iam_policy" "transform_cw_policy" {
  name   = "transform_cw_policy_role"
  policy = data.aws_iam_policy_document.transform_cw_document.json
}

resource "aws_iam_role_policy_attachment" "transform_lambda_s3_policy_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.transform_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "transform_lambda_cw_policy_attachment" {
  role       = aws_iam_role.transform_lambda_role.name
  policy_arn = aws_iam_policy.transform_cw_policy.arn
}

resource "aws_iam_role" "sfn_role" {
  name               = "sfn_role"
  assume_role_policy = <<EOF
    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "sts:AssumeRole"
                ],
                "Principal": {
                    "Service": [
                        "states.amazonaws.com"
                    ]
                }
            }
        ]
    }
    EOF
}

data "aws_iam_policy_document" "sfn_lambda_document" {
  statement {
    effect  = "Allow"
    actions = ["lambda:InvokeFunction"]
    resources = [
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:transform_lambda:*",
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:ingestion_lambda:*",
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:transform_lambda",
      "arn:aws:lambda:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:function:ingestion_lambda"
    ]
  }
}

resource "aws_iam_policy" "sfn_lambda_policy" {
  name   = "sfn_lambda_policy"
  policy = data.aws_iam_policy_document.sfn_lambda_document.json
}

resource "aws_iam_role_policy_attachment" "sfn_lambda_policy_attachment" {
  role       = aws_iam_role.sfn_role.name
  policy_arn = aws_iam_policy.sfn_lambda_policy.arn
}

resource "aws_lambda_permission" "allow_bucket" {
  count         = var.deploy_lambda_bool ? 1 : 0
  statement_id  = "AllowExecutionFromS3Bucket"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_lambda[0].arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.code-bucket.arn
}

resource "aws_s3_bucket_notification" "bucket_notification" {
  count  = var.deploy_lambda_bool ? 1 : 0
  bucket = aws_s3_bucket.code-bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.ingestion_lambda[0].arn
    events              = ["s3:ObjectCreated:*"]
    filter_prefix       = "AWSLogs/"
    filter_suffix       = ".log"
  }
  depends_on = [aws_lambda_permission.allow_bucket]
}