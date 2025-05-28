resource "aws_iam_role" "ingestion_lambda_role" { ##Blank role for ingestion lambda
    name = "lambda_role"
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
}

resource "aws_iam_policy" "ingestion_s3_policy" { ## Attach those permissions to a policy
    name = "ingestion_s3_policy_lambda_role"
    policy = data.aws_iam_policy_document.ingestion_s3_document.json
  
}

resource "aws_iam_role_policy_attachment" "ingestion_lambda_s3_policy_attachment" { ## Attach that policy to our ingestion lambda role
    role = aws_iam_role.ingestion_lambda_role.name
    policy_arn = aws_iam_policy.ingestion_s3_policy.arn
}