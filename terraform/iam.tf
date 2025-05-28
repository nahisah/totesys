resource "aws_iam_role" "lambda_role" {
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
  
data "aws_iam_policy_document" "s3_document" {
  statement {

    actions = ["s3:GetObject"]

    resources = [
      "${aws_s3_bucket.ingestion-bucket.arn}/*",
      "${aws_s3_bucket.processed-bucket.arn}/*",
    ]
  }
}

resource "aws_iam_policy" "s3_policy" {
    name = "s3_policy_lambda_role"
    policy = data.aws_iam_policy_document.s3_document.json
  
}

resource "aws_iam_role_policy_attachment" "lambda_s3_policy_attachment" {
    role = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.s3_policy.arn
}