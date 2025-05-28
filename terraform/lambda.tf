resource "aws_lambda_function" "ingestion_lambda" {

    function_name = "ingestion_lambda"
#   s3_bucket = "bucket-one-ingestion" # linked to code bucket where lambda function setup
#   s3_key =  "ingestion_lambda/funtion.zip"
    role = aws_iam_role.lambda_role.arn
    # handler = ""
    runtime = "python3.9"

}

# data "archive_file" "lambda" {
#   type        = "zip"
#   source_file = "${path.module}/../src/file_reader/reader.py"
#   output_path = "${path.module}/../function.zip"
# }

