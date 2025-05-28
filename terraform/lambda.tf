# resource "aws_lambda_function" "ingestion_lambda" {

#     function_name = "ingestion_lambda"
#     s3_bucket = "code-bucket20250528124923274800000001"
#     s3_key =  "ingestion_lambda/function.zip"
   
#     role = aws_iam_role.ingestion_lambda_role.arn
#     handler = "lambda_handler" 
#     runtime = "python3.9"

# }

# data "archive_file" "lambda" {
#   type        = "zip"
#   source_file = "${path.module}/../src/file_reader/reader.py"
#   output_path = "${path.module}/../function.zip"
# }

## TO REIMPLEMENT WHEN LAMBDA FUNCTION IS AVAILABLE, PERMISSIONS ALREADY SET UP FOR INGESTION LAMBDA

