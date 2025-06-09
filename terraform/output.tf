output "lambda_function_name" {
  description = "The deployed Lambda function name"
  value       = length(aws_lambda_function.ingestion_lambda) > 0 ? aws_lambda_function.ingestion_lambda[0].function_name : ""
}

output "transform_lambda_function_name" {
  description = "The deployed transform Lambda function name"
  value       = length(aws_lambda_function.transform_lambda) > 0 ? aws_lambda_function.transform_lambda[0].function_name : ""
}



output "code_bucket_name" {
value = aws_s3_bucket.code-bucket.bucket
}