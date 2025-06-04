output "lambda_function_name" {
  description = "The deployed Lambda function name"
  value       = length(aws_lambda_function.ingestion_lambda) > 0 ? aws_lambda_function.ingestion_lambda[0].function_name : ""
}


output "code_bucket_name" {
value = aws_s3_bucket.data_bucket.id
}