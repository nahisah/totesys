resource "aws_cloudwatch_event_rule" "ingestion_scheduler" {
  name                = "ingestion_scheduler"
  schedule_expression = "rate(30 minutes)"

}

resource "aws_cloudwatch_event_target" "ingestion_lambda" {
  rule      = aws_cloudwatch_event_rule.ingestion_scheduler.name
  target_id = "ingestion_lambda"
  arn       = aws_lambda_function.ingestion_lambda.arn
}

#allocating permission to scheduler/eventbridge to invoke ingestion_lambda function

# resource "aws_lambda_permission" "allow_eventbridge" {
#     statement_id = "AllowExecutionFromEventBridge"
#     action = "lambda:InvokeFunction"

# }
