# declaring and giving name to event rule
resource "aws_cloudwatch_event_rule" "ingestion_scheduler" {
  name                = "ingestion_scheduler"
  schedule_expression = "rate(1 minute)" # minute becomes minutes if more than 1 else it won't work

}

#setting the target ingestion_lambda funtion
resource "aws_cloudwatch_event_target" "ingestion_lambda" {
  count     = var.deploy_lambda_bool ? 1 : 0
  rule      = aws_cloudwatch_event_rule.ingestion_scheduler.name
  target_id = "ingestion_lambda"
  arn       = aws_lambda_function.ingestion_lambda[0].arn
}

# allocating permission to scheduler/eventbridge to invoke ingestion_lambda function

resource "aws_lambda_permission" "allow_eventbridge" {
  count         = var.deploy_lambda_bool ? 1 : 0
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_lambda[0].function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.ingestion_scheduler.arn

}

### ~~~ EMAIL ALERT SETUP ~~~
resource "aws_sns_topic" "ingestion_lambda_alert_topic" { #AWS service that allows sending messages to 'subscribers'
  name = "ingestion-failure-alerts"
}

resource "aws_sns_topic_subscription" "email_alert" { #specify the 'subscriber' endpoint for the alarm
  topic_arn = aws_sns_topic.ingestion_lambda_alert_topic.arn
  protocol  = "email"
  endpoint  = "taimoor.deds@gmail.com"
}

resource "aws_cloudwatch_metric_alarm" "lambda_error_alarm" {
  count               = var.deploy_lambda_bool ? 1 : 0
  alarm_name          = "LambdaFailureAlarm"
  comparison_operator = "GreaterThanThreshold" #triggers alarm on anything greater than specified threshold
  evaluation_periods  = 1                      #i.e. with a period of 60 (1 minute), how many minutes are we looking at to trigger an alarm
  metric_name         = "Errors"               #lambda metric keyword
  namespace           = "AWS/Lambda"           #namespace for metric
  period              = 60                     # cloudwatch collects and evaluates metric data in 60 seconds intervals here
  statistic           = "Sum"                  #sums errors over specified period
  threshold           = 0                      #the threshold of errors at which an email alert would be sent
  alarm_description   = "Alarm for lambda errors"
  alarm_actions       = [aws_sns_topic.ingestion_lambda_alert_topic.arn]
  dimensions = {
    FunctionName = aws_lambda_function.ingestion_lambda[0].function_name
  }
  depends_on = [aws_lambda_function.ingestion_lambda]


}

resource "aws_sns_topic" "transform_lambda_alert_topic" { #AWS service that allows sending messages to 'subscribers'
  name = "transform-failure-alerts"
}

resource "aws_sns_topic_subscription" "transform_email_alert" { #specify the 'subscriber' endpoint for the alarm
  topic_arn = aws_sns_topic.transform_lambda_alert_topic.arn
  protocol  = "email"
  endpoint  = "taimoor.deds@gmail.com"
}

resource "aws_cloudwatch_metric_alarm" "transform_error_alarm" {
  count               = var.deploy_lambda_bool ? 1 : 0
  alarm_name          = "TransformFailureAlarm"
  comparison_operator = "GreaterThanThreshold" #triggers alarm on anything greater than specified threshold
  evaluation_periods  = 1                      #i.e. with a period of 60 (1 minute), how many minutes are we looking at to trigger an alarm
  metric_name         = "Errors"               #lambda metric keyword
  namespace           = "AWS/Lambda"           #namespace for metric
  period              = 60                     # cloudwatch collects and evaluates metric data in 60 seconds intervals here
  statistic           = "Sum"                  #sums errors over specified period
  threshold           = 0                      #the threshold of errors at which an email alert would be sent
  alarm_description   = "Alarm for transform lambda errors"
  alarm_actions       = [aws_sns_topic.transform_lambda_alert_topic.arn]
  dimensions = { #specify which lambda is tracked
    FunctionName = "transform_lambda"
  }
}

resource "aws_sns_topic" "load_lambda_alert_topic" { #AWS service that allows sending messages to 'subscribers'
  name = "load-failure-alerts"
}

resource "aws_sns_topic_subscription" "load_email_alert" { #specify the 'subscriber' endpoint for the alarm
  topic_arn = aws_sns_topic.load_lambda_alert_topic.arn
  protocol  = "email"
  endpoint  = "taimoor.deds@gmail.com"
}

resource "aws_cloudwatch_metric_alarm" "load_error_alarm" {
  count               = var.deploy_lambda_bool ? 1 : 0
  alarm_name          = "LoadFailureAlarm"
  comparison_operator = "GreaterThanThreshold" #triggers alarm on anything greater than specified threshold
  evaluation_periods  = 1                      #i.e. with a period of 60 (1 minute), how many minutes are we looking at to trigger an alarm
  metric_name         = "Errors"               #lambda metric keyword
  namespace           = "AWS/Lambda"           #namespace for metric
  period              = 60                     # cloudwatch collects and evaluates metric data in 60 seconds intervals here
  statistic           = "Sum"                  #sums errors over specified period
  threshold           = 0                      #the threshold of errors at which an email alert would be sent
  alarm_description   = "Alarm for load lambda errors"
  alarm_actions       = [aws_sns_topic.load_lambda_alert_topic.arn]
  dimensions = { #specify which lambda is tracked
    FunctionName = "load_lambda"
  }
}