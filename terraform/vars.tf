variable "region_name" {
  default = "eu-west-2"

}


variable "deploy_lambda_bool" {
  default = false
  description = "whether or not to deploy lambda function"
  }