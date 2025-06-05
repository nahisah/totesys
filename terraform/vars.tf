variable "region_name" {
  default = "eu-west-2"

}


variable "deploy_lambda_bool" {
  default = false
  description = "whether or not to deploy ingestion lambda function"
  }

  variable "deploy_transform_lambda_bool" {
  default = false
  description = "whether or not to deploy transform lambda function"
  }