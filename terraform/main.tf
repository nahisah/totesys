terraform {
  required_providers {
    aws={
        source = "hashicorp/aws"
        version = "~> 5.0"
    }
    
  }

  backend "s3" {
    bucket = "terraform-state-totsys-backend"
    key = "project/terraform.tfstate"
    region = "eu-west-2"
    
  }
  
}
provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {

      ProjectName = "S3 totsys project"
      Team = "Data Engineering"
      DeployedFrom = "Terraform"
      Repository = "totsys"
      Environment = "dev"
    
    }
  }
}