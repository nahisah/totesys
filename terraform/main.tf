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
  region = var.region_name
  default_tags {
    tags = {

      ProjectName = "S3 totesys project"
      Team = "Data Engineering"
      DeployedFrom = "Terraform"
      Repository = "totesys"
      Environment = "dev"
    
    }
  }
}