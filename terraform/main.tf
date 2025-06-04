module "configuration" {
  source = "./configuration"
  code-bucket = module.code-bucket.code-bucket
}

module "code-bucket" {
  source = "./code-bucket"
}