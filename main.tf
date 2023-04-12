terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 4.0"
    }
  }
}

# Configure the AWS Provider
provider "aws" {
  #region = "us-east-1"
}

resource "aws_qldb_ledger" "transactions-ledger" {
  name                = "transactions-ledger"
  permissions_mode    = "STANDARD"
  deletion_protection = false
}

