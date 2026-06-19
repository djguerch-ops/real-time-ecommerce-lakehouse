terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

resource "aws_s3_bucket" "raw" {
  bucket = var.raw_bucket_name

  tags = {
    Project     = "real-time-ecommerce-lakehouse"
    Environment = "dev"
    Layer       = "raw"
  }
}

resource "aws_s3_bucket" "checkpoints" {
  bucket = var.checkpoints_bucket_name

  tags = {
    Project     = "real-time-ecommerce-lakehouse"
    Environment = "dev"
    Layer       = "checkpoints"
  }
}

resource "aws_s3_bucket_versioning" "raw_versioning" {
  bucket = aws_s3_bucket.raw.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_versioning" "checkpoints_versioning" {
  bucket = aws_s3_bucket.checkpoints.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw_encryption" {
  bucket = aws_s3_bucket.raw.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "checkpoints_encryption" {
  bucket = aws_s3_bucket.checkpoints.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "raw_pab" {
  bucket = aws_s3_bucket.raw.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_public_access_block" "checkpoints_pab" {
  bucket = aws_s3_bucket.checkpoints.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
