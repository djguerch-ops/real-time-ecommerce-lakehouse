variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "raw_bucket_name" {
  description = "S3 bucket for raw events"
  type        = string
}

variable "checkpoints_bucket_name" {
  description = "S3 bucket for Databricks checkpoints"
  type        = string
}
