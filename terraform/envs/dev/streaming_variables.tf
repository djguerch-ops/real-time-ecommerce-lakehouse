# ─────────────────────────────────────────────────────────────
# V2 — Variables for the streaming infrastructure
# ─────────────────────────────────────────────────────────────

variable "project_name" {
  description = "Short project name used as a prefix for V2 streaming resources (Kinesis, Lambda, IAM)"
  type        = string
  default     = "rtl-dev"
}

variable "lambda_zip_path" {
  description = "Path to the zipped Lambda deployment package (built from src/lambda/kinesis_to_s3/)"
  type        = string
  default     = "../../../src/lambda/kinesis_to_s3/build/lambda.zip"
}
