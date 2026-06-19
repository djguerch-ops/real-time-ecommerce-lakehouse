# ─────────────────────────────────────────────────────────────
# V2 — Real-time streaming infrastructure
# Kinesis Data Stream + Lambda consumer writing to S3
# ─────────────────────────────────────────────────────────────

# ---------------------------------------------------------------
# Kinesis Data Stream
# ---------------------------------------------------------------
resource "aws_kinesis_stream" "events" {
  name             = "${var.project_name}-events-stream"
  shard_count      = 1
  retention_period = 24 # hours — enough for a dev/demo project

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  tags = {
    Project     = "real-time-ecommerce-lakehouse"
    Environment = "dev"
    Layer       = "streaming"
  }
}

# ---------------------------------------------------------------
# IAM role assumed by the Lambda function
# ---------------------------------------------------------------
resource "aws_iam_role" "lambda_kinesis_consumer" {
  name = "${var.project_name}-lambda-kinesis-consumer"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
        Action = "sts:AssumeRole"
      }
    ]
  })

  tags = {
    Project     = "real-time-ecommerce-lakehouse"
    Environment = "dev"
  }
}

# Lets Lambda write its own execution logs to CloudWatch
resource "aws_iam_role_policy_attachment" "lambda_basic_execution" {
  role       = aws_iam_role.lambda_kinesis_consumer.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

# Scoped permissions: read from the Kinesis stream, write to the streaming
# prefix of the raw bucket only — nothing broader.
resource "aws_iam_role_policy" "lambda_kinesis_to_s3" {
  name = "${var.project_name}-lambda-kinesis-to-s3"
  role = aws_iam_role.lambda_kinesis_consumer.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "kinesis:GetRecords",
          "kinesis:GetShardIterator",
          "kinesis:DescribeStream",
          "kinesis:DescribeStreamSummary",
          "kinesis:ListShards",
          "kinesis:ListStreams"
        ]
        Resource = aws_kinesis_stream.events.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:PutObject"
        ]
        Resource = "${aws_s3_bucket.raw.arn}/streaming/*"
      }
    ]
  })
}

# ---------------------------------------------------------------
# Lambda function
# Code is packaged separately (see src/lambda/kinesis_to_s3/) and
# zipped at deploy time — the archive path is provided via variable.
# ---------------------------------------------------------------
resource "aws_lambda_function" "kinesis_to_s3" {
  function_name = "${var.project_name}-kinesis-to-s3"
  role          = aws_iam_role.lambda_kinesis_consumer.arn
  handler       = "handler.lambda_handler"
  runtime       = "python3.12"
  timeout       = 30
  memory_size   = 128

  filename         = var.lambda_zip_path
  source_code_hash = filebase64sha256(var.lambda_zip_path)

  environment {
    variables = {
      TARGET_BUCKET = var.raw_bucket_name
      TARGET_PREFIX = "streaming/events"
    }
  }

  tags = {
    Project     = "real-time-ecommerce-lakehouse"
    Environment = "dev"
    Layer       = "streaming"
  }
}

# ---------------------------------------------------------------
# Event source mapping: triggers the Lambda for every batch of
# Kinesis records (polling is managed by AWS, not by us).
# ---------------------------------------------------------------
resource "aws_lambda_event_source_mapping" "kinesis_trigger" {
  event_source_arn  = aws_kinesis_stream.events.arn
  function_name     = aws_lambda_function.kinesis_to_s3.arn
  starting_position = "LATEST"
  batch_size        = 100
  # Wait up to 5s to fill a batch before invoking, to avoid
  # invoking Lambda once per single event under low load.
  maximum_batching_window_in_seconds = 5
}
