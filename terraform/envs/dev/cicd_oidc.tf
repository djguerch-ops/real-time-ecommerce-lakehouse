# ─────────────────────────────────────────────────────────────
# CI/CD — GitHub Actions OIDC authentication to AWS
#
# Lets GitHub Actions assume an IAM role using short-lived,
# automatically-rotated tokens — no static AWS Access Key is
# ever stored in GitHub Secrets. AWS trusts GitHub's OIDC
# provider directly, scoped to this one repository.
# ─────────────────────────────────────────────────────────────

variable "github_org" {
  description = "GitHub organization or username that owns the repository"
  type        = string
  default     = "djguerch-ops"
}

variable "github_repo" {
  description = "Repository name GitHub Actions is allowed to assume this role from"
  type        = string
  default     = "real-time-ecommerce-lakehouse"
}

# ---------------------------------------------------------------
# OIDC Identity Provider for GitHub
# One per AWS account — if it already exists, this resource can
# be imported instead of recreated (see comment below).
# ---------------------------------------------------------------
resource "aws_iam_openid_connect_provider" "github_actions" {
  url = "https://token.actions.githubusercontent.com"

  client_id_list = [
    "sts.amazonaws.com"
  ]

  # GitHub's OIDC thumbprint — well-known, documented by GitHub.
  # AWS no longer strictly validates this value for GitHub's
  # provider but it's still required by the resource schema.
  thumbprint_list = [
    "6938fd4d98bab03faadb97b34396831e3780aea1"
  ]

  tags = {
    Project = "real-time-ecommerce-lakehouse"
    Purpose = "github-actions-ci-cd"
  }
}

# ---------------------------------------------------------------
# IAM role assumed by GitHub Actions workflows
#
# Trust policy restricts WHO can assume this role:
#   - only requests proven (via OIDC token) to originate from
#     GitHub's official token service
#   - only for this exact repository
#   - only for the `main` branch (no PR branches can apply infra)
# ---------------------------------------------------------------
resource "aws_iam_role" "github_actions_terraform" {
  name = "github-actions-terraform-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Principal = {
          Federated = aws_iam_openid_connect_provider.github_actions.arn
        }
        Action = "sts:AssumeRoleWithWebIdentity"
        Condition = {
          StringEquals = {
            "token.actions.githubusercontent.com:aud" = "sts.amazonaws.com"
          }
          StringLike = {
            "token.actions.githubusercontent.com:sub" = "repo:${var.github_org}/${var.github_repo}:ref:refs/heads/main"
          }
        }
      }
    ]
  })

  tags = {
    Project = "real-time-ecommerce-lakehouse"
    Purpose = "github-actions-ci-cd"
  }
}

# ---------------------------------------------------------------
# Read-only visibility, broadly, on the services this project
# uses. Terraform's "refresh" step reads many attributes per
# resource (tags, encryption, versioning, ACLs...) and enumerating
# every single read action one-by-one is impractical to maintain.
# AWS-managed ReadOnly policies cover this safely: they grant
# Describe/Get/List actions only — never Create/Update/Delete.
# ---------------------------------------------------------------
resource "aws_iam_role_policy_attachment" "github_actions_s3_read" {
  role       = aws_iam_role.github_actions_terraform.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
}

resource "aws_iam_role_policy_attachment" "github_actions_kinesis_read" {
  role       = aws_iam_role.github_actions_terraform.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonKinesisReadOnlyAccess"
}

resource "aws_iam_role_policy_attachment" "github_actions_lambda_read" {
  role       = aws_iam_role.github_actions_terraform.name
  policy_arn = "arn:aws:iam::aws:policy/AWSLambda_ReadOnlyAccess"
}

# ---------------------------------------------------------------
# Permissions granted to GitHub Actions.
#
# Write/manage actions only, scoped to this project's resources
# (S3, Kinesis, Lambda, IAM, OIDC) — not AdministratorAccess.
# Read visibility for these services comes from the AWS-managed
# ReadOnly policies attached above.
# ---------------------------------------------------------------
resource "aws_iam_role_policy" "github_actions_terraform_permissions" {
  name = "github-actions-terraform-permissions"
  role = aws_iam_role.github_actions_terraform.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3BucketWrite"
        Effect = "Allow"
        Action = [
          "s3:CreateBucket",
          "s3:PutBucket*",
          "s3:DeleteBucket*",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:PutEncryptionConfiguration",
          "s3:PutBucketVersioning",
          "s3:PutBucketPublicAccessBlock"
        ]
        Resource = [
          "arn:aws:s3:::rtl-dev-*",
          "arn:aws:s3:::rtl-dev-*/*"
        ]
      },
      {
        Sid    = "KinesisWrite"
        Effect = "Allow"
        Action = [
          "kinesis:CreateStream",
          "kinesis:DeleteStream",
          "kinesis:TagResource",
          "kinesis:AddTagsToStream",
          "kinesis:RemoveTagsFromStream"
        ]
        Resource = "arn:aws:kinesis:*:*:stream/rtl-dev-*"
      },
      {
        Sid    = "LambdaFunctionWrite"
        Effect = "Allow"
        Action = [
          "lambda:CreateFunction",
          "lambda:UpdateFunctionCode",
          "lambda:UpdateFunctionConfiguration",
          "lambda:DeleteFunction",
          "lambda:TagResource",
          "lambda:UntagResource"
        ]
        Resource = "arn:aws:lambda:*:*:function:rtl-dev-*"
      },
      {
        Sid    = "LambdaEventSourceMappingWrite"
        Effect = "Allow"
        Action = [
          "lambda:CreateEventSourceMapping",
          "lambda:DeleteEventSourceMapping",
          "lambda:UpdateEventSourceMapping"
        ]
        # Event source mappings are a distinct resource type from
        # Lambda functions, with their own ARN format — they can't
        # be scoped by function name, only by account/region.
        Resource = "arn:aws:lambda:*:*:event-source-mapping:*"
      },
      {
        Sid    = "IAMForProjectRoles"
        Effect = "Allow"
        Action = [
          "iam:GetRole",
          "iam:CreateRole",
          "iam:DeleteRole",
          "iam:PutRolePolicy",
          "iam:GetRolePolicy",
          "iam:DeleteRolePolicy",
          "iam:AttachRolePolicy",
          "iam:DetachRolePolicy",
          "iam:ListRolePolicies",
          "iam:ListAttachedRolePolicies",
          "iam:ListInstanceProfilesForRole",
          "iam:TagRole",
          "iam:ListRoleTags",
          "iam:PassRole"
        ]
        # Scoped to roles this project creates — NOT all IAM roles
        # in the account. GitHub Actions cannot touch IAM outside
        # this project's naming convention.
        Resource = [
          "arn:aws:iam::*:role/databricks-s3-role",
          "arn:aws:iam::*:role/rtl-dev-*",
          "arn:aws:iam::*:role/github-actions-terraform-role"
        ]
      },
      {
        Sid    = "OidcProviderManagement"
        Effect = "Allow"
        Action = [
          "iam:GetOpenIDConnectProvider",
          "iam:CreateOpenIDConnectProvider",
          "iam:DeleteOpenIDConnectProvider",
          "iam:UpdateOpenIDConnectProviderThumbprint",
          "iam:TagOpenIDConnectProvider",
          "iam:ListOpenIDConnectProviderTags"
        ]
        Resource = "arn:aws:iam::*:oidc-provider/token.actions.githubusercontent.com"
      }
    ]
  })
}

output "github_actions_role_arn" {
  description = "ARN to reference in the GitHub Actions workflow's role-to-assume input"
  value       = aws_iam_role.github_actions_terraform.arn
}
