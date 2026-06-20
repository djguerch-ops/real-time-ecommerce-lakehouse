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
# Permissions granted to GitHub Actions.
#
# Scoped to exactly what Terraform needs to manage this project's
# resources: S3, Kinesis, Lambda, and the IAM resources Terraform
# itself creates/updates. Not AdministratorAccess.
# ---------------------------------------------------------------
resource "aws_iam_role_policy" "github_actions_terraform_permissions" {
  name = "github-actions-terraform-permissions"
  role = aws_iam_role.github_actions_terraform.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3BucketManagement"
        Effect = "Allow"
        Action = [
          "s3:CreateBucket",
          "s3:GetBucket*",
          "s3:PutBucket*",
          "s3:ListBucket",
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          # Read-only checks Terraform performs on every refresh,
          # even when the bucket config itself isn't being changed.
          "s3:GetAccelerateConfiguration",
          "s3:GetBucketCORS",
          "s3:GetBucketWebsite",
          "s3:GetBucketLogging",
          "s3:GetBucketObjectLockConfiguration",
          "s3:GetBucketPolicy",
          "s3:GetReplicationConfiguration",
          "s3:GetLifecycleConfiguration",
          "s3:GetEncryptionConfiguration",
          "s3:GetBucketVersioning",
          "s3:GetBucketPublicAccessBlock",
          "s3:GetBucketTagging"
        ]
        Resource = [
          "arn:aws:s3:::rtl-dev-*",
          "arn:aws:s3:::rtl-dev-*/*"
        ]
      },
      {
        Sid    = "KinesisManagement"
        Effect = "Allow"
        Action = [
          "kinesis:CreateStream",
          "kinesis:DeleteStream",
          "kinesis:DescribeStream*",
          "kinesis:ListStreams",
          "kinesis:TagResource",
          "kinesis:AddTagsToStream",
          "kinesis:ListTagsForStream"
        ]
        Resource = "arn:aws:kinesis:*:*:stream/rtl-dev-*"
      },
      {
        Sid    = "LambdaManagement"
        Effect = "Allow"
        Action = [
          "lambda:CreateFunction",
          "lambda:UpdateFunctionCode",
          "lambda:UpdateFunctionConfiguration",
          "lambda:GetFunction",
          "lambda:GetFunctionCodeSigningConfig",
          "lambda:DeleteFunction",
          "lambda:TagResource",
          "lambda:ListTags",
          "lambda:ListVersionsByFunction",
          "lambda:CreateEventSourceMapping",
          "lambda:DeleteEventSourceMapping",
          "lambda:GetEventSourceMapping",
          "lambda:UpdateEventSourceMapping"
        ]
        Resource = "arn:aws:lambda:*:*:function:rtl-dev-*"
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
      },
      {
        Sid    = "GithubActionsRoleSelfManagement"
        Effect = "Allow"
        Action = [
          "iam:PutRolePolicy",
          "iam:GetRolePolicy",
          "iam:DeleteRolePolicy",
          "iam:ListRolePolicies"
        ]
        # Lets this same role update its own inline policy on a
        # future terraform apply (e.g. adding more permissions
        # later) — without this, the role could lock itself out
        # of ever changing its own policy again.
        Resource = "arn:aws:iam::*:role/github-actions-terraform-role"
      },
      {
        Sid    = "TerraformStateRead"
        Effect = "Allow"
        Action = [
          "s3:GetBucketLocation"
        ]
        Resource = "*"
      }
    ]
  })
}

output "github_actions_role_arn" {
  description = "ARN to reference in the GitHub Actions workflow's role-to-assume input"
  value       = aws_iam_role.github_actions_terraform.arn
}
