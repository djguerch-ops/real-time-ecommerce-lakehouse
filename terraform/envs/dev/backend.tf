# ─────────────────────────────────────────────────────────────
# Remote state backend
#
# Stores terraform.tfstate in S3 instead of locally, so both
# the developer's machine and GitHub Actions read/write the
# same state — critical for CI/CD to know what already exists.
# ─────────────────────────────────────────────────────────────

terraform {
  backend "s3" {
    bucket = "rtl-dev-terraform-state-563683519302"
    key    = "envs/dev/terraform.tfstate"
    region = "eu-west-1"
  }
}
