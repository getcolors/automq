terraform {
  required_providers {
    aws = { source = "hashicorp/aws", version = "6.31.0" }
  }
}
provider "aws" { region = "<{ automq-r2-region }>" }
locals {
  buckets = { data = "<{ automq-data-r2-bucket }>", ops = "<{ automq-ops-r2-bucket }>" }
  tags    = { "colors:profile" = "<{ profile }>", "colors:owner" = "automq-storage" }
}
resource "aws_s3_bucket" "application" {
  for_each      = local.buckets
  bucket        = each.value
  force_destroy = true
  lifecycle { prevent_destroy = <{ compute-prevent-destroy }> }
  tags          = local.tags
}
resource "aws_s3_bucket_public_access_block" "application" {
  for_each                = aws_s3_bucket.application
  bucket                  = each.value.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
resource "aws_s3_bucket_server_side_encryption_configuration" "application" {
  for_each = aws_s3_bucket.application
  bucket   = each.value.id
  # S3 now creates buckets with SSE-C blocked and the bucket key off; declare
  # both, or the provider plans to remove and re-add them on every converge.
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
    blocked_encryption_types = ["SSE-C"]
    bucket_key_enabled = false
  }
}
resource "aws_iam_user" "application" {
  name = "<{ profile }>-automq-storage"
  tags = local.tags
}
resource "aws_iam_user_policy" "application" {
  name = "automq-buckets"
  user = aws_iam_user.application.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      { Effect = "Allow", Action = ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"], Resource = [for bucket in aws_s3_bucket.application : bucket.arn] },
      { Effect = "Allow", Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"], Resource = [for bucket in aws_s3_bucket.application : "${bucket.arn}/*"] }
    ]
  })
}
resource "aws_iam_access_key" "application" {
  user       = aws_iam_user.application.name
  depends_on = [aws_iam_user_policy.application]
}
output "access_key_id" {
  value     = aws_iam_access_key.application.id
  sensitive = true
}
output "secret_access_key" {
  value     = aws_iam_access_key.application.secret
  sensitive = true
}
