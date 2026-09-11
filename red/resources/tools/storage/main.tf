<% if automq-storage-oci %>terraform {
  required_providers {
    oci = { source = "oracle/oci", version = "7.32.0" }
    tls = { source = "hashicorp/tls", version = "4.1.0" }
  }
}
provider "oci" {
  config_file_profile = "<{ oci-config-file-profile }>"
  region              = "<{ automq-r2-region }>"
  auth                = "<{ oci-auth }>"
}
provider "oci" {
  alias               = "home"
  config_file_profile = "<{ oci-config-file-profile }>"
  region              = "<{ oci-home-region }>"
  auth                = "<{ oci-auth }>"
}
locals {
  buckets = { data = "<{ automq-data-r2-bucket }>", ops = "<{ automq-ops-r2-bucket }>" }
  policy_scope = "<{ oci-compartment-id }>" == "<{ oci-tenancy-id }>" ? "tenancy" : "compartment id <{ oci-compartment-id }>"
  tags = { "colors-profile" = "<{ profile }>", "colors-owner" = "automq-storage" }
}
resource "oci_objectstorage_bucket" "application" {
  for_each       = local.buckets
  compartment_id = "<{ oci-compartment-id }>"
  namespace      = "<{ oci-namespace }>"
  name           = each.value
  access_type    = "NoPublicAccess"
  storage_tier   = "Standard"
  versioning     = "Disabled"
  freeform_tags  = local.tags
  lifecycle { prevent_destroy = <{ compute-prevent-destroy }> }
}
resource "oci_identity_user" "application" {
  provider       = oci.home
  compartment_id = "<{ oci-tenancy-id }>"
  name           = "<{ profile }>-automq-storage"
  description    = "AutoMQ application bucket access"
  email          = "<{ automq-oci-user-email }>"
  freeform_tags  = local.tags
}
resource "oci_identity_group" "application" {
  provider       = oci.home
  compartment_id = "<{ oci-tenancy-id }>"
  name           = "<{ profile }>-automq-storage"
  description    = "AutoMQ application bucket access"
  freeform_tags  = local.tags
}
resource "oci_identity_user_group_membership" "application" {
  provider = oci.home
  user_id  = oci_identity_user.application.id
  group_id = oci_identity_group.application.id
}
resource "oci_identity_policy" "application" {
  provider       = oci.home
  compartment_id = "<{ oci-compartment-id }>"
  name           = "<{ profile }>-automq-storage"
  description    = "Access only the AutoMQ data and ops buckets"
  freeform_tags  = local.tags
  statements = flatten([for bucket in oci_objectstorage_bucket.application : [
    "Allow group id ${oci_identity_group.application.id} to read buckets in ${local.policy_scope} where target.bucket.name = '${bucket.name}'",
    "Allow group id ${oci_identity_group.application.id} to manage objects in ${local.policy_scope} where target.bucket.name = '${bucket.name}'"
  ]])
}
resource "oci_identity_customer_secret_key" "application" {
  provider     = oci.home
  display_name = "AutoMQ bucket access"
  user_id      = oci_identity_user.application.id
  depends_on   = [oci_identity_policy.application, oci_identity_user_group_membership.application]
}
resource "tls_private_key" "application" {
  algorithm = "RSA"
  rsa_bits  = 2048
}
resource "oci_identity_api_key" "application" {
  provider  = oci.home
  user_id   = oci_identity_user.application.id
  key_value = tls_private_key.application.public_key_pem
}
output "oci_signing_key_b64" {
  value     = base64encode(tls_private_key.application.private_key_pem)
  sensitive = true
}
output "oci_signing_key_id" {
  value     = "<{ oci-tenancy-id }>/${oci_identity_user.application.id}/${oci_identity_api_key.application.fingerprint}"
  sensitive = true
}
output "access_key_id" {
  value     = oci_identity_customer_secret_key.application.id
  sensitive = true
}
output "secret_access_key" {
  value     = oci_identity_customer_secret_key.application.key
  sensitive = true
}
<% else %><% if automq-storage-gcs %>terraform {
  required_providers {
    google = { source = "hashicorp/google", version = "6.0.0" }
  }
}
provider "google" { project = "<{ google-project }>" }
locals {
  buckets = { data = "<{ automq-data-r2-bucket }>", ops = "<{ automq-ops-r2-bucket }>" }
}
resource "google_storage_bucket" "application" {
  for_each                    = local.buckets
  name                        = each.value
  location                    = "<{ automq-r2-region }>"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true
  labels                      = { colors_profile = "<{ profile }>", colors_owner = "automq-storage" }
  soft_delete_policy { retention_duration_seconds = 0 }
  lifecycle { prevent_destroy = <{ compute-prevent-destroy }> }
}
resource "google_service_account" "application" {
  account_id   = substr("<{ profile }>-storage", 0, 30)
  display_name = "AutoMQ <{ profile }> storage"
}
resource "google_storage_bucket_iam_member" "application" {
  for_each = google_storage_bucket.application
  bucket   = each.value.name
  role     = "roles/storage.objectAdmin"
  member   = "serviceAccount:${google_service_account.application.email}"
}
resource "google_storage_bucket_iam_member" "bucket_metadata" {
  for_each = google_storage_bucket.application
  bucket   = each.value.name
  role     = "roles/storage.legacyBucketReader"
  member   = "serviceAccount:${google_service_account.application.email}"
}
resource "google_storage_hmac_key" "application" {
  service_account_email = google_service_account.application.email
  depends_on            = [google_storage_bucket_iam_member.application, google_storage_bucket_iam_member.bucket_metadata]
}
output "access_key_id" {
  value     = google_storage_hmac_key.application.access_id
  sensitive = true
}
output "secret_access_key" {
  value     = google_storage_hmac_key.application.secret
  sensitive = true
}
<% else %>terraform {
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
<% endif %><% endif %>