terraform {
  required_providers {
    google = { source = "hashicorp/google", version = "6.0.0" }
  }
}
provider "google" { project = "colors-example" }
locals {
  buckets = { data = "automq-gcs-fixture-data-colors-example-us-central1", ops = "automq-gcs-fixture-ops-colors-example-us-central1" }
}
resource "google_storage_bucket" "application" {
  for_each                    = local.buckets
  name                        = each.value
  location                    = "us-central1"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = true
  labels                      = { colors_profile = "automq-gcs-fixture", colors_owner = "automq-storage" }
  soft_delete_policy { retention_duration_seconds = 0 }
  lifecycle { prevent_destroy = true }
}
resource "google_service_account" "application" {
  account_id   = substr("automq-gcs-fixture-storage", 0, 30)
  display_name = "AutoMQ automq-gcs-fixture storage"
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
