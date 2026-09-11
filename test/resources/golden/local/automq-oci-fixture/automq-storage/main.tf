terraform {
  required_providers {
    oci = { source = "oracle/oci", version = "7.32.0" }
    tls = { source = "hashicorp/tls", version = "4.1.0" }
  }
}
provider "oci" {
  config_file_profile = "DEFAULT"
  region              = "eu-frankfurt-1"
  auth                = "SecurityToken"
}
provider "oci" {
  alias               = "home"
  config_file_profile = "DEFAULT"
  region              = "eu-frankfurt-1"
  auth                = "SecurityToken"
}
locals {
  buckets = { data = "automq-oci-fixture-data-example", ops = "automq-oci-fixture-ops-example" }
  policy_scope = "ocid1.tenancy.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" == "ocid1.tenancy.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ? "tenancy" : "compartment id ocid1.tenancy.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  tags = { "colors-profile" = "automq-oci-fixture", "colors-owner" = "automq-storage" }
}
resource "oci_objectstorage_bucket" "application" {
  for_each       = local.buckets
  compartment_id = "ocid1.tenancy.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  namespace      = "example"
  name           = each.value
  access_type    = "NoPublicAccess"
  storage_tier   = "Standard"
  versioning     = "Disabled"
  freeform_tags  = local.tags
  lifecycle { prevent_destroy = true }
}
resource "oci_identity_user" "application" {
  provider       = oci.home
  compartment_id = "ocid1.tenancy.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  name           = "automq-oci-fixture-automq-storage"
  description    = "AutoMQ application bucket access"
  email          = "automq-oci-storage@example.com"
  freeform_tags  = local.tags
}
resource "oci_identity_group" "application" {
  provider       = oci.home
  compartment_id = "ocid1.tenancy.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  name           = "automq-oci-fixture-automq-storage"
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
  compartment_id = "ocid1.tenancy.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
  name           = "automq-oci-fixture-automq-storage"
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
  value     = "ocid1.tenancy.oc1..aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/${oci_identity_user.application.id}/${oci_identity_api_key.application.fingerprint}"
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
