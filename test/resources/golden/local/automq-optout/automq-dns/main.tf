terraform {
  required_providers {
    cloudflare = { source = "cloudflare/cloudflare", version = "~> 5.0" }
  }
}
provider "cloudflare" {}
data "cloudflare_zone" "zone" {
  filter = { name = "example.com" }
}
