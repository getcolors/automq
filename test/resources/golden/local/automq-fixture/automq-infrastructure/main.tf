terraform {
  required_providers {
    vultr = { source = "vultr/vultr", version = "~> 2.0" }
  }
}

provider "vultr" {
  # api key comes from VULTR_API_KEY in the environment
}

locals {
  ssh_sources   = ["0.0.0.0/0"]
  kafka_sources = ["0.0.0.0/0"]
  node_count    = 3
  vpc_block     = split("/", "10.40.0.0/24")[0]
  vpc_prefix    = tonumber(split("/", "10.40.0.0/24")[1])
}

# The machine keypair this deployment generated and owns (SSH Keypair
# Standard): the account resource is named after the profile and lives in this
# stack's state, which is what makes its ownership decidable. One key for every
# node — the cluster is one deployment, and a key per machine would multiply
# the thing the standard exists to make singular. Never reference a literal key
# id here in keygen mode.
resource "vultr_ssh_key" "machine" {
  name    = "automq-fixture"
  # fileexists: a delete after a completed delete renders this stack with the
  # key files already gone (the keypair cleanup is the last step) and tofu
  # evaluates file() even while destroying an empty state. A real create has
  # generated the file in preflight before this renders, so the empty branch
  # is never applied.
  ssh_key = fileexists("/home/build-placeholder/.ssh/automq-fixture.pub") ? trimspace(file("/home/build-placeholder/.ssh/automq-fixture.pub")) : ""
}

# The private network carrying the KRaft quorum and inter-broker replication.
# Nothing on those ports is ever published: the firewall below opens 22 and the
# Kafka port only, and the brokers bind 9093/9094 to the address handed out
# here. A VPC rather than the public interface is what makes that possible.
#
# `vultr_vpc`, not `vultr_vpc2`. Vultr has retired the VPC 2.0 API — the
# account returns 404 for /v2/vpc2 and 200 for /v2/vpcs — while the terraform
# provider still ships the `vultr_vpc2` resource and its documentation. The
# provider source is therefore not evidence that an endpoint exists; only the
# live API is.
resource "vultr_vpc" "cluster" {
  region         = "ams"
  description    = "automq-fixture"
  v4_subnet      = local.vpc_block
  v4_subnet_mask = local.vpc_prefix
}

# Every label derives from one resolved name (Compute Name Standard §3), which
# is the profile unless desired state overrides it with vultr-name.
resource "vultr_firewall_group" "cluster" {
  description = "automq-fixture-firewall"
}

# 22 carries convergence and recovery.
resource "vultr_firewall_rule" "ssh" {
  for_each          = toset(local.ssh_sources)
  firewall_group_id = vultr_firewall_group.cluster.id
  protocol          = "tcp"
  port              = "22"
  ip_type           = strcontains(each.value, ":") ? "v6" : "v4"
  subnet            = split("/", each.value)[0]
  subnet_size       = tonumber(split("/", each.value)[1])
}

# The Kafka port, and the only other thing reachable from outside. It is
# deliberately public: this listener is SASL_SSL with SCRAM and an ACL
# authorizer, so authentication gates it rather than the firewall. Narrow
# vultr-kafka-sources when the cluster holds data worth narrowing it for.
resource "vultr_firewall_rule" "kafka" {
  for_each          = toset(local.kafka_sources)
  firewall_group_id = vultr_firewall_group.cluster.id
  protocol          = "tcp"
  port              = "9092"
  ip_type           = strcontains(each.value, ":") ? "v6" : "v4"
  subnet            = split("/", each.value)[0]
  subnet_size       = tonumber(split("/", each.value)[1])
}

# The quorum's own ports, reachable only from inside the VPC.
#
# This rule is not belt-and-braces, it is load-bearing, and the way it fails
# without it is the worst kind: a Vultr firewall group filters the PRIVATE
# interface as well as the public one, and it does so selectively. ICMP passes,
# so every node pings every other node and the network looks healthy. TCP does
# not, so the controllers never exchange a vote, every node stays a candidate
# through epoch after epoch, and the only visible symptom is the broker half
# dying sixty seconds later with "Received a fatal error while waiting for the
# controller to acknowledge that we are caught up" — a message about the
# broker, pointing nowhere near the firewall.
resource "vultr_firewall_rule" "cluster_internal" {
  for_each          = toset(["9093", "9094"])
  firewall_group_id = vultr_firewall_group.cluster.id
  protocol          = "tcp"
  port              = each.value
  ip_type           = "v4"
  subnet            = local.vpc_block
  subnet_size       = local.vpc_prefix
}

resource "vultr_instance" "node" {
  count = local.node_count

  # `label` is the console name and updates in place. There is deliberately no
  # `hostname`: Vultr implements a hostname change as an OS reinstall, so the
  # provider marks that attribute ForceNew, and editing the name would destroy
  # the instance and its disk rather than rename it.
  label             = "automq-fixture-${count.index}"
  region            = "ams"
  plan              = "vc2-4c-8gb"
  os_id             = 2284
  firewall_group_id = vultr_firewall_group.cluster.id
  vpc_ids           = [vultr_vpc.cluster.id]
  # SSH keys are ids already in the account, and ForceNew: changing the key set
  # destroys and recreates the instance instead of re-authorizing it. Rotation
  # is a rebuild, never an edit on a machine whose disk you intend to keep.
  ssh_key_ids = [vultr_ssh_key.machine.id]
  # Wait for ssh before starting Ansible.
  connection {
    type = "ssh"
    user = "root"
    host = self.main_ip
    private_key = file("/home/build-placeholder/.ssh/automq-fixture")
  }
  provisioner "remote-exec" {
    inline = ["ls"]
  }
  lifecycle { prevent_destroy = true }
}

# The SSH Keypair Standard's contract: ownership is the resource id recorded
# in state and surfaced as `params.ssh_key_id`. That is why `params` is an
# object rather than the bare node list it would otherwise be — a list has
# nowhere to put the key id, and ONCE's create preflight would then report a
# key this deployment created as foreign.
#
# `index` is the KRaft node.id, the machine label's suffix, and the broker
# name's ordinal — one number, so the three cannot drift apart.
output "params" {
  value = {
    provider = "vultr"
    ssh_key_id = vultr_ssh_key.machine.id
    nodes = [
      for i, node in vultr_instance.node : {
        index  = i
        ip     = node.main_ip
        vpc_ip = node.internal_ip
        user   = "root"
        sudoer = "root"
        name   = node.label
      }
    ]
  }
}
