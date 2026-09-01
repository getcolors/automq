terraform {
  required_providers {
    vultr = { source = "vultr/vultr", version = "~> 2.0" }
  }
}

provider "vultr" {
  # api key comes from VULTR_API_KEY in the environment
}

locals {
  ssh_sources   = <{ ssh-sources-hcl|safe }>
  kafka_sources = <{ kafka-sources-hcl|safe }>
  node_count    = <{ node-count }>
  vpc_block     = split("/", "<{ vultr-vpc-subnet }>")[0]
  vpc_prefix    = tonumber(split("/", "<{ vultr-vpc-subnet }>")[1])
}

<% if ssh-keygen %># The machine keypair this deployment generated and owns (SSH Keypair
# Standard): the account resource is named after the profile and lives in this
# stack's state, which is what makes its ownership decidable. One key for every
# node — the cluster is one deployment, and a key per machine would multiply
# the thing the standard exists to make singular. Never reference a literal key
# id here in keygen mode.
resource "vultr_ssh_key" "machine" {
  name    = "<{ profile }>"
  ssh_key = trimspace(file("<{ ssh-public-key-path }>"))
}

<% endif %># The private network carrying the KRaft quorum and inter-broker replication.
# Nothing on those ports is ever published: the firewall below opens 22 and the
# Kafka port only, and the brokers bind 9093/9094 to the address handed out
# here. A VPC rather than the public interface is what makes that possible.
resource "vultr_vpc2" "cluster" {
  region        = "<{ vultr-region }>"
  description   = "<{ compute-name }>"
  ip_type       = "v4"
  ip_block      = local.vpc_block
  prefix_length = local.vpc_prefix
}

# Every label derives from one resolved name (Compute Name Standard §3), which
# is the profile unless desired state overrides it with vultr-name.
resource "vultr_firewall_group" "cluster" {
  description = "<{ compute-name }>-firewall"
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
  port              = "<{ kafka-port }>"
  ip_type           = strcontains(each.value, ":") ? "v6" : "v4"
  subnet            = split("/", each.value)[0]
  subnet_size       = tonumber(split("/", each.value)[1])
}

resource "vultr_instance" "node" {
  count = local.node_count

  # `label` is the console name and updates in place. There is deliberately no
  # `hostname`: Vultr implements a hostname change as an OS reinstall, so the
  # provider marks that attribute ForceNew, and editing the name would destroy
  # the instance and its disk rather than rename it.
  label             = "<{ compute-name }>-${count.index}"
  region            = "<{ vultr-region }>"
  plan              = "<{ vultr-plan }>"
  os_id             = <{ vultr-os-id }>
  firewall_group_id = vultr_firewall_group.cluster.id
  vpc2_ids          = [vultr_vpc2.cluster.id]
  # SSH keys are ids already in the account, and ForceNew: changing the key set
  # destroys and recreates the instance instead of re-authorizing it. Rotation
  # is a rebuild, never an edit on a machine whose disk you intend to keep.
<% if ssh-keygen %>  ssh_key_ids = [vultr_ssh_key.machine.id]
<% else %>  ssh_key_ids = ["<{ vultr-ssh-keys }>"]
<% endif %>  # Wait for ssh before starting Ansible.
  connection {
    type = "ssh"
    user = "root"
    host = self.main_ip
<% if ssh-keygen %>    private_key = file("<{ ssh-private-key-path }>")
<% endif %>  }
  provisioner "remote-exec" {
    inline = ["ls"]
  }
  lifecycle { prevent_destroy = <{ compute-prevent-destroy }> }
}

# A list, one entry per node, consumed as the Ansible inventory. `index` is the
# KRaft node.id, the machine label's suffix, and the broker name's ordinal —
# one number, so the three cannot drift apart.
output "params" {
  value = [
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

<% if ssh-keygen %>output "ssh_key_id" {
  value = vultr_ssh_key.machine.id
}
<% endif %>
