---
name: package-automq-red
description: Provision a three-node AutoMQ cluster on Vultr — Kafka-compatible brokers whose storage tier is S3 object storage, behind a public SASL_SSL endpoint with SCRAM authentication and ACL authorization.
license: MIT
---

# AutoMQ cluster (Red)

Read [references/configuration.md](references/configuration.md) before changing
state or running a lifecycle command.

## What this provisions

Three Vultr instances, each running AutoMQ with both KRaft roles
(`broker,controller`). A Vultr VPC carries the controller quorum and
inter-broker traffic; the firewall opens 22 and 9092 only. Cloudflare holds one
A record per node on the bootstrap name and one per broker, all DNS-only. One
Let's Encrypt certificate, issued over DNS-01 by node 0 and distributed to the
others through object storage, covers the bootstrap name and every broker name.

**Durability comes from object storage, not from replicas.** Every topic is
replication factor 1, which is upstream's own default and is not a
misconfiguration: bytes are in R2 before a produce is acknowledged. The three
nodes buy a controller quorum, partition failover, and throughput — not copies.
Read that sentence again before "fixing" the replication factor.

## Safety

- Keep credentials out of `colors.yml`; use ignored `COLORS_PAR_*` exports.
- Never set `COLORS_PAR_PROFILE` and never edit generated `.colors/` files.
- Use `build` and `create --dry-run` before a real lifecycle operation.
- Keep `compute-prevent-destroy: true`. Destroying requires a deliberate
  one-run override of that guard.
- `delete` never touches the object storage buckets. They hold the cluster's
  data, and an accidental `delete` must stay recoverable.
- The two buckets must be **empty** at first adoption and belong to this
  deployment alone. AutoMQ writes hash-prefixed keys at the bucket root and
  supports no path prefix, so it cannot share a bucket with anything.

## Commands

```sh
./red validate     # desired state, tools, and Vultr access
./red build        # render .colors/ only — contacts nothing
./red create --dry-run
./red create
./red delete       # guarded; stops the cluster, destroys DNS and compute
```

On the hosts, over `ssh <profile>` or `ssh <profile>-<n>`:

```sh
automq-status        # quorum, brokers, partitions, certificate expiry
automq-credential    # root only: prints the client SASL credential
automq-smoke         # re-run the on-host gates
automq-rotate        # replace the client password (disconnects clients)
```

## Connecting

```sh
kcat -b <automq-host>:9092 \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 \
  -X sasl.username=automq -X sasl.password=<from automq-credential> -L
```

The client principal may produce and consume on topics and groups under the
configured prefix, and nothing else. It is deliberately not a superuser: 9092
faces the internet.
