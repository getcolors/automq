---
name: package-automq-blue
description: Provision a three-node AutoMQ cluster with Kafka-compatible brokers whose storage tier is S3 object storage, behind a public SASL_SSL endpoint with SCRAM authentication and ACL authorization.
license: MIT
---

# AutoMQ cluster (Blue)

Read [references/configuration.md](references/configuration.md) before changing
state or running a lifecycle command.

## What this provisions

Three machines, each running AutoMQ with both KRaft roles
(`broker,controller`). The pinned colors-compute library provisions their private
network and firewall. SSH uses port 22, clients use 9092, and the private
controller and inter-broker listeners use 9093 and 9094. Cloudflare holds one
A record per node on the bootstrap name and one per broker, all DNS-only. One
Let's Encrypt certificate, issued over DNS-01 by node 0 and distributed to the
others through object storage, covers the bootstrap name and every broker name.

AutoMQ stores durable records in R2 before acknowledging a produce. Keep
replication factor 1, the upstream default. The nodes provide the controller
quorum, partition failover, and throughput.

## Safety

- Keep credentials out of `colors.yml`; use ignored `COLORS_PAR_*` exports.
- Never set `COLORS_PAR_PROFILE` and never edit generated `.colors/` files.
- Use `build` and `create --dry-run` before a real lifecycle operation.
- Keep `compute-prevent-destroy: true`. Destroying requires a deliberate
  one-run override of that guard.
- `delete` retains adopted object storage. With `automq-storage-managed: true`,
  it deletes the S3 data and ops buckets and their contents after stopping the
  brokers. A managed S3 backend bucket is deleted last, after retirement checks.
- The two buckets must be **empty** at first adoption and belong to this
  deployment alone. AutoMQ writes hash-prefixed keys at the bucket root and
  supports no path prefix, so it cannot share a bucket with anything.

## Commands

```sh
./blue validate     # desired state, tools, and credentials
./blue build        # render .colors/ only; contacts nothing
./blue create --dry-run
./blue create
./blue delete       # guarded; stops the cluster, destroys DNS and compute
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
