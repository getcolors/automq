---
name: package-automq-green
description: Provision a three-node AutoMQ cluster with Kafka-compatible brokers backed by Cloudflare R2, AWS S3, or Google Cloud Storage, behind a public SASL_SSL endpoint with SCRAM authentication and ACL authorization.
license: MIT
---

# AutoMQ cluster (Green)

Read [references/configuration.md](references/configuration.md) before changing
state or running a lifecycle command.

## What this provisions

By default, three machines each run AutoMQ with both KRaft roles
`broker,controller`. The pinned colors-compute library provisions their private
network and firewall for the selected compute provider, including Vultr, AWS
and Google Cloud. SSH uses port 22, clients use 9092, and the private controller
and inter-broker listeners use 9093 and 9094.

AutoMQ stores durable records in object storage before acknowledging a produce.
Keep replication factor 1, the upstream default. The nodes provide the
controller quorum, partition failover, and throughput.

| Storage mode | Ownership |
|---|---|
| Adopted Cloudflare R2 | Existing empty data and ops buckets belong to this cluster; delete retains them. |
| Managed AWS S3 | The deployment creates data and ops buckets plus a scoped IAM identity. |
| Managed GCS | The deployment creates data and ops buckets plus a scoped service account and HMAC key. |

OpenTofu state uses a separate R2, S3, or native GCS bucket. Managed S3 and
GCS state buckets follow the deployment lifecycle alongside its managed SSH
keypair. See the configuration reference for provider options and credentials.

The default TLS mode uses Cloudflare DNS-only records and ACME. Node 0 issues
one Let's Encrypt certificate covering every broker name and the bootstrap
name, then distributes it through object storage. Alternatively, set
`provider-dns: none` and `automq-tls-mode: private-ca`. That mode creates no DNS
records, requires no Cloudflare token, advertises broker public IPs, and exports
the public CA certificate for clients.

## Safety

- Keep credentials out of `colors.yml`; use ignored `COLORS_PAR_*` exports or
  the selected provider's ambient authentication.
- Never set `COLORS_PAR_PROFILE` and never edit generated `.colors/` files.
- Use `build` and `create --dry-run` before a real lifecycle operation.
- Keep `compute-prevent-destroy: true`. Destroying requires a deliberate
  one-run override of that guard.
- `delete` retains adopted object storage. With `automq-storage-managed: true`,
  it deletes the S3 or GCS data and ops buckets and their contents after stopping
  the brokers. A managed state bucket is deleted last, after retirement checks.
- Adopted data and ops buckets must be empty at first adoption. Managed storage
  creates new buckets and refuses to adopt existing ones. Each bucket must belong
  to this deployment alone. AutoMQ writes hash-prefixed keys at the bucket root and
  supports no path prefix, so it cannot share a bucket with anything.

## Commands

```sh
./green validate     # desired state, tools, and credentials
./green build        # render .colors/ only; contacts nothing
./green create --dry-run
./green create
./green delete       # guarded; stops the cluster, destroys DNS and compute
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

For `private-ca` mode, replace `<automq-host>` with a broker's public IP and add
`-X ssl.ca.location=.colors/<profile>/automq-acceptance/ca.crt`. Acceptance exports
that public certificate after convergence.

The client principal may produce and consume on topics and groups under the
configured prefix, and nothing else. It is deliberately not a superuser: 9092
faces the internet.
