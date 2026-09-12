---
name: package-automq-green
description: Provision a three-node AutoMQ cluster with Kafka-compatible brokers backed by Cloudflare R2, AWS S3, Google Cloud Storage, or OCI Object Storage, behind a public SASL_SSL endpoint with SCRAM authentication and ACL authorization.
license: MIT
---

# AutoMQ cluster (Green)

Read [references/configuration.md](references/configuration.md) before changing
state or running a lifecycle command.

## What this provisions

By default, three machines each run AutoMQ with both KRaft roles
`broker,controller`. The pinned colors-compute library provisions their private
network and firewall for the selected compute provider, including Vultr, AWS,
Google Cloud and OCI. SSH uses port 22, clients use 9092, and the private controller
and inter-broker listeners use 9093 and 9094.

AutoMQ stores durable records in object storage before acknowledging a produce.
Keep replication factor 1, the upstream default. The nodes provide the
controller quorum, partition failover, and throughput.

| Storage mode | Ownership |
|---|---|
| Adopted Cloudflare R2 | Existing empty data and ops buckets belong to this cluster; delete retains them. |
| Managed AWS S3 | The deployment creates data and ops buckets plus a scoped IAM identity. |
| Managed GCS | The deployment creates data and ops buckets plus a scoped service account and HMAC key. |
| Managed OCI | The deployment creates private data and ops buckets, a scoped IAM identity, a customer secret key and an API signing key. |

OpenTofu state uses a separate R2, S3, GCS or OCI bucket. Managed S3, GCS and
OCI state buckets follow the deployment lifecycle alongside the managed SSH
keypair. OCI state uses its S3 compatibility endpoint; all buckets remain OCI
resources. See the configuration reference for provider options and credentials.

OCI's S3 compatibility endpoint ignores PUT `If-Match`. Restart lease renewal,
takeover and release therefore use native OCI HEAD/PUT with native ETags,
signed by the scoped application's lifecycle-owned API key. The compute
library also uses native OCI conditional writes for ownership journals.
Before genesis, the OCI storage gate proves conditional create and native
replacement, including stale ETag rejection. It waits at most 900 seconds for
new credentials to propagate. The gate also proves missing-object reads and
exact-byte writes, reads and deletes in both application buckets. OCI hosts
use a persistent, scoped native INPUT chain ahead of the platform reject;
UFW remains inactive. See
[the OCI configuration contract](references/configuration.md#managed-oci-object-storage)
before changing credentials, endpoints or lease behavior.

On 2026-09-12, three OCI `VM.Standard.A1.Flex` ARM brokers (1 OCPU and 8 GB
RAM each, across three Frankfurt availability domains) completed three full
converges: two on source `5aac5a3` and one on final source `6b76c01`. Each passed
all 16 public checks; the final run also captured all 9 host gates and unchanged
firewall application on all three hosts. IP-SAN TLS, public records,
authentication and ACL denials passed. An abrupt kill of the selected partition
leader recovered writes in 11, 11 and 10 seconds respectively and retained the
pre-fault records and
committed consumer offsets. The offsets topic's leader was not forced onto the
victim, so this is not proof of offsets-leader recovery. Scoped application
credentials were denied access to the existing state bucket; native CAS and
lease checks passed. A real reboot preserved the repaired OCI platform and
application firewall rules. Final audits retained all 50 independent records,
node identities, secrets and CA hashes across the converges and reboots. Six
final helper calls made no changes, and each host retained one native NTP rule.

The 2026-09-11 account could not launch VMs because of capacity and shape
errors; its storage and partial-deployment cleanup evidence remains historical.
Cleanup of the fully running September 12 cluster has not been repeated.
See the [dated deployment evidence](https://github.com/getcolors/automq-oci/tree/ee2789daa1738b33563488b9294940179234ea09/evidence/2026-09-12)
for the tested scope and retained failures.

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
  it deletes managed S3, GCS or OCI data and ops buckets and their contents after
  stopping the brokers. OCI cleanup verifies live bucket OCIDs and ownership
  tags against recorded state before removing objects and incomplete multipart
  uploads. It then deletes application buckets and IAM resources. A managed
  state bucket is deleted last, after retirement checks.
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
./green delete       # guarded; removes compute and managed storage
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
