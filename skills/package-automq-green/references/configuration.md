# Configuration

`colors.yml` is a flat, non-secret YAML map. The reference deployment is
`automq-vultr/colors.yml`. Validation reports every desired-state problem
together, so one run is enough to fix a file.

## Credentials

| Purpose | Environment variable |
|---|---|
| Cloudflare DNS (records and the DNS-01 challenge) | `COLORS_PAR_CLOUDFLARE_API_TOKEN` |
| AutoMQ object storage | `COLORS_PAR_AUTOMQ_R2_ACCESS_KEY_ID`, `COLORS_PAR_AUTOMQ_R2_SECRET_ACCESS_KEY` |
| R2 state backend | `COLORS_PAR_R2_ACCESS_KEY_ID`, `COLORS_PAR_R2_SECRET_ACCESS_KEY` |
| S3 state backend | Ambient AWS credential chain |

Compute credentials and provider options follow the version of
[colors-compute](https://github.com/getcolors/colors-compute) pinned by this
skill. The library also owns R2 and S3 remote state configuration.

Never export `COLORS_PAR_PROFILE`.

The storage pair is the only credential written to the hosts, which is why it
should be scoped to the two AutoMQ buckets and nothing else. The Cloudflare
token reaches exactly one host — node 0, the certificate issuer — so
compromising either other broker yields no control over the zone.

Every password inside the cluster is generated on node 0 at first converge and
exists nowhere else: the four SASL principals, their SCRAM salts, and the
keystore password. None of them is an operator credential, and none is ever
written to `.colors/`, to a golden file, or to an Ansible variable.

The package refuses to run against a `~/.ssh/config` that already declares
`Host <profile>` or `Host <profile>-<n>` outside its own markers, or whose
first option stands above the first `Host` line.

## Managed AWS storage and private TLS

Set `automq-storage-managed: true` and `automq-storage-provider: s3` to create
both application buckets and an IAM identity restricted to them. The existing
`automq-data-r2-bucket`, `automq-ops-r2-bucket`, `automq-r2-endpoint`, and
`automq-r2-region` keys specify S3 bucket names, HTTPS regional endpoint, and
AWS region. No application storage access keys are required from the operator.
The generated credentials remain in sensitive Terraform outputs and reach
Ansible only through its process environment.

Managed `delete` removes both buckets **and all their data** after stopping
brokers. Adopted R2 storage remains the default and survives `delete`.
Managed first create refuses to adopt an existing or inaccessible bucket.
State must use a third bucket; `provider-backend: s3`, `s3-bucket`, `s3-region`,
and `s3-bucket-mode: managed` select the colors-compute state bucket lifecycle.

`provider-dns: none` requires `automq-tls-mode: private-ca`; no Cloudflare token
is needed. Brokers advertise public IPs, and clients trust the public CA
exported by acceptance to `.colors/<profile>/automq-acceptance/ca.crt`.
The defaults remain Cloudflare DNS and `automq-tls-mode: acme`.

## Desired state

### Cluster

| Key | Meaning |
|---|---|
| `automq-image` | Container image, **required to carry a digest** |
| `automq-node-count` | Node count; must be odd, 1–9 |
| `automq-cluster-id` | Base64 UUID from `kafka-storage.sh random-uuid`; also the object namespace |
| `automq-host` | Bootstrap hostname |
| `automq-broker-name-prefix` | Broker names are `<prefix><i>.<automq-host>` |
| `automq-heap-opts` | JVM heap and direct memory |
| `automq-topic-partitions`, `automq-log-retention-hours` | Topic defaults |

`automq-cluster-id` is not a runtime accident. It is written into every node's
metadata log at genesis *and* is AutoMQ's object namespace, so changing it on a
live deployment orphans the data rather than renaming it.

### Listeners and identity

| Key | Meaning |
|---|---|
| `automq-kafka-port` | Public SASL_SSL listener (9092) |
| `automq-internal-port` | Inter-broker listener, VPC-bound (9094) |
| `automq-controller-port` | KRaft controller listener, VPC-bound (9093) |
| `automq-sasl-user` | The public client principal, ACL-scoped |
| `automq-admin-user`, `automq-broker-user`, `automq-controller-user` | Superuser principals |
| `automq-client-topic-prefix` | The namespace the client principal may use |

All four principals must differ: they share one namespace in the metadata log,
three of them are superusers, and a collision is a privilege escalation.

### Object storage

| Key | Meaning |
|---|---|
| `automq-data-r2-bucket` | Stream objects and the S3 WAL |
| `automq-ops-r2-bucket` | Operational objects, plus this package's own markers |
| `automq-r2-endpoint`, `automq-r2-region` | S3-compatible endpoint |
| `automq-wal-batch-interval-ms`, `automq-wal-max-bytes-in-batch` | WAL batching |

The two buckets must differ from each other and from the state bucket. Never
configure lifecycle rules on either: they would delete live WAL and stream
objects the cluster still references.

`automq-wal-batch-interval-ms` is the lever for object storage that lives in a
different region or provider than the compute. Every produce acknowledgement
waits on an S3 write, so raising it trades latency for throughput.

### Compute

Set `provider-compute` and that provider's options from the pinned
colors-compute library. AutoMQ requires a private network with private-source
firewall filtering. The library rejects providers that cannot meet this
requirement. Supporting another compatible provider requires a library pin bump.

`automq-ssh-sources` controls SSH ingress and must be nonempty.
`automq-kafka-sources` controls client ingress and may be empty. The library
also accepts the corresponding legacy provider-prefixed source keys.
Machines are named `<profile>-<node-id>` unless the provider name override is
set. The library owns the managed profile keypair, or uses explicitly configured
external keys and their private identity path.

Set `provider-backend` to `r2` or `s3`. Compute uses separate shared and per-node
state objects plus a deployment journal. Existing monolithic compute state is
refused and requires an explicit migration before create or delete.

## Recovery

- **A node lost its disk.** The converge refuses to reformat a node that
  previously completed one, because a silent reformat rejoins the quorum as an
  empty voter. Confirm the survivors hold a majority, then re-run with
  `AUTOMQ_ALLOW_REFORMAT=true` to authorize one reformat.
- **The certificate expired.** `automq-cert` on node 0 reissues and publishes;
  every node's `automq-cert-deploy.timer` picks it up and restarts one at a
  time under an object-store lease.
- **The client password leaked.** `automq-rotate`. It is an atomic replace and
  disconnects existing clients; there is no zero-downtime rotation for a single
  principal.
- **Purging storage.** `delete` deliberately leaves the buckets alone. Empty
  them by hand, including the `_colors/<profile>/` markers, before adopting
  them again.
