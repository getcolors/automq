# Configuration

`colors.yml` is a flat, non-secret YAML map. Reference deployments include
`automq-vultr/colors.yml`, `automq-aws/colors.yml`, and `automq-gcloud/colors.yml`. Validation reports every desired-state problem
together, so one run is enough to fix a file.

## Credentials

| Purpose | Environment variable |
|---|---|
| Cloudflare DNS (records and the DNS-01 challenge) | `COLORS_PAR_CLOUDFLARE_API_TOKEN` |
| AutoMQ object storage | `COLORS_PAR_AUTOMQ_R2_ACCESS_KEY_ID`, `COLORS_PAR_AUTOMQ_R2_SECRET_ACCESS_KEY` |
| R2 state backend | `COLORS_PAR_R2_ACCESS_KEY_ID`, `COLORS_PAR_R2_SECRET_ACCESS_KEY` |
| S3 state backend | Ambient AWS credential chain |
| Google compute, GCS storage and state | Application Default Credentials for OpenTofu; active `gcloud` account for lifecycle operations |

Compute credentials and provider options follow the version of
[colors-compute](https://github.com/getcolors/colors-compute) pinned by this
skill. The library also owns R2, S3, native GCS and OCI remote state configuration.
Managed application storage generates its scoped access keys; the operator does
not supply `COLORS_PAR_AUTOMQ_R2_*` in that mode. Cloudflare credentials are
required only when `provider-dns: cloudflare`.

Never export `COLORS_PAR_PROFILE`.

The object-storage credentials installed on each host must be scoped to the two
AutoMQ buckets. In ACME mode, the Cloudflare token reaches only node 0, the
certificate issuer. Private-CA mode needs no Cloudflare token.

The cluster generates its SASL passwords, SCRAM salts and keystore password
at first convergence and distributes the secret bundle to its nodes. Later
convergences reuse a surviving node's bundle. These are not operator-supplied
credentials and are never rendered into `.colors/` or golden files.

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

Set `provider-backend` to `r2`, `s3`, `gcs`, or `oci`. Compute uses separate shared and per-node
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
- **Purging adopted storage.** `delete` retains adopted buckets. Empty them,
  including the `_colors/<profile>/` markers, before adopting them again.
  Managed delete removes the owned buckets and their contents.

## Managed Google Cloud Storage

Set `automq-storage-managed: true`, `automq-storage-provider: gcs`,
`google-project`, `automq-r2-endpoint: https://storage.googleapis.com`, and
`automq-r2-region` to the bucket location. The existing `automq-data-r2-bucket`
and `automq-ops-r2-bucket` keys name the GCS buckets. The package creates a
service account and HMAC key restricted to these two buckets. It refuses to
adopt buckets that are not already recorded in its storage state.

Use a third bucket with `provider-backend: gcs`, `gcs-bucket`, `gcs-region`,
and `gcs-bucket-mode: managed` for native GCS OpenTofu state. Authentication
uses Google Application Default Credentials for OpenTofu. Lifecycle ownership
checks and the state journal use the active `gcloud` account. Both identities
must have access to the selected project. Enable Compute Engine, Cloud
Storage, IAM, Cloud Resource Manager, and Service Usage APIs in the project.

Delete stops the brokers, removes data and ops buckets with their contents,
removes the storage identity, destroys compute and its SSH keypair, then
finalizes the managed state bucket. GCS soft deletion is disabled for owned
application buckets so this lifecycle does not retain deleted object data.
The normal `compute-prevent-destroy` guard still applies.

GCS ownership markers and restart leases use `x-goog-if-generation-match`.
GCS does not accept S3 ETag write preconditions as the same contract.
The storage helper translates botocore's signing headers to `x-goog-*` before
signing because GCS rejects requests that mix `x-amz-*` and `x-goog-*` headers.
Do not substitute `x-amz-if-generation-match`: GCS silently ignores it and
allows competing writers to succeed. The helper replaces conditional headers
on retries so each request carries one value.

## Ubuntu security repository mirror

`automq-apt-security-mirror` optionally selects an HTTP or HTTPS mirror before
base package installation. It updates the `URIs` line directly above the
security `Suites` line in `/etc/apt/sources.list.d/ubuntu.sources`. The task
preserves the security suite, components and Ubuntu archive signing key.
Omit this option to retain the image's repository configuration. Use a mirror
that serves signed metadata for the selected Ubuntu security suite.

## Managed OCI Object Storage

Managed OCI storage uses `automq-storage-provider: oci` and
`automq-storage-managed: true`. The package creates private data and ops
buckets plus a user, group, bucket-scoped policy, customer secret key and RSA API signing key.
Use `oci-tenancy-id`, `oci-compartment-id`, `oci-namespace`,
`oci-config-file-profile`, `automq-oci-user-email`, and the OCI region in
`automq-r2-region`. The service user needs an email unique within the tenancy.
Identity Domains rejected creation without a primary email in the live test.

OCI Ubuntu images reject inbound traffic in native iptables even when UFW is
inactive. The package installs an owned INPUT chain before that reject:
Kafka uses its configured source CIDRs, and internal/controller ports accept
only the cluster peers. A boot unit restores the chain after the platform
firewall and before Docker. It preserves existing platform, iSCSI and Docker
rules and does not enable UFW. See [Oracle's platform image firewall guidance](https://docs.oracle.com/en-us/iaas/Content/Compute/References/images.htm).

New OCI credentials must pass listing, missing-object GET and exact-byte
PUT/GET/DELETE probes against both application buckets before conditional
writes are tested. A successful listing alone did not prove GetObject was
ready in the live run. Authentication failures retry only inside the bounded
pre-genesis readiness gate; ownership and genesis refusals remain failures.
Readiness records each exact random probe key in a root-owned local ledger
before writing. Retries must remove recorded probes and verify absence before
starting new ones. A ledger for a different storage identity fails immediately;
the package never sweeps a prefix or relaxes bucket adoption to ignore debris.
The endpoint is `https://<namespace>.compat.objectstorage.<region>.oraclecloud.com`.
Set `oci-home-region` when the tenancy home region differs, and `oci-auth:
SecurityToken` for a session profile. API key authentication is the default.
State uses a third OCI bucket through `provider-backend: oci`, `oci-bucket`,
`oci-region`, and `oci-bucket-mode: managed`. OpenTofu accesses that bucket
through OCI's S3 compatibility API. It does not create AWS resources.
The state credential pair is `COLORS_PAR_OCI_ACCESS_KEY_ID` and
`COLORS_PAR_OCI_SECRET_ACCESS_KEY`; application credentials are generated
separately and grant access only to the data and ops buckets.

The application stage refuses to adopt existing OCI buckets. It checks every
page of a successful native bucket listing; OCI's ambiguous
`NotAuthorizedOrNotFound` response does not prove a bucket is absent.
On guarded delete, the package stops brokers, checks recorded bucket names,
namespaces, compartments, live bucket OCIDs and ownership tags, aborts incomplete multipart uploads and deletes
objects. OpenTofu then removes buckets and application IAM resources. The
compute library finalizes the state bucket last. Existing adopted storage
keeps its original deletion policy.

The public deployment repository is
[automq-oci](https://github.com/getcolors/automq-oci). See its evidence before
claiming live acceptance for a particular pin.

OCI's S3 compatibility endpoint enforces conditional create but ignored
`If-Match` on PUT in the live negative test. Lease renewal, expired lease
takeover and release therefore use native OCI HEAD/PUT with native ETags and
the scoped application's API signing key. Broker data still uses the S3
compatibility API. The signing key stays in the state bucket and root-only
`store.env` and `cert.env`; it is not a Docker environment variable.

Before genesis, an OCI-only gate verifies conditional create, exact native
ETag replacement, stale replacement rejection and the retained object body.
It removes its temporary key and retries credential propagation for up to
15 minutes. Missing signing credentials fail before Ansible starts.

The host play supports `x86_64` and `aarch64`. Docker's repository architecture
and lego's binary name follow the host. The pinned AutoMQ image must include
that platform. `oci-memory-in-gbs` is optional in the pinned compute library;
omit it to request the shape's API default. Omission did not resolve the
observed A2 ratio rejection, so do not treat it as a verified repair.

Public acceptance verifies literal-IP endpoints against certificate IP SANs
without requiring reverse DNS. Its failover gate selects the actual partition
leader, starts the recovery timer before an abrupt Docker KILL and verifies the
victim stays stopped until the outage checks finish. Graceful-stop timing from
earlier acceptance scripts does not prove abrupt crash recovery.
