# automq

A [getcolors](https://www.getcolors.ai/) Package Skill that provisions a
three-node [AutoMQ](https://github.com/AutoMQ/automq) cluster: the
Kafka 3.9.1 wire protocol, KRaft combined `broker,controller` roles, and
Cloudflare R2, managed AWS S3, managed Google Cloud Storage, or managed OCI Object Storage as the storage tier.

It ships in all three colours — `green/` (Clojure), `red/` (TypeScript) and
`blue/` (Python) — which render byte-identical artifacts from one `colors.yml`.
Pick whichever runtime your project already has; nothing else about the
deployment changes.

Deployment repositories include [`automq-vultr`](https://github.com/getcolors/automq-vultr),
[`automq-aws`](https://github.com/getcolors/automq-aws), and
[`automq-gcloud`](https://github.com/getcolors/automq-gcloud).

## Replication factor 1 is the design

AutoMQ acknowledges a produce once the record is in object storage. Replicas
would multiply cost and write amplification without adding durability, which is
why upstream's own `config/kraft/server.properties` ships RF=1 and why this
package does too. The three nodes exist for the controller quorum, for
partition failover, and for throughput.

What RF=1 does not buy is availability: a partition whose leader dies is
unavailable until it is reassigned. That window is measured by acceptance
rather than assumed away.

## Quick start

```sh
npx skills add getcolors/automq
cp .agents/skills/package-automq-green/green ./green   # or -red/red, or -blue/blue
chmod +x green
./green build              # renders .colors/ — contacts nothing
./green create --dry-run   # walks the DAG with no side effects
```

The installed launcher is a **copy** of the payload, not a symlink: after
`npx skills update -p`, copy it again or the project keeps running the old pin.

`build` and `--dry-run` work on a fresh checkout with an empty environment,
which makes them the safe way to check a `colors.yml` edit.

## What it provisions

| Layer | What |
|---|---|
| Compute | Three machines on a private network via colors-compute; public SSH/client and private quorum rules |
| DNS | Cloudflare DNS records, or `provider-dns: none` for direct public IP access |
| TLS | Let's Encrypt over DNS-01, or an explicit private CA with IP SANs |
| Storage | Two adopted R2 buckets, or two deployment-owned S3 buckets and a scoped IAM identity |
| Identity | Four SASL principals; no anonymous access on any listener |

Managed S3 uses `automq-storage-provider: s3` and
`automq-storage-managed: true`. The existing `automq-data-r2-bucket`,
`automq-ops-r2-bucket`, `automq-r2-endpoint`, and `automq-r2-region` keys also
address S3 for compatibility. Supply the regional S3 endpoint and AWS region;
application access keys are generated with access only to those two buckets,
kept in the state bucket, and passed to Ansible through its environment.
Use a separate `s3-bucket` for state; `s3-bucket-mode: managed` delegates its
lifecycle to colors-compute.

Managed GCS uses `automq-storage-provider: gcs`, the endpoint
`https://storage.googleapis.com`, a Google bucket location in
`automq-r2-region`, and `google-project`. Both application buckets, their
service account, and HMAC credentials belong to the deployment. Native state
uses `provider-backend: gcs` with `gcs-bucket`, `gcs-region`, and
`gcs-bucket-mode: managed`. Google Application Default Credentials authorize
provisioning. Owned GCS buckets disable soft deletion and are removed with
their contents during guarded delete. See the package configuration reference
for required project APIs and ownership checks.

Managed OCI storage uses `automq-storage-provider: oci` and
`automq-storage-managed: true`. The package creates private data and ops
buckets plus a user, group, bucket-scoped policy, customer secret key and RSA API signing key.
Use `oci-tenancy-id`, `oci-compartment-id`, `oci-namespace`,
`oci-config-file-profile`, `automq-oci-user-email`, and the OCI region in
`automq-r2-region`. The service user needs an email unique within the tenancy.
Identity Domains rejected creation without a primary email in the live test.
The endpoint is `https://<namespace>.compat.objectstorage.<region>.oraclecloud.com`.
Set `oci-home-region` when the tenancy home region differs, and `oci-auth:
SecurityToken` for a session profile. API key authentication is the default.
State uses a third OCI bucket through `provider-backend: oci`, `oci-bucket`,
`oci-region`, and `oci-bucket-mode: managed`. OpenTofu accesses that bucket
through OCI's S3 compatibility API. It does not create AWS resources.
The state credential pair is `COLORS_PAR_OCI_ACCESS_KEY_ID` and
`COLORS_PAR_OCI_SECRET_ACCESS_KEY`; application credentials are generated
separately and grant access only to the data and ops buckets.

Docker and lego use the host architecture, `amd64` or `arm64`; unsupported
architectures fail before installation. The AutoMQ image pin must contain
the selected platform.

OCI lease replacement uses native signed HEAD/PUT because the compatibility
endpoint ignores PUT `If-Match`. An OCI-only gate proves conditional writes
before genesis and waits up to 15 minutes for new credentials to propagate.

For operation without DNS credentials, set `provider-dns: none` and
`automq-tls-mode: private-ca`. Brokers advertise public IPs and acceptance
exports the public CA to `.colors/<profile>/automq-acceptance/ca.crt`. Clients
must trust that CA, for example with kcat `-X ssl.ca.location=<ca.crt>`.
The issuer's private CA key stays on node 0 in `/etc/automq/ca`. If that CA
is lost while a published certificate exists, renewal refuses to mint a new
trust root; restore the issuer CA from backup before renewing.

## Connecting

```sh
kcat -b <automq-host>:9092 \
  -X security.protocol=SASL_SSL -X sasl.mechanism=SCRAM-SHA-512 \
  -X sasl.username=automq -X sasl.password=<password> -L
```

The password is generated on the server. Retrieve it with `automq-credential`
over SSH — deliberately a separate command from `automq-status`, so routine
health output cannot leak a credential.

The client principal is not a superuser. It may produce and consume under the
configured topic prefix and nothing else; 9092 faces the internet, and
authentication alone is not a boundary.

## Operating

Over `ssh <profile>` (node 0) or `ssh <profile>-<n>`:

| Command | What it does |
|---|---|
| `automq-status` | Quorum, brokers, offline/under-replicated partitions, certificate expiry |
| `automq-credential` | Root only: prints the client SASL credential |
| `automq-smoke` | Re-runs the on-host gates |
| `automq-rotate` | Replaces the client password — atomic and disruptive |

## Recovery

- **A node lost its disk.** The converge refuses to reformat a node that
  previously completed a format, because rejoining a quorum as an empty voter
  is how a recovery becomes a data-loss event. Confirm the survivors hold a
  majority, then re-run with `AUTOMQ_ALLOW_REFORMAT=true`.
- **Certificate renewal.** Node 0's timer reissues and publishes; every node
  picks it up and restarts one at a time under an object-store lease.
- **Purging adopted storage.** `delete` leaves adopted R2 buckets intact. Empty
  them by hand, including `_colors/<profile>/` markers, before adopting again.
- **Deleting managed storage.** `automq-storage-managed: true` opts into ownership
  of both S3 application buckets and their IAM identity. An authorized `delete`
  stops the brokers, removes these buckets **including all cluster data**, then
  destroys compute. Existing or inaccessible buckets are refused on first create.
  The default adopted-storage mode never creates or deletes buckets.

## Limitations, stated plainly

- **Transactional workloads are unsupported here.** `__transaction_state` is
  RF=1 like every other internal topic; its behaviour across a broker outage is
  neither tested nor claimed.
- **No metrics export and no alerting.** `automq-status` is a point-in-time
  health surface. The counts it derives from the broker log are labelled
  best-effort, because logs rotate and reset. Exporting JMX to an observability
  stack is a prerequisite before calling this production-ready.
- **Rotation disconnects clients.** A SCRAM upsert replaces a credential
  immediately; there is no zero-downtime rotation for a single principal.

## Development

```sh
cd green && bb test && bb golden     # unit tests; fixtures: keygen, opt-out, and AWS
cd red   && bun test && bun run typecheck
cd blue  && uv run pytest
./scripts/parity.sh                  # the three colours, byte for byte
./scripts/launcher.sh                # the three copied payloads
```

`bb golden:accept` regenerates the committed output — only after reading the
diff. `scripts/parity.sh` is the net the goldens cannot be: it renders all three
fixtures in all three colours and diffs the trees, and it diffs the template
copies each colour carries. Use `AUTOMQ_LIB_ROOT` (the repository root, for
any colour), `COLORS_COMPUTE_LIB_ROOT`, `GREEN_LIB_ROOT` and `ONCE_LIB_ROOT` to develop across repository
boundaries.

## License

MIT.

Compute providers, SSH keys, and R2/S3 state are supplied by the pinned
[colors-compute library](https://github.com/getcolors/colors-compute). The package
declares AutoMQ's private-network requirements and node count; the library fans
out the same node workflow and joins its outputs for Ansible. Compatible provider
additions require a library dependency bump. Existing monolithic compute state
requires explicit migration and is refused by the new lifecycle.
