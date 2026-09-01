# Configuration

`colors.yml` is a flat, non-secret YAML map. The reference deployment is
`automq-vultr/colors.yml`. Validation reports every desired-state problem
together, so one run is enough to fix a file.

## Credentials

| Purpose | Environment variable |
|---|---|
| Vultr API | `COLORS_PAR_VULTR_API_KEY` |
| Cloudflare DNS (records and the DNS-01 challenge) | `COLORS_PAR_CLOUDFLARE_API_TOKEN` |
| AutoMQ object storage | `COLORS_PAR_AUTOMQ_R2_ACCESS_KEY_ID`, `COLORS_PAR_AUTOMQ_R2_SECRET_ACCESS_KEY` |
| R2 state backend | `COLORS_PAR_R2_ACCESS_KEY_ID`, `COLORS_PAR_R2_SECRET_ACCESS_KEY` |
| S3 state backend | `COLORS_PAR_S3_ACCESS_KEY_ID`, `COLORS_PAR_S3_SECRET_ACCESS_KEY` |

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

`vultr-region`, `vultr-plan`, `vultr-os-id`, `vultr-vpc-subnet`,
`vultr-ssh-sources`, `vultr-kafka-sources`. There is no required `vultr-name`:
machines are named after the profile with a numeric suffix, and the key exists
only as an override. Omitting `vultr-ssh-keys` selects keygen mode, where the
package owns `~/.ssh/<profile>`.

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
