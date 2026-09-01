# Plan: `automq` Package Skill + `automq-vultr` 3-node deployment

_Locked via claudex-loop — by Claude + Alberto. Revised after Codex rounds 1-5._

## Goal

Ship `automq`, a green-only getcolors Package Skill that provisions a three-node
AutoMQ 1.7.4 cluster (Kafka 3.9.1 wire protocol, S3-backed storage) on Vultr,
and `automq-vultr`, the deployment that consumes it. AutoMQ's durability comes
from Cloudflare R2, not from replicas; the three nodes exist for the KRaft
controller quorum, partition failover, and throughput. The public endpoint is
`SASL_SSL`/`SCRAM-SHA-512` on 9092 with a Let's Encrypt certificate and an ACL
authorizer; the quorum and inter-broker traffic never leave a Vultr VPC. Done
means a real converge whose acceptance gates — including a broker kill targeted
at a partition that broker leads — pass against the live cluster, both
repositories pushed to `getcolors`, and the launcher stamped from a pushed SHA.

## Approach

### 1. Repository `automq/` (green-only, root-level layout)

Following `alice`/`rama`, not `neon`'s tri-colour subdir:

```
bb.edn  deps.edn  colors.yml  devenv.nix  CLAUDE.md  README.md  index.html
green -> skills/package-automq-green/green
scripts/{golden.sh,launcher.sh}   tasks/pin.clj
src/clj/io/github/getcolors/automq/{validate,workflow,tools,ssh,ssh_config,utils,operator}.clj
src/resources/io/github/getcolors/automq/tools/
    infrastructure/main.tf        dns/main.tf
    ansible-local/{ansible.cfg,inventory.ini,main.yml}
    ansible/{ansible.cfg,main.yml,compose.yml,server.properties,
             adopt.py,format.sh,scram.sh,cert.sh,cert-deploy.sh,
             status.sh,credential.sh,smoke.sh,rotate.sh}
    acceptance/acceptance.sh
test/{fixtures/{colors.yml,optout.yml},clj/...,resources/golden/...}
skills/package-automq-green/{SKILL.md,green,references/configuration.md}
```

SDK pins: green `ceb4159a19ec281d60f10d11295c05c5de5f1c42`, once
`759eb0311b4bdf881eab813cfe5d00f76b9310cc` (`:deps/root "green"`). Launcher
contract starts at 1; `automq-sha` is `nil` until `bb pin` stamps a pushed SHA.

### 2. Desired state additions

`automq-admin-user: automq-admin` (superuser principal used by on-host tooling),
`automq-topic-partitions`, `automq-log-retention-hours`, `automq-client-topic-prefix:
colors-`. Everything else as committed in Phase 2.

### 3. Stage graph

```
create:  start -> infrastructure -> ssh-config -> dns -> ansible -> acceptance
delete:  start -> ansible-local(absent) -> dns -> infrastructure -> ssh-cleanup
```

`start` runs preflight: defaults, `COLORS_PAR_*` overlay, `COLORS_PAR_PROFILE`
refusal, desired-state validation, credential presence, tool presence, a live
Vultr API probe distinguishing 401/403 from 5xx from a `000` local network
failure, the ONCE keypair create matrix, and the `~/.ssh/config` ownership check.
Backend advice keys state at `<profile>/<stage>.tfstate`.

### 4. `infrastructure` stage — one stack, three nodes

- `vultr_vpc2` with `automq-vpc-subnet`; every node attached.
- One `vultr_firewall_group`; 22 from `vultr-ssh-sources`, 9092 from
  `vultr-kafka-sources`, `for_each` with v4/v6 discrimination. 9093/9094 are
  never opened — they bind the VPC address only.
- `vultr_ssh_key.machine` in keygen mode, named after the profile.
- `vultr_instance.node`, `count = <node-count>`, labels
  `<compute-name>-${count.index}`, `lifecycle { prevent_destroy = … }`.
- `output "params"` is a list of `{index, ip, vpc_ip, user, sudoer, name}`.

### 5. `dns` stage

`cloudflare_dns_record` constructs in `record.tf.json` via
`tofu/constructs-json`: three A records on the bootstrap name and one per broker
(`b<i>.automq.bigconfig.online`). **All `proxied: false`** — Cloudflare's proxy is
HTTP-only and would black-hole 9092. Runs after `infrastructure`, before
`ansible`.

### 6. `ansible` stage — converge each node

**6a. Host and runtime.** Docker Engine + Compose plugin; `/etc/automq`,
`/var/lib/automq` owned by the container uid; `/etc/automq/secrets` 0700. The
Compose service runs `network_mode: host` — a bridged container cannot bind the
host's VPC address, which the CONTROLLER and INTERNAL listeners require.

**6b. Certificates — one issuer, object-store distribution, staggered restarts.**
Node 0 alone runs ACME and is the only host that receives the Cloudflare token;
nodes 1 and 2 never hold a zone-editing credential. The SAN list is derived
explicitly, never guessed from the zone: the exact bootstrap name
(`automq-host`) plus every broker name `b<i>.<automq-host>`. No wildcard.

Distribution does not depend on an Ansible control connection, because renewal
happens on a timer months after the converge that installed it. Node 0's deploy
hook publishes the certificate and key to a reserved `_colors/<profile>/tls/`
key in the **ops bucket**; every node runs a timer that pulls when the object's
ETag changes, writes a temporary PKCS#12, validates it, atomically renames it,
and restarts. No new trust path is created: every node already holds the R2
credential, and the alternative — node 0 holding SSH access to its peers —
would add lateral movement between publicly reachable brokers.

Restarts are serialized by a **lease, not by hope**. Three nodes watching the
same ETag will observe it at the same moment, and each would pass its own local
quorum check before any of them had gone down — a local check cannot order
independent actors. The renewal generation is published as an object listing the
node order; a node may restart only after acquiring the deploy lease
(conditional create, `If-None-Match: *`, with a TTL so a dead holder cannot
wedge the cluster) *and* seeing its predecessor's completion acknowledgement.
It verifies quorum health, broker registration, and the served certificate
serial, writes its own acknowledgement, and releases. These are combined
broker+controller nodes: restarting all three together destroys the KRaft
majority.

**6c. Bucket adoption — verified, atomic, with an explicit state machine.**
`adopt.py` runs `run_once` before any node boots. Each bucket is in exactly one
of `{empty, init, ready}`, and the pair's combined state decides the outcome:

1. **Emptiness is proven, not inferred.** Paginate the *entire* bucket; on first
   adoption any object at all is fatal. A marker check alone would miss a
   foreign AutoMQ cluster's `<hash>/_kafka_…` keys, which live at the root.
2. **Ownership is claimed conditionally** (`If-None-Match: *`) with a nonce, so
   two deployments racing an empty bucket cannot both win.
3. **The transaction id is minted before either claim and written into both**
   init markers, alongside profile, cluster id, bucket role, endpoint and schema
   version. It is never held only in memory: a converge that dies between the two
   claims must be resumable by a later process that never saw the original id.
   `(empty, empty)` → mint and claim both. `(init, empty)` → **the existing init
   marker is authoritative** if its immutable identity matches desired state; its
   transaction id is recovered from the marker and used to claim the empty peer
   conditionally. `(init, init)` with equal ids → resume; unequal → fatal, two
   deployments raced. `(ready, ready)` with matching identity → adopt. Anything
   else, `(ready, empty)` included, is fatal with an explicit operator
   instruction: storage was replaced underneath a live cluster.
4. **Identity is immutable.** A changed bucket, endpoint or cluster id is refused
   with migration instructions rather than silently attaching empty storage.
5. `.colors-ready` is written from one `run_once` task after the smoke gates.

**6d. Metadata storage — genesis is authorized by an epoch, not by readiness.**
`.colors-ready` cannot authorize formatting: it is written only after smoke, so
an interrupted first converge would leave formatted nodes while the only durable
state still said "not ready" — and the next run would reformat them. Instead a
**genesis epoch** and a per-node format record are written to the ops bucket,
and those records are what authorize formatting. The record is two-phase —
`format-intent` before the format, `format-complete` after `meta.properties`
exists — because a single record written beforehand would poison the next run:
a converge killed between the record and the format would look exactly like
disk loss. Missing metadata with a matching `format-intent` is a resumable
incomplete genesis; missing metadata *after* `format-complete` is disk loss, and
that fails the converge with a recovery procedure and requires an explicit
one-run authorization to reformat a node the cluster already knows. Every converge parses
`meta.properties` and fails on a cluster-id or node-id mismatch.

**6e. SCRAM — one salt, computed once, identical on every voter.**
`--add-scram` with a *plaintext* password generates a random salt independently
on each node, so formatting three voters that way produces three divergent
bootstrap checkpoints for the same user. Each principal's salt and salted
password are therefore computed **once**, and all three genesis formats receive
the identical explicit form
`SCRAM-SHA-512=[name=…,salt=…,saltedpassword=…,iterations=…]` — one per SCRAM
principal: `automq-broker`, `automq-admin` and `automq`. `automq-controller` is
deliberately *not* among them: it authenticates with PLAIN from a static JAAS
file precisely so that the controller quorum depends on nothing stored in the
metadata log it is trying to form.
This is the same class of trap as neon's stored SCRAM verifiers, and for the
same reason: randomness inside a converge destroys determinism.

Replacement nodes are formatted **without** bootstrap records — KRaft keeps SCRAM
credentials in the replicated metadata log and a replacement voter catches up
from the quorum. Drift is detected by *authenticating* with the desired
credential, never by `kafka-configs --describe`, which exposes iterations and
salt but nothing comparable to a plaintext. An upsert runs only when that
authentication fails or a versioned rotation marker changes.

**6f. Authentication and authorization — four principals, no anonymous access.**
`authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer`,
`allow.everyone.if.no.acl.found=false`,
`super.users=User:automq-admin;User:automq-broker;User:automq-controller`.

Every listener authenticates. `ANONYMOUS` appears nowhere, and there is no
plaintext trust boundary to argue about:

| Listener | Protocol | Principal |
|---|---|---|
| `CONTROLLER` | `SASL_PLAINTEXT` + `sasl.mechanism.controller.protocol=PLAIN` (static JAAS) | `automq-controller` |
| `INTERNAL` | `SASL_PLAINTEXT` + `sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512` | `automq-broker` |
| `EXTERNAL` | `SASL_SSL`, `SCRAM-SHA-512` | `automq` (client), `automq-admin` (tooling) |

The controller listener is the one place where the mechanism differs, and the
reasoning went through three positions before landing. Plaintext with
`ANONYMOUS` as a superuser was too weak. SCRAM was then proposed on the grounds
that genesis `--add-scram` records break the metadata-log bootstrap cycle — but
controller-quorum SCRAM is not a safe load-bearing assumption on this version
(KAFKA-15513 reports exactly this failure, and Kafka's own test harness still
marks controller-quorum SCRAM unsupported).

So the controller listener uses **SASL/PLAIN with a static, root-only JAAS file**
— identical on every node, generated create-once, never read from the metadata
log. The point is not that PLAIN is stronger than SCRAM; it is that PLAIN has
**no bootstrap dependency at all**, which makes it correct whether or not
controller SCRAM works in 3.9. The transport is confined to the VPC, and no
runtime fallback decides the security posture: a converge whose configuration
depends on what happens when it runs is not desired state.

Outbound identity is configured, not assumed: creating a credential does not tell
a broker who to log in *as*. Each node renders listener-scoped JAAS into a
root-only 0600 file:

- `listener.name.internal.scram-sha-512.sasl.jaas.config` — a `ScramLoginModule`
  carrying `automq-broker`'s username and password.
- `listener.name.controller.plain.sasl.jaas.config` — a `PlainLoginModule`
  carrying **both directions**, because PLAIN has no credential store behind it:
  the outbound `username`/`password`, and the inbound
  `user_automq-controller="<password>"` entry that lets a peer authenticate.
  Accompanied by `listener.name.controller.sasl.enabled.mechanisms=PLAIN`, which
  `sasl.mechanism.controller.protocol` does not imply.

The public principal `automq` is not a superuser. Its exact ACL set:
`Topic:PREFIXED:colors-` → `Describe, Read, Write`; `Group:PREFIXED:colors-` →
`Describe, Read`; no `Create`, no `Alter`, no `ClusterAction`, no
`TransactionalId`. Acceptance asserts both an allow (produce/consume on
`colors-acceptance`) and a deny (a cluster-level operation, and a topic outside
the prefix).

**6g. Broker configuration.** `server.properties` per node:
`process.roles=broker,controller`, `node.id=<index>`,
`controller.listener.names=CONTROLLER` (mandatory in KRaft; not implied by the
protocol map), `controller.quorum.voters=0@<vpc0>:9093,…`,
`listeners=CONTROLLER://<vpc>:9093,INTERNAL://<vpc>:9094,EXTERNAL://0.0.0.0:9092`,
`advertised.listeners=INTERNAL://<vpc>:9094,EXTERNAL://b<i>.<host>:9092`,
`inter.broker.listener.name=INTERNAL`,
`listener.security.protocol.map=CONTROLLER:SASL_PLAINTEXT,INTERNAL:SASL_PLAINTEXT,EXTERNAL:SASL_SSL`,
`sasl.mechanism.controller.protocol=PLAIN`,
`listener.name.controller.sasl.enabled.mechanisms=PLAIN`,
`sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512`,
listener-scoped `listener.name.controller.plain.sasl.jaas.config` (static
`PlainLoginModule`, root-only 0600) and
`listener.name.internal.scram-sha-512.sasl.jaas.config`,
`sasl.enabled.mechanisms=SCRAM-SHA-512`,
`listener.name.external.sasl.enabled.mechanisms=SCRAM-SHA-512`,
`listener.name.external.ssl.keystore.type=PKCS12`,
`listener.name.external.ssl.keystore.location=/etc/automq/tls/keystore.p12`,
`listener.name.external.ssl.keystore.password=…`,
`listener.name.external.ssl.key.password=…`, `ssl.client.auth=none`,
RF=1 for all internal topics, and the S3 wiring:
`s3.data.buckets=0@s3://automq-data?region=auto&endpoint=…&pathStyle=true`,
`s3.ops.buckets=1@s3://automq-ops?…`,
`s3.wal.path=0@s3://automq-data?…&batchInterval=250&maxBytesInBatch=8388608`.
`KAFKA_S3_ACCESS_KEY`/`KAFKA_S3_SECRET_KEY` reach the container as literal
`{{ lookup('env', 'COLORS_PAR_AUTOMQ_R2_*') }}` expressions.

**6h. Operator commands.** `automq-status` reports health only — quorum voters
and leader, offline/under-replicated partitions, JVM heap, and certificate expiry
in days, all read from live APIs. Counts derived from the broker log (S3 request
failures, authentication failures) are labelled **best-effort diagnostics**: logs
rotate, reset on restart, and can double-count, so they are not health accounting
and the plan does not pretend otherwise. `automq-credential` is a separate
root-only command that prints the password on explicit invocation, so the routine
command cannot leak it into scrollback or automation logs.

`automq-rotate` performs an **atomic replace** and says so: a SCRAM upsert for the
same principal and mechanism overwrites the credential immediately, so there is no
"prove the new one, then remove the old one" for a single principal — the earlier
plan claimed a sequence that Kafka cannot provide. Rotation therefore disconnects
existing clients, verifies the new credential authenticates afterwards, and fails
loudly if it does not. Zero-downtime rotation via a second versioned principal is
documented as an operator procedure, not automated: this deployment has one client
principal, and automating a migration dance for it would be ceremony.

### 7. Acceptance — what proves it, not what exits zero

On-host (`automq-smoke`, gates `.colors-ready`), over the INTERNAL listener:

1. Three brokers in cluster metadata.
2. `kafka-metadata-quorum describe --status`: 3 voters, one leader.
3. Topic `colors-acceptance` (RF=1, 6 partitions); N records produced and
   consumed back with an exact match.
4. Objects under `automq-data` — proof the storage tier is R2, not disk.
5. `automq-ops` receives objects.
6. Wrong SCRAM password refused; unauthenticated connection refused; the
   `automq` principal denied a cluster-level operation.

From the workstation (`acceptance.sh` — the operator path Ansible cannot prove):

7. TLS chain validates for the bootstrap name and each broker name.
8. `kcat` over `SASL_SSL` produces and consumes through the public endpoint.
9. **Targeted failover.** Discover a partition whose leader is node 2; produce
   keyed records that hash to exactly that partition; stop node 2's container;
   assert the partition becomes writable again and no record is lost or
   duplicated; record the recovery interval. A generic round trip is not accepted
   as evidence — unkeyed records to six partitions can pass without ever touching
   the failed broker. On restart, "rejoined" means three things measured, not a
   voter list entry: the broker re-registers, quorum replication lag is bounded,
   and its high-watermark matches the leader's. A static voter stays listed even
   while it is dead, so the voter list alone proves nothing.
10. **Consumer-group survival during the same outage:** a group with committed
    offsets on `__consumer_offsets` partitions led by node 2 rebalances and
    resumes from its committed position. RF=1 makes those partitions
    unavailable until reassignment, and the measured window is documented rather
    than assumed away.
11. ACLs are proven in both directions: the `automq` principal produces and
    consumes on `colors-acceptance`, and is **denied** a cluster-level operation
    and a topic outside the `colors-` prefix.
12. p99 produce latency from `kafka-producer-perf-test.sh`, reported not
    asserted — the cost of R2 living one provider away.
13. **Controller authentication survives a restart:** restart a controller and
    assert the quorum re-forms and it re-authenticates — the gate that would
    catch a controller-listener mechanism that only appears to work at genesis.
14. Idempotency is defined, not asserted by feel: a second converge reports
    `changed=0` for every configuration task, all probes are read-only, and
    first / second / interrupted-then-resumed converges are each exercised.

## Key decisions & tradeoffs

- **RF=1**, justified from AutoMQ's shared-storage model — every byte is in R2
  before an ack, and upstream ships RF=1 defaults. The earlier "triple write
  amplification" rationale is withdrawn as unverified: AutoMQ documents
  replica-related settings as unnecessary, which is a different and sufficient
  argument. What RF=1 does *not* buy is availability, hence gates 9 and 10.
- **One issuer, one token.** Node 0 alone runs ACME and holds the Cloudflare
  token; a compromise of the two other public brokers cannot touch DNS. Costs a
  key distribution step.
- **SCRAM at genesis only.** Credentials live in the replicated metadata log;
  injecting bootstrap records into a replacement voter would be inconsistent.
- **Authentication is not authorization** — StandardAuthorizer, an enumerated
  ACL set, and a public principal that is not a superuser, since 9092 is open to
  the internet. Every listener authenticates as its own principal — controller,
  inter-broker, admin, client — and `ANONYMOUS` is granted nothing anywhere.
- **Buckets adopted, never created**, with whole-bucket emptiness verification
  and conditional-create ownership.
- **Static `controller.quorum.voters`** — three fixed nodes are desired state,
  and static voters keep the goldens meaningful.

## Assumptions

The confirmed 17-entry ledger with sources is in `PLAN-REVIEW-LOG.md`. The
load-bearing ones: AutoMQ has no configurable object-path prefix (verified in
`BucketURI`/`ObjectUtils` at tag 1.7.4); the R2 token can neither create nor list
buckets but *can* list objects (verified live); both buckets exist and are empty
in EEUR; `vc2-4c-8gb` is available in `ams` at $40/mo; the Cloudflare token edits
exactly `bigconfig.online`.

## Risks / open questions

1. **`region=auto`** passed to `Region.of(...)`. Fall back to `us-east-1`, which
   R2 also accepts, if signing rejects it.
2. **Cross-provider WAL latency** on the ack path; mitigated by
   `batchInterval=250`, measured by gate 11.
3. **8 GiB per node** across heap, direct memory and page cache. If the perf test
   OOMs the broker, the direct-memory budget comes down before the plan reaches
   for a bigger plan.
4. **Container restart semantics** — whether AutoMQ tolerates stop/start or needs
   `--force-recreate` like neon's compute node; gate 9 is where this surfaces.
5. **`If-None-Match: *` on R2** — conditional create is documented as supported;
   if it is not honoured, adoption falls back to refusing any non-empty bucket
   plus operator-provisioned ownership, and the race is documented.

## Out of scope

Tri-colour red/blue ports; the AutoMQ web console; Kafka Connect, Schema
Registry, table topics; **transactional workloads** — `__transaction_state` is
RF=1 like every other internal topic, its behaviour across a broker outage is
neither tested nor claimed, and the honest statement is that transactions are
unsupported here rather than silently untested; multi-region; automated bucket lifecycle; deleting the
R2 buckets on `delete` (a documented manual purge, since they hold data); and
**metrics export to an external observability stack** — `automq-status` is a
point-in-time health surface, not monitoring, and this deployment ships with no
alerting. That is a stated limitation of a demonstration cluster, not an
oversight.
