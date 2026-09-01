# CLAUDE.md

## Repository

`automq` is a green-only Package Skill for a three-node [AutoMQ](https://github.com/AutoMQ/automq)
cluster on Vultr: Kafka 3.9.1 wire protocol, KRaft combined `broker,controller`
roles, and Cloudflare R2 as the storage tier. OpenTofu manages a VPC, a
firewall that opens **22 and 9092 only**, and the instances; a second tofu
stack manages Cloudflare records; Ansible converges a Compose stack on each
node. The first consumer is `../automq-vultr`.

## The thing to understand before anything else

**Replication factor is 1 everywhere, and that is correct.** AutoMQ's
durability is object storage: a produce is acknowledged after the record is in
R2, not after it is on another broker's disk. Upstream's own
`config/kraft/server.properties` ships RF=1. The three nodes exist for the
controller quorum, for partition failover, and for throughput — not for copies.
A reviewer who "fixes" this to RF=3 adds cost and write amplification and
removes nothing from the risk column.

What RF=1 does *not* buy is availability. A partition whose leader dies is
unavailable until it is reassigned, which is why acceptance kills a broker that
leads a *specific* partition and measures the recovery window rather than
producing to six partitions and hoping.

## Why this package owns its templates

Upstream's `docker/docker-compose.yaml` is a single-node quick start: MinIO,
`node.id=0`, a hard-coded cluster id, PLAINTEXT everywhere. Nothing about it
survives contact with three nodes, TLS, or authentication. This package derives
its configuration from the 1.7.4 tree and maintains it as its own, with the
image pinned by digest in desired state — so when upstream changes shape,
nothing here follows silently. Re-read `config/kraft/server.properties` **at
the pinned tag** when bumping `automq-image`.

## What fails silently here, and the traps already paid for

- **`controller.listener.names` is mandatory in KRaft** and is not implied by
  `listener.security.protocol.map`. Without it the broker does not start.
- **The controller listener cannot use SCRAM.** Its credentials would live in
  the metadata log that the quorum must form to serve — KAFKA-15513 is exactly
  this. CONTROLLER uses PLAIN from a static JAAS entry; INTERNAL and EXTERNAL
  use SCRAM, which is fine because both are used only after the quorum exists.
- **`--add-scram` with a plaintext password salts randomly per invocation.**
  Formatting three voters that way writes three divergent bootstrap records for
  one user. The salt and salted password are computed once and passed in the
  explicit `salt=…,saltedpassword=…` form. Same class of trap as neon's stored
  verifiers: randomness inside a converge destroys determinism.
- **A bridged container cannot bind the host's VPC address**, which the
  CONTROLLER and INTERNAL listeners require. The Compose service uses
  `network_mode: host`.
- **Cloudflare's proxy is HTTP-only.** Every record is `proxied: false`; a
  proxied record publishes an address that speaks HTTP to a Kafka client.
- **Three nodes renewing certificates on identical timers race** on the shared
  `_acme-challenge` TXT record for the bootstrap name, each deleting the
  others' proof. Node 0 is the only issuer, publishes to the ops bucket, and
  the others pull. Restarts are ordered by a conditional-create lease in object
  storage, not by each node's local quorum check — a local check cannot order
  independent actors, and these are combined broker+controller nodes where a
  simultaneous restart destroys the majority.
- **Formatting is authorized by a two-phase record**, `format-intent` then
  `format-complete`. A single record written before the format would make a
  converge that died mid-format indistinguishable from disk loss on the next
  run — and those demand opposite responses.
- **There are two firewalls, and ping cannot see the one that matters.** The
  Vultr Ubuntu image ships **ufw enabled** with a single `22/tcp` rule. It
  passes ICMP, so every node pings every other node while every inter-node TCP
  connection is dropped: the quorum never elects and the broker half dies sixty
  seconds later blaming itself. The provider's firewall group is desired state
  in the compute stage; ufw is converged by the play. Both are required, and
  the provider group alone proves nothing because the host drops the packet
  after it gets through. Test raw TCP across the VPC, never ping.
- **A marker is not evidence.** Genesis is decided by whether any node has a
  **format-complete** record, never by a marker written before the format. An
  earlier design claimed the marker first; the converge that followed failed,
  and every later run formatted without SCRAM bootstrap records — producing a
  cluster that could never authenticate anyone and could not be repaired,
  because `kafka-configs --bootstrap-controller` answers
  `UnsupportedEndpointTypeException`.
- **The secret bundle belongs to the cluster, not to node 0.** It is sourced
  from whichever node still holds it, and generation is refused when any node
  has a format-complete record. Regenerating it on a rebuilt node 0 would push
  new passwords and SCRAM salts over working ones while the metadata log kept
  the old.
- **`docker compose up -d` does not restart on a changed bind-mounted file.**
  Compose compares its own service definition, not the bytes behind the mount,
  so a configuration edit would converge cleanly while the JVM ran the old
  config. A changed render triggers a throttled restart.
- **Gates must pass twice.** They run on every converge against a cluster that
  keeps its data, so anything counting absolute totals — or assuming which node
  leads a partition — passes the first time and fails forever after. Recreate
  the gate's topic and tag records per run.
- **Never run `build` while a converge is in flight.** `.colors/` is live input
  to the running stage; re-rendering it under a running script makes bash
  resume mid-token and report a syntax error in a valid file.
- **Exit codes are not evidence.** `automq-smoke` asks the cluster what it has:
  every broker registered, a quorum with a leader, 500 records back out
  verbatim, objects actually present in both buckets, a wrong password refused,
  and the client principal denied a cluster operation.

## Object storage is adopted, never created

The scoped token cannot create buckets, so both must exist and be **empty**.
AutoMQ writes `<hash>/_kafka_<cluster-id>/…` at the bucket root and supports no
configurable prefix, so it cannot share a bucket with anything — including
another AutoMQ cluster. `store.py` proves emptiness by paginating the whole
bucket, claims ownership with a conditional create, and carries one transaction
id across both buckets so a half-adopted pair is resumable and a mismatched one
is fatal.

## The SSH keypair and `~/.ssh/config`

Born conforming to three workspace standards. Read
`../workspace/standards/ssh-keypair.md` before touching `ssh.clj`,
`../workspace/standards/ssh-config.md` before touching `ssh_config.clj`, and
`../workspace/standards/compute-name.md` for why there is no required
`vultr-name`. The keypair behaviour is ONCE's, reused so one standard has one
implementation; the config block is this package's own copy (§7). The two
disagree on ordering deliberately — the config block is removed *before* the
compute destroy, the keypair *after* it.

This deployment claims `<profile>` and `<profile>-<n>` for each node, and the
adoption check covers all of them.

`build` and `--dry-run` render `/home/build-placeholder/.ssh/<profile>` rather
than reading `~/.ssh`, which is what makes the committed goldens mean the same
thing on every workstation.

## Commands

```sh
bb test              # 44 tests
bb golden            # two fixtures: keygen and opt-out
bb golden:accept     # only after reading the diff
./scripts/launcher.sh
./green build
./green create --dry-run
./green create       # requires explicit authorization
./green delete       # guarded and destructive
```

Never read `.envrc.private`, edit `.colors/`, export `COLORS_PAR_PROFILE`, or
weaken `compute-prevent-destroy`. Build and dry-run are credential-free and
must not touch `~/.ssh`.

## Coupling

Pins Green and ONCE in `deps.edn`; the ONCE pin cannot go below `bc06f2f`,
where the machine keypair moved into the operator's `~/.ssh`. Use
`GREEN_LIB_ROOT`, `ONCE_LIB_ROOT`, and `AUTOMQ_LIB_ROOT` for working-tree
development. Final launchers use a pushed SHA managed by `bb pin`; deployment
launchers are copies, not symlinks.

## Documentation

`index.html` is this repository's landing page and carries two analytics tags:
GA4 measurement ID `G-4VKP1WY4QJ`, whose explicit `page_title` must exactly
equal the decoded HTML `<title>` and stay distinct and stable, and the
self-hosted Rybbit snippet
`<script src="https://rybbit.getcolors.ai/api/script.js" data-site-id="9fb9c41a6d49" defer></script>`.
Never add one tag without the other.

## Git

Work on the current branch. Do not commit or push unless explicitly authorized.
