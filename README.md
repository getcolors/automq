# automq

A green-only [getcolors](https://www.getcolors.ai/) Package Skill that
provisions a three-node [AutoMQ](https://github.com/AutoMQ/automq) cluster on
Vultr: the Kafka 3.9.1 wire protocol, KRaft combined `broker,controller` roles,
and Cloudflare R2 as the storage tier.

The reference deployment is [`automq-vultr`](https://github.com/getcolors/automq-vultr).

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
cp .agents/skills/package-automq-green/green ./green
chmod +x green
./green build              # renders .colors/ — contacts nothing
./green create --dry-run   # walks the DAG with no side effects
```

`build` and `--dry-run` work on a fresh checkout with an empty environment,
which makes them the safe way to check a `colors.yml` edit.

## What it provisions

| Layer | What |
|---|---|
| Compute | Three Vultr instances in one VPC; firewall opens 22 and 9092 only |
| DNS | One A record per node on the bootstrap name, one per broker, all DNS-only |
| TLS | One Let's Encrypt certificate over DNS-01, issued by node 0, covering the bootstrap name and every broker name |
| Storage | Two R2 buckets — data (stream objects and the S3 WAL) and ops |
| Identity | Four SASL principals; no anonymous access on any listener |

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
- **Purging storage.** `delete` deliberately leaves both buckets intact — they
  hold the cluster's data. Empty them by hand, including the
  `_colors/<profile>/` markers, before adopting them again.

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
bb test              # unit tests
bb golden            # two fixtures: keygen and opt-out keypair modes
./scripts/launcher.sh
```

`bb golden:accept` regenerates the committed output — only after reading the
diff. Use `AUTOMQ_LIB_ROOT`, `GREEN_LIB_ROOT` and `ONCE_LIB_ROOT` to develop
across repository boundaries.

## License

MIT.
