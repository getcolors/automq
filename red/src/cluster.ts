// Everything that turns `automq-node-count` into concrete cluster facts.
//
// This module exists because a three-node cluster has far more derived
// identity than a single-node one, and every derivation is a place to be wrong
// in a way no exit code reports: a broker that advertises the wrong name is
// reachable and useless, a quorum string that disagrees between nodes forms no
// quorum at all, and a certificate whose SAN list misses one broker fails only
// for the client that happens to be routed there.
//
// The node set itself — how many nodes, their ids, the fallback addresses a
// `build` renders with, and the refusal of a state that does not describe the
// whole cluster — is the Compute Cluster Standard's
// (`workspace/standards/compute-cluster.md`) and is ONCE's `computeCluster`
// module, called with the `spec` below and never copied. What stays here is
// AutoMQ's: broker names, the SAN list, the quorum string, listeners,
// principals and ACLs.
//
// Everything here is a pure function of desired state plus the compute stage's
// outputs, so the whole of it is reachable from the test suite and visible in
// the goldens. Nothing in this file may read the environment, the filesystem,
// or the network.

import type { Opts } from "red/workflow";
import { compute, computeCluster } from "package-once-red";

// ---------------------------------------------------------------- the spec

// provider-compute -> what that choice implies.
//
// `required` are the non-secret keys the provider's template interpolates,
// `secrets` the credentials it needs through COLORS_PAR_*, `tofuEnv` the
// subset OpenTofu reads from the process environment itself, and `network` the
// private network the cluster's quorum crosses — created by this package from
// `vultr-vpc-subnet`, never discovered. Keeping them together is what stops a
// provider being validated against one set of keys and run with another. The
// keys of this map are the advertised providers; Vultr is the only one this
// package has a template and a golden for.
//
// Two keys the template reads are deliberately not required. `vultr-name` is
// an optional override of the profile (Compute Name Standard), and
// `vultr-ssh-keys` is meaningful by its absence (SSH Keypair Standard).
export const computeProviders: computeCluster.ClusterRegistry = {
  vultr: {
    required: ["vultr-region", "vultr-plan", "vultr-os-id", "vultr-vpc-subnet",
               "vultr-ssh-sources", "vultr-kafka-sources"],
    secrets: ["vultr-api-key"],
    tofuEnv: { "vultr-api-key": "VULTR_API_KEY" },
    network: { mode: "created", key: "vultr-vpc-subnet" },
  },
};

// The provider a deployment created before this package recorded one in its
// compute output must be running: the only one it ever offered.
export const defaultComputeProvider = "vultr";

export const defaultNodeCount = 3;

// How this package describes itself to ONCE's `computeCluster`. One
// homogeneous role whose count is `automq-node-count` (three by default); the
// bare `<profile>` alias reaches node 0, the default entry. `sources` names the
// firewall lists the template reads — SSH must list at least one CIDR, an empty
// Kafka list means no public Kafka access.
export const spec: computeCluster.ClusterSpec = {
  registry: computeProviders,
  default: defaultComputeProvider,
  sources: { nonEmpty: ["ssh-sources"], mayBeEmpty: ["kafka-sources"] },
  roles: [{ role: null, countKey: "automq-node-count", count: defaultNodeCount }],
};

// ------------------------------------------------------------------- names

// How many nodes the cluster has: `automq-node-count` when desired state
// carries it, else three. ONCE's; validation refuses a present value that is
// not a positive integer before any derivation runs.
export function nodeCount(opts: Opts): number {
  return computeCluster.nodeCount(spec, opts, null) as number;
}

// Node indexes, `0..n-1`. The index is the KRaft `node.id`, the suffix in the
// machine label, and the ordinal in the broker name: one number, so the three
// can never disagree. ONCE's ids are 0-based per role, which is what keeps
// `node.id = index` true.
export function indexes(opts: Opts): number[] {
  return computeCluster.nodeIds(spec, opts).map((id) => id.index);
}

// The public name broker `i` advertises, `b<i>.<automq-host>`.
//
// Kafka redirects a client from the bootstrap name to whatever a broker
// advertises, so this name must resolve publicly and must appear in that
// broker's certificate. Both the DNS stage and the SAN list below derive from
// this one function.
export function brokerName(opts: Opts, i: number): string {
  const prefix = String(opts["automq-broker-name-prefix"] ?? "");
  return `${prefix.length > 0 ? prefix : "b"}${i}.${opts["automq-host"]}`;
}

export function brokerNames(opts: Opts): string[] {
  return indexes(opts).map((i) => brokerName(opts, i));
}

// The exact SAN list: the bootstrap name plus every broker name.
//
// Derived rather than guessed. An earlier design used a wildcard, which
// required deriving the zone from the host and left the apex needing its own
// SAN anyway; enumerating the names this cluster actually serves is both
// shorter and checkable.
export function certificateNames(opts: Opts): string[] {
  return [String(opts["automq-host"]), ...brokerNames(opts)];
}

// The cluster's base machine name (Compute Name Standard §1-2): the profile,
// unless desired state overrides it with `vultr-name`. ONCE's, so every label
// derives from the same value.
export function computeName(opts: Opts): string {
  return compute.computeName(opts);
}

// The label of machine `i`, `<compute-name>-<i>`: the Cluster Standard's
// fallback name for the null role, which is also what the template labels the
// instance. Numbered because there is more than one; the standard names the
// machine after the profile, and the index disambiguates without introducing a
// second naming scheme.
export function machineName(opts: Opts, i: number): string {
  return computeCluster.fallbackNodeName(spec, opts, { role: null, index: i });
}

export function machineNames(opts: Opts): string[] {
  return indexes(opts).map((i) => machineName(opts, i));
}

// --------------------------------------------------------------------- nodes

// A node as this package's renderers read it: ONCE's five fields with `vpc-ip`
// in the package's kebab spelling, plus the broker name it advertises, plus
// whatever else the template recorded.
export interface Node {
  role: string | null;
  index: number;
  name: string;
  ip: string;
  "vpc-ip": string;
  user: string;
  sudoer: string;
  "broker-name": string;
  [extra: string]: unknown;
}

// One of ONCE's nodes as this package's renderers read it: `vpc-ip` in the
// package's kebab spelling — the templates, the inventory and the quorum string
// were written against it, and adapting here keeps every rendered file
// byte-identical — plus the broker name this node advertises.
function automqNode(opts: Opts, node: computeCluster.Node): Node {
  const { vpc_ip, ...rest } = node;
  return { ...rest, "vpc-ip": vpc_ip as string, "broker-name": brokerName(opts, node.index) } as Node;
}

// What a credential-free `build` renders in place of a compute output: ONCE's
// fallbacks — public addresses from `192.0.2.0/24`, private ones cut from
// `vultr-vpc-subnet`, offset 10 — so a build is byte-identical on every
// workstation and the committed goldens mean something.
export function fallbackNodes(opts: Opts): Node[] {
  return computeCluster.fallbackNodes(spec, opts).map((node) => automqNode(opts, node));
}

// The node list the Ansible stage and the templates consume.
//
// `params` is the compute stage's recorded `params` map, adopted under
// `once/cluster` on a real run. On a build there is none, so the fallbacks
// stand in. On a real run ONCE refuses a state that does not describe every
// declared node with every field, and never substitutes a fallback: rendering
// a two-voter quorum string for a three-node cluster would produce a cluster
// that starts and then cannot elect.
export function nodes(opts: Opts, params?: computeCluster.ClusterParams | null): Node[] {
  return computeCluster.nodes(spec, opts, params).map((node) => automqNode(opts, node));
}

// ----------------------------------------------------------------- listeners

export function controllerPort(opts: Opts): number {
  return (opts["automq-controller-port"] as number) ?? 9093;
}

export function internalPort(opts: Opts): number {
  return (opts["automq-internal-port"] as number) ?? 9094;
}

export function kafkaPort(opts: Opts): number {
  return (opts["automq-kafka-port"] as number) ?? 9092;
}

// `controller.quorum.voters`, identical on every node.
//
// Static rather than dynamic: three fixed nodes are desired state, and a static
// list is what makes the rendered configuration deterministic and the goldens
// meaningful. Built from VPC addresses — the quorum never crosses the public
// interface.
export function quorumVoters(opts: Opts, list: Node[]): string {
  return list.map((n) => `${n.index}@${n["vpc-ip"]}:${controllerPort(opts)}`).join(",");
}

// `listeners` for node `n`. CONTROLLER and INTERNAL bind the VPC address
// specifically, which is why the container runs with host networking: a bridged
// container cannot bind an address that belongs only to the host. EXTERNAL
// binds every interface because it is the public endpoint.
export function listeners(opts: Opts, n: Node): string {
  return `CONTROLLER://${n["vpc-ip"]}:${controllerPort(opts)}` +
    `,INTERNAL://${n["vpc-ip"]}:${internalPort(opts)}` +
    `,EXTERNAL://0.0.0.0:${kafkaPort(opts)}`;
}

// What node `n` tells clients to come back to. INTERNAL advertises the VPC
// address; EXTERNAL advertises this broker's own public name, which must
// resolve and must be in its certificate. CONTROLLER is deliberately absent —
// Kafka rejects a controller entry in `advertised.listeners`.
export function advertisedListeners(opts: Opts, n: Node): string {
  return `INTERNAL://${n["vpc-ip"]}:${internalPort(opts)}` +
    `,EXTERNAL://${n["broker-name"]}:${kafkaPort(opts)}`;
}

// ---------------------------------------------------------------- principals

function principal(value: unknown, fallback: string): string {
  const text = String(value ?? "");
  return text.length > 0 ? text : fallback;
}

export const adminUser = (opts: Opts) => principal(opts["automq-admin-user"], "automq-admin");
export const brokerUser = (opts: Opts) => principal(opts["automq-broker-user"], "automq-broker");
export const controllerUser = (opts: Opts) =>
  principal(opts["automq-controller-user"], "automq-controller");
export const clientUser = (opts: Opts) => principal(opts["automq-sasl-user"], "automq");

// The principals bootstrapped into the metadata log by the genesis format.
//
// The controller principal is deliberately absent: it authenticates with PLAIN
// from a static JAAS file, precisely so that forming the controller quorum
// depends on nothing stored in the metadata log the quorum is trying to serve.
export function scramPrincipals(opts: Opts): string[] {
  return [adminUser(opts), brokerUser(opts), clientUser(opts)];
}

// `super.users`. The client principal is never here — it is ACL-scoped, and a
// public endpoint whose only authenticated identity is a superuser is an
// authorization hole with a password on it.
export function superUsers(opts: Opts): string {
  return [adminUser(opts), brokerUser(opts), controllerUser(opts)]
    .map((user) => `User:${user}`).join(";");
}

export function topicPrefix(opts: Opts): string {
  return principal(opts["automq-client-topic-prefix"], "colors-");
}

export interface Acl {
  principal: string;
  "resource-type": string;
  "pattern-type": string;
  name: string;
  operations: string[];
}

// The client principal's complete authority, enumerated so it can be read and
// tested rather than inferred. No Create, no Alter, no ClusterAction, no
// TransactionalId — acceptance asserts the denials as well as the grants.
export function clientAcls(opts: Opts): Acl[] {
  const user = clientUser(opts);
  const prefix = topicPrefix(opts);
  return [
    { principal: user, "resource-type": "topic", "pattern-type": "prefixed",
      name: prefix, operations: ["Describe", "Read", "Write"] },
    { principal: user, "resource-type": "group", "pattern-type": "prefixed",
      name: prefix, operations: ["Describe", "Read"] },
  ];
}
