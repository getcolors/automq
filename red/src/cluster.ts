// Everything that turns `automq-node-count` into concrete cluster facts.
//
// This module exists because a three-node cluster has far more derived
// identity than a single-node one, and every derivation is a place to be wrong
// in a way no exit code reports: a broker that advertises the wrong name is
// reachable and useless, a quorum string that disagrees between nodes forms no
// quorum at all, and a certificate whose SAN list misses one broker fails only
// for the client that happens to be routed there.
//
// Everything here is a pure function of desired state plus the compute stage's
// outputs, so the whole of it is reachable from the test suite and visible in
// the goldens. Nothing in this file may read the environment, the filesystem,
// or the network.

import type { Opts } from "red/workflow";

export const defaultNodeCount = 3;

export interface Node {
  index: number;
  name: string;
  "broker-name": string;
  ip: string;
  "vpc-ip": string;
  user: string;
  sudoer: string;
}

export function nodeCount(opts: Opts): number {
  const n = opts["automq-node-count"];
  return typeof n === "number" && Number.isInteger(n) ? n : defaultNodeCount;
}

// Node indexes, `0..n-1`. The index is the KRaft `node.id`, the suffix in the
// machine label, and the ordinal in the broker name: one number, so the three
// can never disagree.
export function indexes(opts: Opts): number[] {
  return [...Array(nodeCount(opts)).keys()];
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
// unless desired state overrides it with `vultr-name`.
export function computeName(opts: Opts): string {
  const override = String(opts["vultr-name"] ?? "");
  return override.trim().length === 0 ? String(opts.profile ?? "") : override;
}

// The label of machine `i`. Numbered because there is more than one; the
// standard names the machine after the profile, and the index disambiguates
// without introducing a second naming scheme.
export function machineName(opts: Opts, i: number): string {
  return `${computeName(opts)}-${i}`;
}

export function machineNames(opts: Opts): string[] {
  return indexes(opts).map((i) => machineName(opts, i));
}

// --------------------------------------------------------------------- nodes

// What a credential-free `build` renders in place of a compute output. Fixed
// addresses from the documentation ranges (RFC 5737 / RFC 1918) so a build is
// byte-identical on every workstation and the committed goldens mean something.
export const fallbackNode = {
  ip: "192.0.2.10", "vpc-ip": "10.40.0.10", user: "root", sudoer: "root",
};

export function fallbackNodes(opts: Opts): Node[] {
  return indexes(opts).map((i) => ({
    ...fallbackNode,
    index: i,
    name: machineName(opts, i),
    ip: `192.0.2.${10 + i}`,
    "vpc-ip": `10.40.0.${10 + i}`,
    "broker-name": brokerName(opts, i),
  }));
}

function byIndex(params: Record<string, unknown>[]): Map<number, Record<string, unknown>> {
  return new Map(params.map((p) => [Number(p.index), p]));
}

// The node list the Ansible stage and the templates consume.
//
// `params` is the compute stage's output, a list of maps keyed by index. On a
// build there is none, so the fallbacks stand in. On a real run a missing or
// short list is a hard error rather than a silent partial cluster: rendering a
// two-voter quorum string for a three-node cluster would produce a cluster that
// starts and then cannot elect.
export function nodes(opts: Opts, params?: unknown): Node[] {
  const list = Array.isArray(params) ? params as Record<string, unknown>[] : [];
  if (list.length === 0) return fallbackNodes(opts);
  const found = byIndex(list);
  return indexes(opts).map((i) => {
    const p = found.get(i) ?? {};
    const carried: Record<string, unknown> = {};
    for (const key of ["ip", "vpc-ip", "user", "sudoer"]) {
      if (p[key] !== undefined) carried[key] = p[key];
    }
    return {
      ...fallbackNode,
      index: i,
      name: machineName(opts, i),
      "broker-name": brokerName(opts, i),
      ...carried,
    } as Node;
  });
}

// The error for a compute output that does not cover every index, or that omits
// an address. Returned rather than thrown so the workflow can report it the
// same way it reports every other failure.
export function missingNodeError(opts: Opts, params?: unknown): string | undefined {
  const list = Array.isArray(params) ? params as Record<string, unknown>[] : [];
  if (list.length === 0) return undefined;
  const found = byIndex(list);
  const missing = indexes(opts).filter((i) => {
    const p = found.get(i);
    return !(p && String(p.ip ?? "").trim() !== "" && String(p["vpc-ip"] ?? "").trim() !== "");
  });
  if (missing.length === 0) return undefined;
  return `the compute stage did not report an address for node${missing.length > 1 ? "s" : ""} ` +
    `${missing.join(", ")}. Refusing to render a partial cluster: a quorum string that ` +
    "names fewer voters than the cluster has will start and then " +
    "fail to elect a controller.";
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
