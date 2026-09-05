// Desired-state, credential, tool, and Vultr validation.
//
// Green renders its keys as Clojure keywords, so every message here carries the
// same leading colon — the three colours must report identical errors for one
// colors.yml.

import { parName } from "red/cli";
import { runtime, type ExecResult } from "red/runtime";
import type { Opts } from "red/workflow";
import { compute, computeCluster, providers } from "package-once-red";
import * as cluster from "./cluster.ts";
import { onceSsh } from "./once.ts";

export const profilePar = parName("profile");

// The registry and the spec live in `cluster`, which every node derivation
// needs and which this module already depends on for the principals; they are
// named here too so the lifecycle reads them from the validator, as the other
// delegating packages do.
export const computeProviders = cluster.computeProviders;
export const defaultComputeProvider = cluster.defaultComputeProvider;
export const spec = cluster.spec;

// Every key desired state must carry whichever provider is selected. The
// provider-scoped keys come from `computeProviders`.
//
// `vultr-ssh-keys` is deliberately absent: per the SSH Keypair Standard its
// *absence* selects keygen mode, and requiring it would make a conforming
// deployment invalid. `vultr-name` is absent for the same shape of reason — the
// Compute Name Standard makes the profile the default and the key only an
// override (§2, §5).
export const required = [
  "profile", "workdir", "provider-compute", "provider-dns", "provider-backend",
  "compute-prevent-destroy",
  "automq-image", "automq-node-count", "automq-cluster-id",
  "automq-host", "automq-broker-name-prefix",
  "automq-letsencrypt-email", "automq-lego-version",
  "automq-kafka-port", "automq-internal-port", "automq-controller-port",
  "automq-sasl-user", "automq-sasl-mechanism", "automq-heap-opts",
  "automq-data-r2-bucket", "automq-ops-r2-bucket",
  "automq-r2-endpoint", "automq-r2-region",
  "automq-wal-batch-interval-ms", "automq-wal-max-bytes-in-batch",
  "r2-bucket", "r2-endpoint",
];

const hostRe = /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$/;
const emailRe = /^[^@\s]+@[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$/;
const imageRe = /^[^\s:@]+(?:\/[^\s:@]+)*(?::[^\s:@]+)?(?:@sha256:[0-9a-f]{64})?$/;
const digestRe = /@sha256:[0-9a-f]{64}$/;
const bucketRe = /^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$/;
const endpointRe = /^https:\/\/[a-z0-9.-]+(?::\d+)?\/?$/;
const prefixRe = /^[a-z][a-z0-9-]{0,15}$/;
// kafka-storage.sh random-uuid: a UUID in unpadded URL-safe base64.
const clusterIdRe = /^[A-Za-z0-9_-]{22}$/;
const principalRe = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;

export function missing(value: unknown): boolean {
  return value === null || value === undefined ||
    (typeof value === "string" && value.trim() === "");
}

function isInteger(value: unknown): value is number {
  return typeof value === "number" && Number.isInteger(value);
}

// Whether this deployment owns its machine keypair. Delegates to ONCE, the
// standard's reference implementation, so one rule decides it everywhere.
export function keygen(opts: Opts): boolean {
  return onceSsh.keygen(opts);
}

export function envErrors(env: Record<string, string | undefined>): string[] {
  return String(env[profilePar] ?? "").length
    ? [`${profilePar} is set; profile must come from colors.yml only`]
    : [];
}

function port(value: unknown): boolean {
  return isInteger(value) && value >= 1 && value <= 65535;
}

// Every problem with desired state at once: the missing keys (this package's
// and the selected provider's), the package's own checks, then the Compute
// Cluster Standard's — selection, the source lists, the provider rules, the
// created network's CIDR and the topology — which are ONCE's over `spec`.
export function stateErrors(opts: Opts): string[] {
  const errors: string[] = [];
  for (const key of [...required, ...compute.requiredKeys(spec, opts)]) {
    if (missing(opts[key])) errors.push(`:${key} is required`);
  }
  if (opts["provider-dns"] !== "cloudflare") errors.push(":provider-dns must be cloudflare");
  if (!["local", "s3", "r2"].includes(String(opts["provider-backend"]))) {
    errors.push(":provider-backend must be local, s3, or r2");
  }
  // A boolean, not `true`. The guard is lifted for exactly one run by
  // COLORS_PAR_COMPUTE_PREVENT_DESTROY=false, which arrives through the same
  // overlay as every other parameter — so demanding `true` here would reject
  // the override before the delete-time guard could honour it, and the
  // documented way to destroy this deployment would not work at all. What must
  // stay true is the value COMMITTED to colors.yml, and that is a review rule
  // rather than something validation can see.
  if (typeof opts["compute-prevent-destroy"] !== "boolean") {
    errors.push(":compute-prevent-destroy must be true or false");
  }

  // --- cluster shape
  // An even count is not merely unusual, it is worse than the odd count below
  // it: four voters tolerate one failure, exactly as three do, while adding a
  // node that can fail. One node is allowed because it is a legitimate
  // development shape, but it is not a quorum.
  const count = opts["automq-node-count"];
  if (!missing(count)) {
    if (!isInteger(count)) errors.push(":automq-node-count must be an integer");
    else if (!(count >= 1 && count <= 9)) errors.push(":automq-node-count must be from 1 to 9");
    else if (count % 2 === 0 && count > 1) {
      errors.push(":automq-node-count must be odd: an even quorum tolerates no more failures than the odd size below it");
    }
  }
  if (!missing(opts["automq-cluster-id"]) && !clusterIdRe.test(String(opts["automq-cluster-id"]))) {
    errors.push(":automq-cluster-id must be a 22-character base64 UUID as produced by `kafka-storage.sh random-uuid`");
  }
  if (!missing(opts["automq-host"]) && !hostRe.test(String(opts["automq-host"]))) {
    errors.push(":automq-host must be a fully qualified hostname");
  }
  if (!missing(opts["automq-broker-name-prefix"]) &&
      !prefixRe.test(String(opts["automq-broker-name-prefix"]))) {
    errors.push(":automq-broker-name-prefix must be a short lowercase label");
  }
  if (!missing(opts["automq-letsencrypt-email"]) &&
      !emailRe.test(String(opts["automq-letsencrypt-email"]))) {
    errors.push(":automq-letsencrypt-email must be an email address");
  }

  // --- image
  if (!missing(opts["automq-image"]) && !imageRe.test(String(opts["automq-image"]))) {
    errors.push(":automq-image must be a container image reference");
  }
  // This package owns its unit and configuration templates rather than running
  // an upstream installer, so nothing tells it when a floating tag moves
  // underneath it. A digest is what turns a silent retag into a failure at pull
  // time instead of a behaviour change at run time.
  if (!missing(opts["automq-image"]) && !digestRe.test(String(opts["automq-image"]))) {
    errors.push(":automq-image must be pinned by digest (…@sha256:…)");
  }

  // --- listeners
  const portKeys = ["automq-kafka-port", "automq-internal-port", "automq-controller-port"];
  for (const key of portKeys) {
    if (!missing(opts[key]) && !port(opts[key])) {
      errors.push(`:${key} must be an integer from 1 to 65535`);
    }
  }
  const ports = portKeys.map((key) => opts[key]).filter((value) => value !== undefined && value !== null);
  if (ports.length === 3 && new Set(ports).size !== 3) {
    errors.push(":automq-kafka-port, :automq-internal-port and :automq-controller-port must differ");
  }
  if (!missing(opts["automq-sasl-mechanism"]) &&
      opts["automq-sasl-mechanism"] !== "SCRAM-SHA-512") {
    errors.push(":automq-sasl-mechanism must be SCRAM-SHA-512");
  }
  // Four principals share one namespace in the metadata log, and two that
  // collide would silently merge authorities — the client principal is ACL
  // scoped and the others are superusers, so a collision is a privilege
  // escalation rather than a naming annoyance.
  const principals: Array<[string, string]> = [
    ["automq-sasl-user", cluster.clientUser(opts)],
    ["automq-admin-user", cluster.adminUser(opts)],
    ["automq-broker-user", cluster.brokerUser(opts)],
    ["automq-controller-user", cluster.controllerUser(opts)],
  ];
  for (const [key, value] of principals) {
    if (!principalRe.test(value)) {
      errors.push(`:${key} must be a safe 1-64 character principal name`);
    }
  }
  const users = principals.map(([, value]) => value);
  if (new Set(users).size !== users.length) {
    errors.push("the client, admin, broker and controller principals must all differ");
  }

  // --- object storage
  for (const key of ["automq-data-r2-bucket", "automq-ops-r2-bucket"]) {
    if (!missing(opts[key]) && !bucketRe.test(String(opts[key]))) {
      errors.push(`:${key} must be a valid bucket name`);
    }
  }
  // AutoMQ addresses the two roles by distinct bucket ids and writes different
  // key layouts under each; it also supports no path prefix at all, so one
  // bucket cannot host both roles side by side.
  if (!missing(opts["automq-data-r2-bucket"]) &&
      opts["automq-data-r2-bucket"] === opts["automq-ops-r2-bucket"]) {
    errors.push(":automq-data-r2-bucket and :automq-ops-r2-bucket must be different buckets");
  }
  // The state bucket is the operator's, holds every deployment's tfstate, and
  // AutoMQ writes hash-prefixed keys at the bucket root. Sharing them is not a
  // style question.
  for (const key of ["automq-data-r2-bucket", "automq-ops-r2-bucket"]) {
    if (!missing(opts[key]) && String(opts[key]) === String(opts["r2-bucket"])) {
      errors.push(`:${key} must not be the OpenTofu state bucket: AutoMQ writes keys at the bucket root`);
    }
  }
  if (!missing(opts["automq-r2-endpoint"]) && !endpointRe.test(String(opts["automq-r2-endpoint"]))) {
    errors.push(":automq-r2-endpoint must be an https endpoint URL");
  }
  const interval = opts["automq-wal-batch-interval-ms"];
  if (!(missing(interval) || (isInteger(interval) && interval >= 1 && interval <= 60000))) {
    errors.push(":automq-wal-batch-interval-ms must be an integer from 1 to 60000");
  }
  const batch = opts["automq-wal-max-bytes-in-batch"];
  if (!(missing(batch) || (isInteger(batch) && batch > 0))) {
    errors.push(":automq-wal-max-bytes-in-batch must be a positive integer");
  }

  // --- compute: the Compute Cluster Standard's checks are ONCE's over the
  // spec — selection, the source lists, the Vultr os id and name rules, the
  // canonical VPC CIDR, and the node count as a positive integer.
  errors.push(...computeCluster.stateErrors(spec, opts));
  return errors;
}

export function backendSecrets(opts: Opts): string[] {
  return providers["provider-backend"]?.[String(opts["provider-backend"])]?.secrets ?? [];
}

// What talking to Cloudflare needs, on any real event. The compute provider's
// credential comes from the registry.
export const dnsSecrets = ["cloudflare-api-token"];

// What converging the cluster needs, and therefore only a create. Every SASL
// password, the keystore password, and the SCRAM salts are generated on the
// hosts and are never supplied by the operator.
export const applicationSecrets = [
  "automq-r2-access-key-id", "automq-r2-secret-access-key",
];

// Credentials a real event needs: the selected compute provider's,
// Cloudflare's, the backend's, and on a create the storage keys. A delete tears
// down infrastructure and never converges anything, so it asks for the provider
// credentials only; demanding the storage keys to destroy machines would be a
// lock on the exit.
export function secretErrors(opts: Opts, event: string): string[] {
  const keys = [...new Set([
    ...compute.secrets(spec, opts),
    ...dnsSecrets,
    ...(event === "create" ? applicationSecrets : []),
    ...backendSecrets(opts),
  ])];
  return keys.filter((key) => missing(opts[key]))
    .map((key) => `required credential is not set: ${parName(key)}`);
}

export function tofuEnv(opts: Opts, slot: string): Record<string, string> {
  switch (slot) {
    case "provider-compute":
      return compute.tofuEnv(spec, opts);
    case "provider-dns":
      return { "cloudflare-api-token": "CLOUDFLARE_API_TOKEN" };
    case "provider-backend":
      return providers["provider-backend"]?.[String(opts["provider-backend"])]?.tofuEnv ?? {};
    default:
      return {};
  }
}

// ------------------------------------------------------------ runtime checks

export const requiredTools = ["tofu", "ansible-playbook", "ssh", "curl", "openssl"];

export type Runner = (
  cmd: string[],
  options?: { env?: Record<string, string | undefined>; timeoutMs?: number },
) => Promise<ExecResult>;

async function commandPresent(runner: Runner, command: string): Promise<boolean> {
  const result = await runner(["sh", "-c", 'command -v "$1" >/dev/null 2>&1', "sh", command], {});
  return result.exit === 0;
}

export const accountUrl = "https://api.vultr.com/v2/account";

// Turn one probe of the Vultr account endpoint into an error, or undefined.
//
// The distinction is the point. A single message covering every non-2xx status
// reports a Vultr outage as a bad credential and sends the operator off to
// rotate a key that was never the problem. Only 401 and 403 say anything about
// the key. A request that never reached the API at all shows up as curl's
// literal `000`, which is not an HTTP status: that is the operator's network,
// and naming it saves the same wasted rotation.
export function apiError(result: { exit: number; out?: string }): string | undefined {
  const match = /\d{3}$/.exec(String(result.out ?? "").trim());
  const status = match ? Number(match[0]) : undefined;
  if (status === undefined || status === 0) {
    return `could not reach the Vultr API at ${accountUrl} (curl exit ${result.exit}): ` +
      "this is a local network, DNS, or TLS failure, not a credential problem. " +
      "Check connectivity and retry.";
  }
  if (status >= 200 && status <= 299) return undefined;
  if (status === 401 || status === 403) {
    return `Vultr rejected COLORS_PAR_VULTR_API_KEY (HTTP ${status}): the key is ` +
      "missing, revoked, or its allowed-subnet list does not include this machine. " +
      "Check the key in the Vultr console and update .envrc.private.";
  }
  if (status === 429) {
    return "Vultr rate-limited the credential check (HTTP 429). The key is valid; " +
      "wait for the limit to reset and retry.";
  }
  if (status >= 500 && status <= 599) {
    return `the Vultr API returned HTTP ${status} for ${accountUrl}. That is a failure ` +
      "on Vultr's side, not your credential — do not rotate COLORS_PAR_VULTR_API_KEY. " +
      "Check https://status.vultr.com and retry.";
  }
  return `unexpected HTTP ${status} from ${accountUrl} during the credential check.`;
}

// Check local tools and authenticate the configured Vultr key. The runner
// argument keeps command decisions testable without network access.
export async function runtimeErrors(opts: Opts, runner: Runner = runtime.exec): Promise<string[]> {
  const present = new Map<string, boolean>();
  for (const tool of requiredTools) {
    present.set(tool, await commandPresent(runner, tool));
  }
  const errors = requiredTools.filter((tool) => !present.get(tool))
    .map((tool) => `required tool is not on PATH: ${tool}`);
  const key = opts["vultr-api-key"];
  // No `-f`: the status code is the diagnosis, so it has to survive into
  // stdout instead of collapsing into curl's exit code.
  const result = !missing(key) && present.get("curl")
    ? await runner(["curl", "-sS", "-o", "/dev/null", "-w", "%{http_code}",
        "--connect-timeout", "10", "--max-time", "20",
        "-H", `Authorization: Bearer ${key}`, accountUrl], {})
    : undefined;
  const probe = result ? apiError(result) : undefined;
  return probe ? [...errors, probe] : errors;
}
