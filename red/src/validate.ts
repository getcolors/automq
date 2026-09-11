// Application requirements and delegated compute validation.
//
// Green renders its keys as Clojure keywords, so every message here carries the
// same leading colon — the three colours must report identical errors for one
// colors.yml.

import {providers as onceProviders} from "package-once-red";
import { parName } from "red/cli";
import { runtime, type ExecResult } from "red/runtime";
import type { Opts } from "red/workflow";
import {registry,validate as computeValidate,credential_requirements,plan_deployment,keyMode} from "colors-compute-red";
import * as cluster from "./cluster.ts";


export const profilePar = parName("profile");

// The registry and the spec live in `cluster`, which every node derivation
// needs and which this module already depends on for the principals; they are
// named here too so the lifecycle reads them from the validator, as the other
// delegating packages do.
export const computeProviders = registry.compute;
export const defaultComputeProvider = cluster.defaultComputeProvider;


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

// The compute library decides whether the deployment owns its machine key.
export function keygen(opts: Opts): boolean {
  return keyMode(opts).mode==='managed';
}

export function envErrors(env: Record<string, string | undefined>): string[] {
  return String(env[profilePar] ?? "").length
    ? [`${profilePar} is set; profile must come from colors.yml only`]
    : [];
}

function port(value: unknown): boolean {
  return isInteger(value) && value >= 1 && value <= 65535;
}

// Application checks run alongside the shared provider and rendering contracts.
export function stateErrors(opts: Opts): string[] {
  const errors: string[] = [];
  for (const key of [...required, ...(opts["provider-backend"] === "r2" ? ["r2-bucket", "r2-endpoint"] : opts["provider-backend"] === "s3" ? ["s3-bucket", "s3-region"] : opts["provider-backend"] === "gcs" ? ["gcs-bucket", "gcs-region"] : [])]) {
    if (missing(opts[key])) errors.push(`:${key} is required`);
  }
  if (!["cloudflare", "none"].includes(opts["provider-dns"])) errors.push(":provider-dns must be cloudflare or none");
  if (!["acme", "private-ca"].includes(opts["automq-tls-mode"] ?? "acme")) errors.push(":automq-tls-mode must be acme or private-ca");
  if (opts["provider-dns"] === "none" && opts["automq-tls-mode"] !== "private-ca") errors.push(":provider-dns none requires :automq-tls-mode private-ca");
  if (opts["automq-tls-mode"] === "private-ca" && opts["provider-dns"] !== "none") errors.push(":automq-tls-mode private-ca requires :provider-dns none");
  if ("automq-apt-security-mirror" in opts && !/^https?:\/\/[a-z0-9.-]+(?::[0-9]+)?\/[A-Za-z0-9._~/-]+$(?![\s\S])/.test(String(opts["automq-apt-security-mirror"]))) errors.push(":automq-apt-security-mirror must be an HTTP or HTTPS repository URL");
  if (!["s3", "r2", "gcs", "oci"].includes(String(opts["provider-backend"]))) {
    errors.push(":provider-backend must be s3, r2, gcs or oci");
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
  if ("automq-storage-managed" in opts && typeof opts["automq-storage-managed"] !== "boolean") errors.push(":automq-storage-managed must be true or false");
  if (opts["automq-storage-managed"] && !["s3", "gcs", "oci"].includes(String(opts["automq-storage-provider"]))) errors.push("managed storage requires :automq-storage-provider s3, gcs or oci");
  if ("automq-oci-user-email" in opts && !emailRe.test(String(opts["automq-oci-user-email"]))) errors.push(":automq-oci-user-email must be an email address unique to the OCI service user");
  if (opts["automq-storage-managed"] && opts["automq-storage-provider"] === "oci" && (["oci-tenancy-id","oci-compartment-id","oci-namespace","oci-config-file-profile","automq-r2-region","automq-oci-user-email"].some(key=>missing(opts[key])) || opts["automq-r2-region"] === "auto" || opts["automq-r2-endpoint"] !== `https://${opts["oci-namespace"]}.compat.objectstorage.${opts["automq-r2-region"]}.oraclecloud.com`)) errors.push("managed OCI storage requires tenancy, compartment, namespace, config profile, unique user email, region and the matching OCI compatibility endpoint");
  if (opts["automq-storage-managed"] && opts["automq-storage-provider"] === "gcs" && (missing(opts["google-project"]) || opts["automq-r2-endpoint"] !== "https://storage.googleapis.com")) errors.push("managed GCS storage requires :google-project and :automq-r2-endpoint https://storage.googleapis.com");
  if (opts["automq-storage-managed"] && opts["automq-storage-provider"] === "s3" && opts["automq-r2-region"] === "auto") errors.push("managed S3 storage requires an AWS region in :automq-r2-region");
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
    if (!missing(opts[key]) && String(opts[key]) === String(opts[`${opts["provider-backend"]}-bucket`])) {
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

  errors.push(...computeValidate(opts));
  if(!errors.length)try{plan_deployment(opts,cluster.topology(opts),cluster.requirements(opts));}catch(error){errors.push(String((error as Error).message));}
  return errors;
}

export function backendSecrets(opts: Opts): string[] {
  return (registry.backend as any)[String(opts["provider-backend"])]?.secrets ?? [];
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
    ...(event==='validate'?credential_requirements(opts).map(name=>name.replace(/^COLORS_PAR_/,'').toLowerCase().replaceAll('_','-')):[]),
    ...(opts["provider-dns"] === "none" ? [] : dnsSecrets),
    ...(event === "create" && !opts["automq-storage-managed"] ? applicationSecrets : []),
    ...backendSecrets(opts),
  ])];
  return keys.filter((key) => missing(opts[key]))
    .map((key) => `required credential is not set: ${parName(key)}`);
}

export function tofuEnv(opts:Opts,slot:string):Record<string,string>{return slot==='provider-dns'?{'cloudflare-api-token':'CLOUDFLARE_API_TOKEN'}:slot==='provider-backend'?(onceProviders['provider-backend']?.[String(opts['provider-backend'])]?.tofuEnv??{}):{};}

// ------------------------------------------------------------ runtime checks

export const requiredTools = ["tofu", "aws", "ansible-playbook", "ssh", "ssh-keygen", "curl", "openssl"];

export type Runner = (
  cmd: string[],
  options?: { env?: Record<string, string | undefined>; timeoutMs?: number },
) => Promise<ExecResult>;

async function commandPresent(runner: Runner, command: string): Promise<boolean> {
  const result = await runner(["sh", "-c", 'command -v "$1" >/dev/null 2>&1', "sh", command], {});
  return result.exit === 0;
}

export async function runtimeErrors(opts:Opts,runner:Runner=runtime.exec):Promise<string[]> {
 const errors:string[]=[];for(const tool of [...requiredTools,...(opts["automq-storage-provider"] === "gcs" ? ["gcloud"] : opts["automq-storage-provider"] === "oci" ? ["oci"] : [])])if(!await commandPresent(runner,tool))errors.push('required tool is not on PATH: '+tool);return errors;
}
