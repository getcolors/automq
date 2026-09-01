// Compute, DNS, local SSH config, cluster convergence, and acceptance stages.

import * as ansible from "red/ansible";
import { stageDir } from "red/cli";
import { PRESERVE_JINJA_DELIMITERS, contentSpec, scaffold, type Spec, type Template } from "red/scaffold";
import * as tofu from "red/tofu";
import { runtime } from "red/runtime";
import { failed, type Opts } from "red/workflow";
import { registrableDomain } from "package-once-red";
import * as cluster from "./cluster.ts";
import * as sshConfig from "./ssh-config.ts";
import * as validate from "./validate.ts";

import infrastructureMainTf from "../resources/tools/infrastructure/main.tf" with { type: "text" };
import dnsMainTf from "../resources/tools/dns/main.tf" with { type: "text" };
import ansibleLocalCfg from "../resources/tools/ansible-local/ansible.cfg" with { type: "text" };
import ansibleLocalInventory from "../resources/tools/ansible-local/inventory.ini" with { type: "text" };
import ansibleLocalMain from "../resources/tools/ansible-local/main.yml" with { type: "text" };
import ansibleCfg from "../resources/tools/ansible/ansible.cfg" with { type: "text" };
import ansibleMain from "../resources/tools/ansible/main.yml" with { type: "text" };
import ansibleCleanup from "../resources/tools/ansible/cleanup.yml" with { type: "text" };
import ansibleCompose from "../resources/tools/ansible/compose.yml" with { type: "text" };
import ansibleServerProperties from "../resources/tools/ansible/server.properties" with { type: "text" };
import ansibleStore from "../resources/tools/ansible/store.py" with { type: "text" };
import ansibleSecrets from "../resources/tools/ansible/secrets.sh" with { type: "text" };
import ansibleRenderConfig from "../resources/tools/ansible/render-config.sh" with { type: "text" };
import ansibleFormat from "../resources/tools/ansible/format.sh" with { type: "text" };
import ansibleAcl from "../resources/tools/ansible/acl.sh" with { type: "text" };
import ansibleScram from "../resources/tools/ansible/scram.sh" with { type: "text" };
import ansibleCert from "../resources/tools/ansible/cert.sh" with { type: "text" };
import ansibleCertDeploy from "../resources/tools/ansible/cert-deploy.sh" with { type: "text" };
import ansibleCertDeployService from "../resources/tools/ansible/cert-deploy.service" with { type: "text" };
import ansibleCertDeployTimer from "../resources/tools/ansible/cert-deploy.timer" with { type: "text" };
import ansibleCertRenewService from "../resources/tools/ansible/cert-renew.service" with { type: "text" };
import ansibleCertRenewTimer from "../resources/tools/ansible/cert-renew.timer" with { type: "text" };
import ansibleStatus from "../resources/tools/ansible/status.sh" with { type: "text" };
import ansibleCredential from "../resources/tools/ansible/credential.sh" with { type: "text" };
import ansibleSmoke from "../resources/tools/ansible/smoke.sh" with { type: "text" };
import ansibleRotate from "../resources/tools/ansible/rotate.sh" with { type: "text" };
import acceptanceSh from "../resources/tools/acceptance/acceptance.sh" with { type: "text" };

export const infrastructureTool = "automq-infrastructure";
export const dnsTool = "automq-dns";
export const ansibleTool = "automq-ansible";
export const ansibleLocalTool = "automq-ansible-local";
export const acceptanceTool = "automq-acceptance";
export const templateOpts = PRESERVE_JINJA_DELIMITERS;

export function toolDir(opts: Opts, tool: string): string {
  return stageDir(opts, tool, { defaultProfile: "automq" });
}

const template = (name: string, content: string): Template => ({ name, content });

function spec(source: Template, target: string, data: Opts): Spec {
  return { template: source, target, data, opts: templateOpts };
}

const rawSpec = (target: string, content: string): Spec => contentSpec(target, content);

export function cidrs(opts: Opts, key: string): string[] {
  const value = opts[key];
  const parts = Array.isArray(value) ? value : String(value ?? "").split(/[,\s]+/);
  return parts.map((part) => String(part).trim()).filter((part) => part.length > 0);
}

export function credentialEnv(opts: Opts, ...slots: string[]): Record<string, string> | undefined {
  const mapping: Record<string, string> = Object.assign(
    {},
    ...[...slots, "provider-backend"].map((slot) => validate.tofuEnv(opts, slot)),
  );
  const env: Record<string, string> = {};
  for (const [key, envVar] of Object.entries(mapping)) {
    const value = String(opts[key] ?? "");
    if (value.length > 0) env[envVar] = value;
  }
  return Object.keys(env).length > 0 ? env : undefined;
}

export const backendCredentialEnv = (opts: Opts) => credentialEnv(opts);

// HCL object keys are snake_case and the rest of this package is kebab-case.
// `vpc_ip` is the first output in this project with a word boundary at all —
// ip, user and name are spelled identically in both conventions — so nothing
// had exposed the mismatch before.
function hyphenateKeys(value: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(value).map(([key, nested]) => [key.replaceAll("_", "-"), nested]));
}

// The compute stage's `params` output.
//
// The outer keys are deliberately NOT hyphenated: `ssh_key_id` is the SSH
// Keypair Standard's contract with ONCE's create preflight, which reads it
// verbatim. Only the node entries are converted.
export function normalizeParams(params: unknown): Record<string, unknown> | undefined {
  if (!params || typeof params !== "object") return undefined;
  const map = { ...(params as Record<string, unknown>) };
  if (Array.isArray(map.nodes)) {
    map.nodes = (map.nodes as Record<string, unknown>[]).map(hyphenateKeys);
  }
  return map;
}

// The compute stage's `params` output, normalized.
export function outputParams(result: Opts): Record<string, unknown> | undefined {
  const outputs = result["tofu/outputs"] as Record<string, unknown> | undefined;
  return normalizeParams(outputs?.params);
}

// The applied `params`, or undefined when no state is readable. The SSH Keypair
// Standard's create matrix keys on this best-effort read: an unreadable state
// (a fresh clone, a missing backend) counts as absent.
export async function stateOutput(opts: Opts): Promise<Record<string, unknown> | undefined> {
  try {
    const outputs = await tofu.outputs(toolDir(opts, infrastructureTool), backendCredentialEnv(opts));
    return normalizeParams(outputs?.params);
  } catch {
    return undefined;
  }
}

export function nodes(opts: Opts): cluster.Node[] {
  const params = opts["automq/params"] as Record<string, unknown> | undefined;
  return cluster.nodes(opts, params?.nodes);
}

// ------------------------------------------------------------------ compute

export function infrastructureData(opts: Opts): Opts {
  return {
    ...opts,
    "ssh-keygen": validate.keygen(opts),
    "node-count": cluster.nodeCount(opts),
    "compute-name": cluster.computeName(opts),
    // The firewall rule renders this. A template key that is absent renders as
    // empty rather than failing, so omitting it produced `port = ""` — which
    // survives build, golden, dry-run and validate, and is rejected only by the
    // provider on a real apply.
    "kafka-port": cluster.kafkaPort(opts),
    // The quorum and inter-broker ports are opened to the VPC subnet only — see
    // the firewall comment in main.tf for why that rule has to exist at all.
    "controller-port": cluster.controllerPort(opts),
    "internal-port": cluster.internalPort(opts),
    "ssh-sources-hcl": tofu.hclList(cidrs(opts, "vultr-ssh-sources")),
    "kafka-sources-hcl": tofu.hclList(cidrs(opts, "vultr-kafka-sources")),
  };
}

export async function infrastructureStep(opts: Opts): Promise<Opts> {
  const dir = toolDir(opts, infrastructureTool);
  const specs = [spec(template("infrastructure/main.tf", infrastructureMainTf),
                      `${dir}/main.tf`, infrastructureData(opts))];
  const result = await tofu.tofuWithSpec(opts, specs,
    { dir, env: credentialEnv(opts, "provider-compute") });
  if (failed(result)) return result;
  if (opts["red/event"] === "build") return result;
  if (opts["red/event"] === "delete") return result;
  const params = outputParams(result);
  const error = cluster.missingNodeError(opts, params?.nodes);
  if (error) return { ...result, "red/exit": 1, "red/err": error };
  return { ...result, "automq/params": params };
}

// ---------------------------------------------------------------------- dns

// The Cloudflare zone the cluster's names belong to (their registrable domain).
export function zone(opts: Opts): string | undefined {
  return registrableDomain(opts["automq-host"]);
}

// Every A record this cluster needs.
//
// The bootstrap name carries one record per node, so a client that knows only
// that name reaches some broker and is redirected from there. Each broker also
// gets its own name, because that is what it advertises and what its
// certificate must cover.
//
// `proxied` is false on every record and is not a preference. Cloudflare's
// proxy terminates HTTP; Kafka is a raw TCP protocol on 9092, and a proxied
// record would publish an address that speaks HTTP to a client speaking Kafka.
export function dnsJson(opts: Opts, list: cluster.Node[]): string {
  return tofu.constructsJson([
    ...list.map((node, i) =>
      tofu.construct("resource", "cloudflare_dns_record", `bootstrap_${i}`, {
        zone_id: "${data.cloudflare_zone.zone.id}",
        name: opts["automq-host"], content: node.ip, type: "A",
        proxied: false, ttl: 60,
      })),
    ...list.map((node) =>
      tofu.construct("resource", "cloudflare_dns_record", `broker_${node.index}`, {
        zone_id: "${data.cloudflare_zone.zone.id}",
        name: node["broker-name"], content: node.ip, type: "A",
        proxied: false, ttl: 60,
      })),
  ]);
}

export async function dnsStep(opts: Opts): Promise<Opts> {
  const dir = toolDir(opts, dnsTool);
  const list = nodes(opts);
  const data: Opts = { ...opts, "automq-zone": zone(opts) };
  const specs = [
    spec(template("dns/main.tf", dnsMainTf), `${dir}/main.tf`, data),
    rawSpec(`${dir}/record.tf.json`, dnsJson(data, list)),
  ];
  return tofu.tofuWithSpec(opts, specs, { dir, env: credentialEnv(opts, "provider-dns") });
}

// ------------------------------------------------------- ssh config (local)

// Only what a `build` genuinely knows. Addresses are run-time facts and reach
// the play as extra-vars instead, so the rendered playbook carries no IP and is
// identical on every workstation (SSH Config Standard §6).
export function ansibleLocalData(opts: Opts): Opts {
  return {
    ...opts,
    "ssh-keygen": validate.keygen(opts),
    "ssh-config-identity-file": sshConfig.identityFile(opts),
    "host-alias": sshConfig.hostAlias(opts),
  };
}

export function ansibleLocalSpecs(opts: Opts): Spec[] {
  const dir = toolDir(opts, ansibleLocalTool);
  const data = ansibleLocalData(opts);
  return [
    spec(template("ansible-local/ansible.cfg", ansibleLocalCfg), `${dir}/ansible.cfg`, data),
    spec(template("ansible-local/inventory.ini", ansibleLocalInventory), `${dir}/inventory.ini`, data),
    spec(template("ansible-local/main.yml", ansibleLocalMain), `${dir}/main.yml`, data),
  ];
}

// The `~/.ssh/config` entries, as data the play loops over: the bare profile
// pointing at node 0, then one alias per node.
export function sshConfigHosts(opts: Opts, list: cluster.Node[]): Array<{ name: string; ip: string }> {
  return [
    { name: sshConfig.hostAlias(opts), ip: list[0]!.ip },
    ...list.map((node) => ({ name: sshConfig.nodeAlias(opts, node.index), ip: node.ip })),
  ];
}

// Write or remove the `~/.ssh/config` block. The same playbook serves both
// events; `block_state` is what distinguishes them.
export async function ansibleLocalStep(opts: Opts): Promise<Opts> {
  const dir = toolDir(opts, ansibleLocalTool);
  const isDelete = opts["red/event"] === "delete";
  return ansible.ansibleWithSpec(opts, {
    dir,
    inventory: "inventory.ini",
    playbooks: { create: "main.yml", delete: "main.yml" },
    extraVars: {
      host_alias: sshConfig.hostAlias(opts),
      ssh_hosts: sshConfigHosts(opts, nodes(opts)),
      block_state: isDelete ? "absent" : "present",
    },
  }, ansibleLocalSpecs(opts));
}

// ------------------------------------------------------------------ ansible

// Cheshire's pretty-printer, which is Green's byte-level artifact contract:
// spaces around colons, compact empty collections, and keys in the order they
// are given — which is why every map below is built already sorted.
function pretty(value: unknown, indent = 0): string {
  if (Array.isArray(value)) {
    if (value.length === 0) return "[ ]";
    return `[ ${value.map((item) => pretty(item, indent)).join(", ")} ]`;
  }
  if (value !== null && typeof value === "object") {
    const entries = Object.entries(value as Record<string, unknown>);
    if (entries.length === 0) return "{ }";
    const pad = " ".repeat(indent + 2);
    return `{\n${entries
      .map(([key, nested]) => `${pad}${JSON.stringify(key)} : ${pretty(nested, indent + 2)}`)
      .join(",\n")}\n${" ".repeat(indent)}}`;
  }
  return JSON.stringify(value ?? null);
}

function sorted(value: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(value).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
}

// One host per node, each carrying the facts only it has.
//
// Per-node values live here rather than in the rendered templates because there
// is one template set for the whole cluster: the playbook fills `node.id`, the
// listeners and the advertised names from these variables. The cluster-wide
// values that must be *identical* everywhere — the quorum string above all —
// are rendered once into the play instead, so three nodes cannot disagree about
// them.
export function inventory(opts: Opts, list: cluster.Node[]): string {
  const hosts: Record<string, unknown> = {};
  for (const node of list) {
    const host: Record<string, unknown> = {
      ansible_host: node.ip,
      ansible_user: node.user ?? "root",
      automq_node_id: node.index,
      automq_vpc_ip: node["vpc-ip"],
      automq_broker_name: node["broker-name"],
      automq_listeners: cluster.listeners(opts, node),
      automq_advertised_listeners: cluster.advertisedListeners(opts, node),
      // Node 0 is the only ACME client and the only host that receives the
      // zone-editing token.
      automq_cert_issuer: node.index === 0,
    };
    if (validate.keygen(opts)) {
      host.ansible_ssh_private_key_file = opts["ssh-private-key-path"];
    }
    hosts[String(node.name)] = sorted(host);
  }
  return pretty({ all: { children: { automq: { hosts: sorted(hosts) } } } });
}

// Template values for the convergence stage.
//
// Deliberately carries no credential. The R2 keys and the Cloudflare token
// reach the hosts as Ansible `lookup('env', ...)` expressions written literally
// into main.yml, where `preserve-jinja-delimiters` passes them through
// untouched — routing them through this map would let the renderer HTML-escape
// the quotes and hand Ansible `&#39;`. The secret therefore exists only in the
// process that needs it: not in `.colors/`, not in a golden, not in this map.
export function ansibleData(opts: Opts): Opts {
  const list = nodes(opts);
  return {
    ...opts,
    "ssh-keygen": validate.keygen(opts),
    "node-count": cluster.nodeCount(opts),
    "quorum-voters": cluster.quorumVoters(opts, list),
    "certificate-names": cluster.certificateNames(opts),
    "certificate-names-csv": cluster.certificateNames(opts).join(","),
    "bootstrap-internal": list.map((node) =>
      `${node["vpc-ip"]}:${cluster.internalPort(opts)}`).join(","),
    "bootstrap-external": `${opts["automq-host"]}:${cluster.kafkaPort(opts)}`,
    "admin-user": cluster.adminUser(opts),
    "broker-user": cluster.brokerUser(opts),
    "controller-user": cluster.controllerUser(opts),
    "client-user": cluster.clientUser(opts),
    "scram-principals": cluster.scramPrincipals(opts),
    "super-users": cluster.superUsers(opts),
    "client-acls": cluster.clientAcls(opts),
    "topic-prefix": cluster.topicPrefix(opts),
    "controller-port": cluster.controllerPort(opts),
    "internal-port": cluster.internalPort(opts),
    "kafka-port": cluster.kafkaPort(opts),
  };
}

export const ansibleFiles: Array<[string, string]> = [
  ["ansible.cfg", ansibleCfg],
  ["main.yml", ansibleMain],
  ["cleanup.yml", ansibleCleanup],
  ["compose.yml", ansibleCompose],
  ["server.properties", ansibleServerProperties],
  ["store.py", ansibleStore],
  ["secrets.sh", ansibleSecrets],
  ["render-config.sh", ansibleRenderConfig],
  ["format.sh", ansibleFormat],
  ["acl.sh", ansibleAcl],
  ["scram.sh", ansibleScram],
  ["cert.sh", ansibleCert],
  ["cert-deploy.sh", ansibleCertDeploy],
  ["cert-deploy.service", ansibleCertDeployService],
  ["cert-deploy.timer", ansibleCertDeployTimer],
  ["cert-renew.service", ansibleCertRenewService],
  ["cert-renew.timer", ansibleCertRenewTimer],
  ["status.sh", ansibleStatus],
  ["credential.sh", ansibleCredential],
  ["smoke.sh", ansibleSmoke],
  ["rotate.sh", ansibleRotate],
];

export function ansibleSpecs(opts: Opts): Spec[] {
  const dir = toolDir(opts, ansibleTool);
  const data = ansibleData(opts);
  return [
    ...ansibleFiles.map(([name, content]) =>
      spec(template(`ansible/${name}`, content), `${dir}/${name}`, data)),
    rawSpec(`${dir}/inventory.json`, inventory(data, nodes(opts))),
  ];
}

export async function ansibleStep(opts: Opts): Promise<Opts> {
  const dir = toolDir(opts, ansibleTool);
  const params = opts["automq/params"] as Record<string, unknown> | undefined;
  const applied = Array.isArray(params?.nodes) ? params.nodes as unknown[] : [];
  if (opts["red/event"] === "delete" && applied.length === 0) {
    // No compute in state: there is nothing to stop, and the cleanup play would
    // only fail against the placeholder addresses.
    return { ...opts, "red/exit": 0 };
  }
  return ansible.ansibleWithSpec(opts, {
    dir,
    inventory: "inventory.json",
    playbooks: { create: "main.yml", delete: "cleanup.yml" },
    hostKeyChecking: false,
  }, ansibleSpecs(opts));
}

// --------------------------------------------------------------- acceptance

export function acceptanceSpecs(opts: Opts): Spec[] {
  const dir = toolDir(opts, acceptanceTool);
  return [spec(template("acceptance/acceptance.sh", acceptanceSh),
               `${dir}/acceptance.sh`, ansibleData(opts))];
}

export function processResult(
  opts: Opts, label: string, result: { exit: number; out?: string; err?: string },
): Opts {
  if (result.exit === 0) return { ...opts, "red/exit": 0 };
  return {
    ...opts,
    "red/exit": Math.max(1, result.exit),
    "red/err": `${label} failed: ` +
      (String(result.err ?? "").length > 0 ? result.err
        : String(result.out ?? "").length > 0 ? result.out : "(no output)"),
  };
}

// The operator path, proved from the workstation.
//
// Everything the playbook can prove, the playbook already proved on the hosts
// before the ready marker was written. What is left is what only a client
// outside the deployment can establish: that the public names resolve, that the
// certificate they serve validates, that SASL_SSL admits the client principal
// and refuses a wrong password, that the ACLs deny what they should, and that
// killing a broker which leads a partition does not lose the records written to
// it.
//
// Forty-five minutes, not twenty. Every wait in that script is bounded, but the
// bounds add up: the partition becoming writable again (300s), the survival
// read retried while the partition is reassigned (120s), the victim rejoining
// with bounded lag (600s), and the controller quorum re-forming (600s). Those
// are worst cases and the usual run is a fraction of them — but a ceiling below
// the sum of the parts turns a slow cluster into a killed test, and a killed
// test cannot run the trap that restarts the broker it stopped.
export async function acceptanceStep(opts: Opts): Promise<Opts> {
  const rendered = scaffold(opts, acceptanceSpecs(opts));
  if (opts["red/event"] !== "create") return rendered;
  const result = await runtime.exec(
    ["bash", `${toolDir(opts, acceptanceTool)}/acceptance.sh`], { timeoutMs: 2700000 });
  return processResult(rendered, "acceptance", result);
}

export function generatedCleanupStep(opts: Opts): Opts {
  return scaffold(scaffold(opts, ansibleSpecs(opts)), acceptanceSpecs(opts));
}
