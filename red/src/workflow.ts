// AutoMQ lifecycle DAG, validation, and package-specific backend state keys.

import { readPars, parName } from "red/cli";
import * as dryRun from "red/dry-run";
import { preflight } from "red/lifecycle";
import * as progress from "red/progress";
import * as tofu from "red/tofu";
import { adviceAdd, failed, workflow, type Opts, type WireDecl } from "red/workflow";
import * as cluster from "./cluster.ts";
import * as ssh from "./ssh.ts";
import * as sshConfig from "./ssh-config.ts";
import * as tools from "./tools.ts";
import * as validate from "./validate.ts";

export const defaults: Opts = {
  "provider-compute": "vultr",
  "provider-dns": "cloudflare",
  "provider-backend": "local",
  "compute-prevent-destroy": true,
  workdir: ".colors",
  "automq-node-count": cluster.defaultNodeCount,
  "automq-broker-name-prefix": "b",
  "automq-kafka-port": 9092,
  "automq-internal-port": 9094,
  "automq-controller-port": 9093,
  "automq-sasl-user": "automq",
  "automq-admin-user": "automq-admin",
  "automq-broker-user": "automq-broker",
  "automq-controller-user": "automq-controller",
  "automq-sasl-mechanism": "SCRAM-SHA-512",
  "automq-client-topic-prefix": "colors-",
  "automq-topic-partitions": 6,
  "automq-log-retention-hours": 168,
  "automq-r2-region": "auto",
  "automq-wal-batch-interval-ms": 250,
  "automq-wal-max-bytes-in-batch": 8388608,
  "vultr-vpc-subnet": "10.40.0.0/24",
};

// Events that authenticate against Vultr and require the local toolchain.
const checkedEvents = ["create", "delete", "validate"];

export async function startStep(
  opts: Opts,
  env: Record<string, string | undefined> = process.env,
): Promise<Opts> {
  // The tool and Vultr checks shell out, and preflight's validators are
  // synchronous — so they run here, over the same overlaid state preflight will
  // build, and reach the validator list through a closure. Rebuilding the
  // overlay is deliberate: reporting a missing tool only on the run *after* the
  // operator fixed their colors.yml is exactly the "one thing at a time"
  // behaviour exit code 2 exists to avoid.
  const overlaid = readPars({ ...defaults, ...opts }, env);
  const event = typeof opts["red/event"] === "string" ? opts["red/event"] as string : undefined;
  const runtimeErrors = !opts["red/dry-run"] && event && checkedEvents.includes(event)
    ? await validate.runtimeErrors(overlaid)
    : [];
  return preflight(opts, {
    defaults,
    overlay: readPars,
    validators: [
      (_opts, environment) => validate.envErrors(environment),
      (current) => validate.stateErrors(current),
      (current, _environment, { event, real }) =>
        real && (event === "create" || event === "delete")
          ? validate.secretErrors(current, event)
          : [],
      (current, _environment, { event, real }) =>
        real && event === "delete" && current["compute-prevent-destroy"]
          ? [`compute destruction is protected; set ${parName("compute-prevent-destroy")}=false to delete`]
          : [],
      () => runtimeErrors,
    ],
    // The machine key's create matrix and the Vultr preflight run before any
    // template is rendered: an unowned key on disk or at the provider stops the
    // run while stopping is still free. Delete fills the same template values —
    // a destroy renders before it destroys — but checks nothing, because its
    // key cleanup runs after the compute destroy.
    afterValidate: async (current, _environment, { event, real }) => {
      if (real && event === "delete") {
        const params = await tools.stateOutput(current);
        return {
          ...ssh.withMachineKey(current),
          ...(params ? { "automq/params": params } : {}),
          "red/exit": 0,
        };
      }
      if (real && event === "create") {
        let next = await ssh.ensureKey(current, tools.stateOutput);
        if (failed(next)) return next;
        next = await ssh.preflight(ssh.withMachineKey(next));
        if (!failed(next)) next = sshConfig.preflight(next);
        return failed(next) ? next : { ...next, "red/exit": 0 };
      }
      return { ...ssh.withMachineKey(current), "red/exit": 0 };
    },
  }, env);
}

export function wireFn(step: string, runOpts: Opts): WireDecl | undefined {
  // `validate` answers "would this run?" and must not render or plan anything
  // to do it. Falling through to the create chain would call `tofu validate` on
  // a compute stage that reads the machine public key — a file only a real
  // create generates — so the check would fail on exactly the fresh checkout it
  // exists to serve.
  if (runOpts["red/event"] === "validate") {
    const graph: Record<string, WireDecl> = { "automq/start": [startStep] };
    return graph[step];
  }
  if (runOpts["red/event"] === "delete") {
    // The `~/.ssh/config` block goes before the destroy, the keypair after it.
    // A block that outlives its host is stale but harmless; a key that
    // predeceases its host locks the operator out of machines that still exist.
    // Both orders are deliberate — standards/ssh-config.md §4 is explicit that
    // they must not be tidied into agreement.
    const graph: Record<string, WireDecl> = {
      "automq/start": [startStep, "automq/ansible"],
      "automq/ansible": [tools.ansibleStep, "automq/ssh-config"],
      "automq/ssh-config": [tools.ansibleLocalStep, "automq/dns"],
      // DNS goes before the compute destroy: records pointing at addresses that
      // have been released are worse than no records, because a reissued
      // address makes them point at somebody else's machine.
      "automq/dns": [tools.dnsStep, "automq/infrastructure"],
      "automq/infrastructure": [tools.infrastructureStep, "automq/ssh-cleanup"],
      "automq/ssh-cleanup": [ssh.cleanupStep],
    };
    return graph[step];
  }
  const graph: Record<string, WireDecl> = {
    "automq/start": [startStep, "automq/infrastructure"],
    "automq/infrastructure": [tools.infrastructureStep, "automq/ssh-config"],
    "automq/ssh-config": [tools.ansibleLocalStep, "automq/dns"],
    // DNS before convergence, because every broker advertises a name that must
    // already resolve — and because the certificate is issued for those names
    // during the play.
    "automq/dns": [tools.dnsStep, "automq/ansible"],
    "automq/ansible": [tools.ansibleStep, "automq/acceptance"],
    "automq/acceptance": [tools.acceptanceStep],
  };
  return graph[step];
}

export function backendAdvice(tool: string) {
  return tofu.conventionalBackendAdvice({
    dir: (opts) => tools.toolDir(opts, tool),
    key: (opts) => `${opts.profile ?? ""}/${tool}.tfstate`,
  });
}

export const sideEffecting = [
  "automq/infrastructure", "automq/dns", "automq/ssh-config", "automq/ansible",
  "automq/acceptance", "automq/ssh-cleanup",
];

function create() {
  let wf = workflow({ start: "automq/start", wireFn });
  wf = adviceAdd(wf, "automq/infrastructure", "before", "automq.workflow/backend",
    backendAdvice(tools.infrastructureTool));
  wf = adviceAdd(wf, "automq/dns", "before", "automq.workflow/backend",
    backendAdvice(tools.dnsTool));
  return dryRun.advise(progress.advise(wf), sideEffecting);
}

export const automqWorkflow = create();
