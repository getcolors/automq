// AutoMQ lifecycle DAG, validation, and package-specific backend state keys.

import { readPars, parName } from "red/cli";
import * as dryRun from "red/dry-run";
import { preflight, type PreflightContext } from "red/lifecycle";
import * as progress from "red/progress";
import * as tofu from "red/tofu";
import { adviceAdd, failed, workflow, type Opts, type WireDecl } from "red/workflow";
import {read_deployment, finalize_backend, backend_plan} from "colors-compute-red";
import * as cluster from "./cluster.ts";
import * as ssh from "./ssh.ts";
import * as sshConfig from "./ssh-config.ts";
import * as tools from "./tools.ts";
import * as storage from "./storage.ts";
import * as validate from "./validate.ts";

export const defaults: Opts = {
  "provider-compute": validate.defaultComputeProvider,
  "provider-dns": "cloudflare",
  "automq-tls-mode": "acme",
  "provider-backend": "r2",
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

export interface StartDeps {reader?:(opts:Opts)=>Promise<any>;runtimeErrors?:(opts:Opts)=>Promise<string[]>}
export async function startStep(opts:Opts,env:Record<string,string|undefined>=process.env,deps:StartDeps={}):Promise<Opts>{
 const overlaid=readPars({...defaults,...opts},env);const real=!overlaid['red/dry-run']&&checkedEvents.includes(overlaid['red/event']);
 const errors=real?await (deps.runtimeErrors??validate.runtimeErrors)(overlaid):[];
 return preflight(opts,{defaults,overlay:readPars,validators:[
  (_o,e)=>validate.envErrors(e),(o)=>validate.stateErrors(o),
  (o,_e,c)=>c.real&&checkedEvents.includes(c.event??'')&&!validate.stateErrors(o).length?validate.secretErrors(o,c.event??''):[],
  (o,_e,c)=>c.real&&c.event==='delete'&&o['compute-prevent-destroy']?['compute destruction is protected; set COLORS_PAR_COMPUTE_PREVENT_DESTROY=false to delete']:[],()=>errors,
 ],afterValidate:async(current,_e,c)=>{
  if(c.real&&c.event==='delete'){
   const result=await (deps.reader??((o)=>read_deployment(o,env)))(current);
   if(result.status!=='present'&&current[`${current['provider-backend']}-bucket-mode`]==='managed')return {...current,'automq/finalize-only':true,'red/exit':0};
   if(result.status==='destroyed')return {...current,'automq/already-destroyed':true,'red/exit':0};
   if(result.status!=='present')return {...current,'red/exit':1,'red/err':'compute state unavailable; legacy monolithic state requires explicit migration'};
   return {...current,'colors-compute/cluster':result.cluster,...(result.key?.private_key_path?{'ssh-private-key-path':result.key.private_key_path}:{}),'red/exit':0};
  }
  if(c.real&&c.event==='create')return sshConfig.preflight(current);
  return {...ssh.withMachineKey(current),'red/exit':0};
 }},env);
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
      "automq/dns": [tools.dnsStep, runOpts["automq-storage-managed"] ? "automq/storage" : "automq/infrastructure"],
      "automq/storage": [storage.storageStep, "automq/infrastructure"],
      "automq/infrastructure": runOpts[`${runOpts["provider-backend"]}-bucket-mode`] === "managed" ? [tools.infrastructureStep, "automq/backend-finalize"] : [tools.infrastructureStep],
      "automq/backend-finalize": [backendFinalizeStep],
    };
    return graph[step];
  }
  const graph: Record<string, WireDecl> = {
    "automq/start": [startStep, "automq/infrastructure"],
    "automq/infrastructure": [tools.infrastructureStep, runOpts["automq-storage-managed"] ? "automq/storage" : "automq/ssh-config"],
    "automq/storage": [storage.storageStep, "automq/ssh-config"],
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
  return (opts: Opts) => opts["provider-backend"] === "oci"
    ? tofu.s3BackendAdvice((o: Opts) => tools.toolDir(o, tool), (o: Opts) => (backend_plan(o,`${o.profile}/${tool}.tfstate`).config as any).terraform.backend.s3)(opts)
    : opts["provider-backend"] === "gcs"
    ? tofu.gcsBackendAdvice((o: Opts) => tools.toolDir(o, tool), (o: Opts) => ({bucket:o["gcs-bucket"],prefix:`${o.profile}/${tool}.tfstate` }))(opts)
    : tofu.conventionalBackendAdvice({
    dir: (opts) => tools.toolDir(opts, tool),
    key: (opts) => `${opts.profile ?? ""}/${tool}.tfstate`,
  })(opts);
}

export const sideEffecting = [
  "automq/infrastructure", "automq/dns", "automq/ssh-config", "automq/ansible",
  "automq/acceptance", "automq/ssh-cleanup", "automq/storage", "automq/backend-finalize",
];

export async function backendFinalizeStep(opts: Opts): Promise<Opts> {
  try {
    const result = await finalize_backend(opts, {...process.env, ...storage.awsEnv(opts)});
    return ["skipped", "absent", "destroyed"].includes(result.status) ? {...opts, "red/exit":0} : {...opts, "red/exit":1, "red/err":"managed backend finalization failed"};
  } catch { return {...opts, "red/exit":1, "red/err":"managed backend finalization failed; inspect ownership and remaining state"}; }
}

export function nextSteps(step: string, next: string[] | null | undefined, opts: Opts): Array<[string, Opts]> {
  if (failed(opts)) return [];
  if (opts["automq/already-destroyed"]) return [];
  if (step === "automq/start" && opts["automq/finalize-only"]) return [["automq/backend-finalize",opts]];
  return (next??[]).map(target=>[target,opts]);
}

function create() {
  let wf = workflow({ start: "automq/start", wireFn, nextFn:nextSteps });
  wf = adviceAdd(wf, "automq/dns", "before", "automq.workflow/backend",
    backendAdvice(tools.dnsTool));
  wf = adviceAdd(wf, "automq/storage", "before", "automq.workflow/storage-backend", backendAdvice(storage.tool));
  return dryRun.advise(progress.advise(wf), sideEffecting);
}

export const automqWorkflow = create();
