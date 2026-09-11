// Deployment-owned application buckets and scoped credentials.
import {stageDir} from "red/cli";
import {runtime} from "red/runtime";
import {scaffold, PRESERVE_JINJA_DELIMITERS, type Spec} from "red/scaffold";
import * as tofu from "red/tofu";
import type {Opts} from "red/workflow";
import mainTf from "../resources/tools/storage/main.tf" with {type: "text"};

import ociStorage from "../resources/tools/storage/oci-storage.py" with {type: "text"};

export const tool = "automq-storage";
export const managed = (opts: Opts): boolean => opts["automq-storage-managed"] === true;
export const directory = (opts: Opts): string => stageDir(opts, tool, {defaultProfile: "automq"});
export function awsEnv(opts: Opts): Record<string, string> {
  return Object.fromEntries(Object.entries(opts["provider-backend"] === "oci" ? {"oci-access-key-id":"AWS_ACCESS_KEY_ID","oci-secret-access-key":"AWS_SECRET_ACCESS_KEY"} : {"aws-access-key-id":"AWS_ACCESS_KEY_ID", "aws-secret-access-key":"AWS_SECRET_ACCESS_KEY", "aws-session-token":"AWS_SESSION_TOKEN"}).filter(([key]) => opts[key]).map(([key, variable]) => [variable, String(opts[key])]));
}
export function specs(opts: Opts): Spec[] {
  const result: Spec[] = [{template:{name:"tools/storage/main.tf", content:mainTf}, target:directory(opts)+"/main.tf", data:{...opts,"automq-storage-gcs":opts["automq-storage-provider"] === "gcs","automq-storage-oci":opts["automq-storage-provider"] === "oci","oci-auth":opts["oci-auth"] ?? "APIKey","oci-home-region":opts["oci-home-region"] ?? opts["automq-r2-region"]}, opts:PRESERVE_JINJA_DELIMITERS}];
  if (opts["automq-storage-provider"] === "oci") result.push({template:{name:"tools/storage/oci-storage.py",content:ociStorage},target:directory(opts)+"/oci-storage.py",data:opts,opts:PRESERVE_JINJA_DELIMITERS});
  return result;
}
async function ociOperation(opts: Opts, action: string): Promise<void> {
  const values = Object.fromEntries(["profile","oci-auth","oci-config-file-profile","oci-namespace","oci-compartment-id","automq-r2-region","automq-data-r2-bucket","automq-ops-r2-bucket","compute-prevent-destroy"].map(key=>[key,opts[key]]));
  const result = await runtime.exec(["python3","oci-storage.py",action,JSON.stringify(values)],{cwd:directory(opts),env:awsEnv(opts)});
  if (result.exit !== 0) throw new Error("OCI storage operation failed");
}
export async function ownershipPreflight(opts: Opts): Promise<void> {
  if (opts["automq-storage-provider"] === "oci") return ociOperation(opts,"preflight");
  const config = {cwd:directory(opts), env:awsEnv(opts)};
  const init = await runtime.exec(["tofu", "init", "-input=false", "-no-color"], config);
  if (init.exit !== 0) throw new Error("managed storage state operation failed");
  const state = await runtime.exec(["tofu", "state", "list"], config);
  if (state.exit !== 0 && !/No state file was found!/.test(state.err)) throw new Error("managed storage state operation failed");
  let resources: Array<{address:string, values?:{bucket?:string,name?:string}}> = [];
  if (state.exit === 0 && state.out.trim()) {
    const shown = await runtime.exec(["tofu", "show", "-json"], config);
    if (shown.exit !== 0) throw new Error("managed storage state operation failed");
    resources = JSON.parse(shown.out).values?.root_module?.resources ?? [];
  }
  for (const [role,key] of [["data","automq-data-r2-bucket"],["ops","automq-ops-r2-bucket"]]) {
    if (resources.some(resource => resource.address === `${opts["automq-storage-provider"] === "gcs" ? "google_storage_bucket" : "aws_s3_bucket"}.application["${role}"]` && (resource.values?.bucket ?? resource.values?.name) === opts[key!])) continue;
    const probe = await runtime.exec(opts["automq-storage-provider"] === "gcs" ? ["gcloud","storage","buckets","describe",`gs://${opts[key!]}`,"--project",String(opts["google-project"]),"--format=json"] : ["aws","s3api","head-bucket","--bucket",String(opts[key!]),"--region",String(opts["automq-r2-region"])],config);
    if (!(probe.exit > 0 && /\(404\)|Not Found|NoSuchBucket|HTTPError 404|not found: 404/.test(probe.err))) throw new Error("managed storage refuses to adopt an existing or inaccessible bucket");
  }
}
export async function storageStep(opts: Opts): Promise<Opts> {
  if (!managed(opts)) return {...opts,"red/exit":0};
  try {
    const documents = specs(opts);
    if (opts["red/event"] === "delete" && opts["automq-storage-provider"] === "oci") { scaffold({...opts,"red/event":"create"},documents); await ociOperation(opts,"cleanup"); }
    if (opts["red/event"] === "create") { scaffold(opts,documents); await ownershipPreflight(opts); }
    return await tofu.tofuWithSpec(opts,documents,{dir:directory(opts),env:awsEnv(opts),outputKey:"automq/storage-credentials"});
  } catch { return {...opts,"red/exit":1,"red/err":"managed storage failed; inspect bucket ownership, state access, and provider permissions"}; }
}
export function credentialEnv(opts: Opts): Record<string,string> {
  const {access_key_id,secret_access_key,oci_signing_key_b64,oci_signing_key_id} = opts["automq/storage-credentials"] ?? {};
  if (!String(access_key_id??"").trim() || !String(secret_access_key??"").trim() || (opts["automq-storage-provider"] === "oci" && (!oci_signing_key_b64 || !oci_signing_key_id))) throw new Error("managed storage credentials unavailable");
  return {COLORS_PAR_AUTOMQ_R2_ACCESS_KEY_ID:access_key_id,COLORS_PAR_AUTOMQ_R2_SECRET_ACCESS_KEY:secret_access_key,COLORS_PAR_AUTOMQ_OCI_SIGNING_KEY_B64:oci_signing_key_b64 ?? "",COLORS_PAR_AUTOMQ_OCI_SIGNING_KEY_ID:oci_signing_key_id ?? "",ANSIBLE_HOST_KEY_CHECKING:"False"};
}
