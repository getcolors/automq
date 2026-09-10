// Deployment-owned S3 application buckets and scoped credentials.
import {stageDir} from "red/cli";
import {runtime} from "red/runtime";
import {scaffold, PRESERVE_JINJA_DELIMITERS, type Spec} from "red/scaffold";
import * as tofu from "red/tofu";
import type {Opts} from "red/workflow";
import mainTf from "../resources/tools/storage/main.tf" with {type: "text"};

export const tool = "automq-storage";
export const managed = (opts: Opts): boolean => opts["automq-storage-managed"] === true;
export const directory = (opts: Opts): string => stageDir(opts, tool, {defaultProfile: "automq"});
export function awsEnv(opts: Opts): Record<string, string> {
  return Object.fromEntries(Object.entries({"aws-access-key-id":"AWS_ACCESS_KEY_ID", "aws-secret-access-key":"AWS_SECRET_ACCESS_KEY", "aws-session-token":"AWS_SESSION_TOKEN"}).filter(([key]) => opts[key]).map(([key, variable]) => [variable, String(opts[key])]));
}
export function specs(opts: Opts): Spec[] {
  return [{template:{name:"tools/storage/main.tf", content:mainTf}, target:directory(opts)+"/main.tf", data:opts, opts:PRESERVE_JINJA_DELIMITERS}];
}
export async function ownershipPreflight(opts: Opts): Promise<void> {
  const config = {cwd:directory(opts), env:awsEnv(opts)};
  const init = await runtime.exec(["tofu", "init", "-input=false", "-no-color"], config);
  if (init.exit !== 0) throw new Error("managed storage state operation failed");
  const state = await runtime.exec(["tofu", "state", "list"], config);
  if (state.exit !== 0 && !/No state file was found!/.test(state.err)) throw new Error("managed storage state operation failed");
  let resources: Array<{address:string, values?:{bucket?:string}}> = [];
  if (state.exit === 0 && state.out.trim()) {
    const shown = await runtime.exec(["tofu", "show", "-json"], config);
    if (shown.exit !== 0) throw new Error("managed storage state operation failed");
    resources = JSON.parse(shown.out).values?.root_module?.resources ?? [];
  }
  for (const [role,key] of [["data","automq-data-r2-bucket"],["ops","automq-ops-r2-bucket"]]) {
    if (resources.some(resource => resource.address === `aws_s3_bucket.application["${role}"]` && resource.values?.bucket === opts[key!])) continue;
    const probe = await runtime.exec(["aws","s3api","head-bucket","--bucket",String(opts[key!]),"--region",String(opts["automq-r2-region"])],config);
    if (!(probe.exit > 0 && /\(404\)|Not Found|NoSuchBucket/.test(probe.err))) throw new Error("managed storage refuses to adopt an existing or inaccessible bucket");
  }
}
export async function storageStep(opts: Opts): Promise<Opts> {
  if (!managed(opts)) return {...opts,"red/exit":0};
  try {
    const documents = specs(opts);
    if (opts["red/event"] === "create") { scaffold(opts,documents); await ownershipPreflight(opts); }
    return await tofu.tofuWithSpec(opts,documents,{dir:directory(opts),env:awsEnv(opts),outputKey:"automq/storage-credentials"});
  } catch { return {...opts,"red/exit":1,"red/err":"managed S3 storage failed; inspect bucket ownership, state access, and AWS permissions"}; }
}
export function credentialEnv(opts: Opts): Record<string,string> {
  const {access_key_id,secret_access_key} = opts["automq/storage-credentials"] ?? {};
  if (!String(access_key_id??"").trim() || !String(secret_access_key??"").trim()) throw new Error("managed storage credentials unavailable");
  return {COLORS_PAR_AUTOMQ_R2_ACCESS_KEY_ID:access_key_id,COLORS_PAR_AUTOMQ_R2_SECRET_ACCESS_KEY:secret_access_key,ANSIBLE_HOST_KEY_CHECKING:"False"};
}
