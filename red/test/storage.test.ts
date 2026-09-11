import {afterEach, expect, spyOn, test} from "bun:test";
import {runtime} from "red/runtime";
import * as storage from "../src/storage.ts";
import * as workflow from "../src/workflow.ts";
let mocked: ReturnType<typeof spyOn> | undefined;
afterEach(() => mocked?.mockRestore());
const opts = {profile:"owned",workdir:"/tmp", "automq-storage-managed":true,"automq-data-r2-bucket":"owned-data", "automq-ops-r2-bucket":"owned-ops", "automq-r2-region":"eu-central-1"};
test("managed storage refuses existing or inaccessible untracked buckets", async () => {
  for (const probe of [{exit:0,out:"",err:""},{exit:1,out:"",err:"(403) Forbidden"}]) {
    mocked = spyOn(runtime,"exec").mockResolvedValueOnce({exit:0,out:"",err:""}).mockResolvedValueOnce({exit:0,out:"",err:""}).mockResolvedValue(probe);
    await expect(storage.ownershipPreflight(opts)).rejects.toThrow("refuses to adopt");
    mocked.mockRestore();
  }
});
test("managed storage accepts absent buckets or addresses tracked by its own state", async () => {
  mocked = spyOn(runtime,"exec").mockResolvedValueOnce({exit:0,out:"",err:""}).mockResolvedValueOnce({exit:0,out:'aws_s3_bucket.application["data"]\n',err:""}).mockResolvedValueOnce({exit:0,out:JSON.stringify({values:{root_module:{resources:[{address:'aws_s3_bucket.application["data"]',values:{bucket:"owned-data"}}]}}}),err:""}).mockResolvedValue({exit:1,out:"",err:"(404) Not Found"});
  await storage.ownershipPreflight(opts);
  expect(mocked).toHaveBeenCalledTimes(4);
});
test("managed storage is ordered after compute create and before compute delete", () => {
  expect(workflow.wireFn("automq/infrastructure",{...opts,"red/event":"create"})?.[1]).toBe("automq/storage");
  expect(workflow.wireFn("automq/dns",{...opts,"red/event":"delete"})?.[1]).toBe("automq/storage");
  expect(workflow.wireFn("automq/storage",{...opts,"red/event":"delete"})?.[1]).toBe("automq/infrastructure");
});
test("retired compute routes directly to managed backend finalization on delete retry", () => {
  const retry = {...opts, "provider-backend":"s3", "s3-bucket-mode":"managed", "automq/finalize-only":true};
  expect(workflow.nextSteps("automq/start",["automq/ansible"],retry)).toEqual([["automq/backend-finalize",retry]]);
  expect(workflow.nextSteps("automq/backend-finalize",[],retry)).toEqual([]);
  expect(workflow.wireFn("automq/infrastructure",{...retry,"red/event":"delete"})?.[1]).toBe("automq/backend-finalize");
});
test("fresh remote state permits creation but unreadable state fails closed", async () => {
  mocked = spyOn(runtime,"exec").mockResolvedValueOnce({exit:0,out:"",err:""}).mockResolvedValueOnce({exit:1,out:"",err:"No state file was found!"}).mockResolvedValue({exit:1,out:"",err:"(404) Not Found"});
  await storage.ownershipPreflight(opts);
  expect(mocked).toHaveBeenCalledTimes(4);
  mocked.mockRestore();
  mocked = spyOn(runtime,"exec").mockResolvedValueOnce({exit:0,out:"",err:""}).mockResolvedValueOnce({exit:1,out:"",err:"AccessDenied"});
  await expect(storage.ownershipPreflight(opts)).rejects.toThrow("state operation failed");
});
test("renaming a tracked bucket must still refuse adoption of an existing destination", async () => {
  mocked = spyOn(runtime,"exec").mockResolvedValueOnce({exit:0,out:"",err:""}).mockResolvedValueOnce({exit:0,out:'aws_s3_bucket.application["data"]',err:""}).mockResolvedValueOnce({exit:0,out:JSON.stringify({values:{root_module:{resources:[{address:'aws_s3_bucket.application["data"]',values:{bucket:"old-data"}}]}}}),err:""}).mockResolvedValue({exit:0,out:"",err:""});
  await expect(storage.ownershipPreflight(opts)).rejects.toThrow("refuses to adopt");
});

test("GCS ownership probes fail closed and finalize the selected backend", async () => {
  const gcs = {...opts,"automq-storage-provider":"gcs","google-project":"colors-508307","provider-backend":"gcs","gcs-bucket-mode":"managed"};
  mocked = spyOn(runtime,"exec").mockImplementation(async (args) => ({exit:args[0] === "gcloud" ? 1 : 0,out:"",err:args[0] === "gcloud" ? "gs://owned-data not found: 404." : ""}));
  await storage.ownershipPreflight(gcs);
  expect(mocked.mock.calls.filter((call: any[]) => call[0][0] === "gcloud")).toHaveLength(2);
  mocked.mockRestore();
  mocked = spyOn(runtime,"exec").mockImplementation(async (args) => ({exit:args[0] === "gcloud" ? 1 : 0,out:"",err:"HTTPError 403: Forbidden"}));
  await expect(storage.ownershipPreflight(gcs)).rejects.toThrow("refuses to adopt");
  expect(workflow.wireFn("automq/infrastructure",{...gcs,"red/event":"delete"})?.[1]).toBe("automq/backend-finalize");
});
