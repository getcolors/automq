import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { StepError, type Opts } from "red/workflow";
import {collect,source_cidrs} from "colors-compute-red";
import * as cluster from "../src/cluster.ts";
import * as ssh from "../src/ssh.ts";
import * as sshConfig from "../src/ssh-config.ts";
import * as tools from "../src/tools.ts";
import * as validate from "../src/validate.ts";
import * as workflow from "../src/workflow.ts";

const fixtureFile = join(import.meta.dir, "../../test/fixtures/colors.yml");
const optoutFile = join(import.meta.dir, "../../test/fixtures/optout.yml");

function readFixture(path: string, overrides: Opts): Opts {
  const text = readFileSync(path, "utf8").replaceAll("WORKDIR", ".colors");
  return { ...(Bun.YAML.parse(text) as Opts), "provider-backend":"r2", ...overrides };
}

const fixture = (overrides: Opts = {}) => readFixture(fixtureFile, overrides);
const optout = (overrides: Opts = {}) => readFixture(optoutFile, overrides);

// The compute stage's recorded `params`, as ONCE reads it: snake_case node
// keys, every field present.
const params: any = {
  provider: "vultr",
  ssh_key_id: "7692e92a",
  nodes: [
    { provider:"vultr", node_id: "0", role: null, index: 0, ip: "203.0.113.10", vpc_ip: "10.40.0.3", user: "root", sudoer: "root", name: "automq-vultr-0" },
    { provider:"vultr", node_id: "1", role: null, index: 1, ip: "203.0.113.11", vpc_ip: "10.40.0.4", user: "root", sudoer: "root", name: "automq-vultr-1" },
    { provider:"vultr", node_id: "2", role: null, index: 2, ip: "203.0.113.12", vpc_ip: "10.40.0.5", user: "root", sudoer: "root", name: "automq-vultr-2" },
  ],
};

const applied = (overrides: Opts = {}) =>
  fixture({ profile: "automq-vultr", "colors-compute/cluster": params, ...overrides });

// ~/.ssh redirection: ONCE's ssh module and this package's ssh-config both read
// $HOME at call time, exactly so tests can point them at a fresh temporary home.
let savedHome: string | undefined;
let home: string;
beforeEach(() => {
  savedHome = process.env.HOME;
  home = mkdtempSync(join(tmpdir(), "automq-red-test"));
  process.env.HOME = home;
});
afterEach(() => {
  process.env.HOME = savedHome;
  rmSync(home, { recursive: true, force: true });
});

// --- the cluster's derived identity ------------------------------------------

describe("cluster", () => {
  const opts = fixture({ profile: "automq-vultr" });



  test("the machine label, the node id and the broker ordinal are one number", () => {
    expect(cluster.machineNames(opts)).toEqual(
      ["automq-vultr-0", "automq-vultr-1", "automq-vultr-2"]);
    expect(cluster.brokerNames(opts)).toEqual(
      ["b0.automq.example.com", "b1.automq.example.com", "b2.automq.example.com"]);
  });

  test("the machine is named after the profile, unless overridden", () => {
    expect(cluster.computeName(opts)).toBe("automq-vultr");
    expect(cluster.computeName({ ...opts, "vultr-name": "legacy" })).toBe("legacy");
    // A blank override is not an override.
    expect(cluster.computeName({ ...opts, "vultr-name": "  " })).toBe("automq-vultr");
  });

  test("the certificate covers the bootstrap name and every broker", () => {
    // A client's first connection is to the bootstrap name and every later one
    // is to a broker name, so a SAN list missing either half fails for exactly
    // the client that happens to be routed there.
    expect(cluster.certificateNames(opts)).toEqual([
      "automq.example.com",
      "b0.automq.example.com", "b1.automq.example.com", "b2.automq.example.com",
    ]);
  });

  test("the quorum is built from private addresses only", () => {
    const voters = cluster.quorumVoters(opts, cluster.nodes(opts, params));
    expect(voters).toBe("0@10.40.0.3:9093,1@10.40.0.4:9093,2@10.40.0.5:9093");
    expect(voters).not.toContain("203.0.113");
  });

  test("listeners bind privately and advertise publicly", () => {
    const node = cluster.nodes(opts, params)[0]!;
    expect(cluster.listeners(opts, node)).toBe(
      "CONTROLLER://10.40.0.3:9093,INTERNAL://10.40.0.3:9094,EXTERNAL://0.0.0.0:9092");
    // Kafka rejects a controller entry in advertised.listeners.
    expect(cluster.advertisedListeners(opts, node)).not.toContain("CONTROLLER");
    expect(cluster.advertisedListeners(opts, node)).toBe(
      "INTERNAL://10.40.0.3:9094,EXTERNAL://b0.automq.example.com:9092");
  });

  test("a build renders fixed documentation-range addresses", () => {
    // ONCE's fallbacks: TEST-NET-1 publicly, the VPC subnet privately, offset
    // 10 — so the goldens mean the same thing on every workstation.
    const list = cluster.nodes({...opts,"red/event":"build"});
    expect(list.length).toBe(3);
    expect(list.map((n) => n.ip)).toEqual(["192.0.2.10", "192.0.2.11", "192.0.2.12"]);
    expect(list.map((n) => n["vpc-ip"])).toEqual(["10.40.0.10", "10.40.0.11", "10.40.0.12"]);
    expect(list.map((n) => n.name)).toEqual(["automq-vultr-0", "automq-vultr-1", "automq-vultr-2"]);
    expect(list.map((n) => n["broker-name"])).toEqual(
      ["b0.automq.example.com", "b1.automq.example.com", "b2.automq.example.com"]);
  });

  test("nodes on a real run come from state, in the renderers' spelling", () => {
    // ONCE hands back every node as recorded, `vpc_ip` and all; this package's
    // templates were written against `vpc-ip`, so the wrapper respells it and
    // adds the broker name. Nothing else is touched: the name is the label the
    // template gave the instance, never recomputed, and extension fields ride
    // through.
    const recorded: any = {
      ...params,
      nodes: [
        { ...params.nodes![0]!, extra: "kept" },
        { ...params.nodes![1]!, name: "renamed-in-console" },
        params.nodes![2]!,
      ],
    };
    const list = cluster.nodes(opts, recorded);
    expect(list.map((n) => n.ip)).toEqual(["203.0.113.10", "203.0.113.11", "203.0.113.12"]);
    expect(list.map((n) => n["vpc-ip"])).toEqual(["10.40.0.3", "10.40.0.4", "10.40.0.5"]);
    expect(list.some((n) => "vpc_ip" in n)).toBe(false);
    expect(list[1]!.name).toBe("renamed-in-console");
    expect(list[0]!.extra).toBe("kept");
    expect(list[1]!["broker-name"]).toBe("b1.automq.example.com");
  });

  test("principals are distinct and the client is not a superuser", () => {
    expect(cluster.scramPrincipals(opts)).toEqual(["automq-admin", "automq-broker", "automq"]);
    // The controller principal is absent from the SCRAM set on purpose.
    expect(cluster.scramPrincipals(opts)).not.toContain("automq-controller");
    const supers = cluster.superUsers(opts);
    expect(supers).toContain("User:automq-admin");
    expect(supers).toContain("User:automq-controller");
    expect(/User:automq;|User:automq$/.test(supers)).toBe(false);
  });

  test("the client ACLs grant no administration", () => {
    const acls = cluster.clientAcls(opts);
    const operations = new Set(acls.flatMap((acl) => acl.operations));
    expect(new Set(acls.map((acl) => acl["resource-type"]))).toEqual(new Set(["topic", "group"]));
    expect(acls.every((acl) => acl["pattern-type"] === "prefixed")).toBe(true);
    expect(operations).toEqual(new Set(["Describe", "Read", "Write"]));
    for (const denied of ["Create", "Alter", "ClusterAction"]) {
      expect(operations.has(denied)).toBe(false);
    }
  });
});

// --- desired state -----------------------------------------------------------

describe("validate", () => {
  test("both fixtures are valid", () => {
    expect(validate.stateErrors(fixture())).toEqual([]);
    expect(validate.stateErrors(optout())).toEqual([]);
  });

  test("the machine key is not required", () => {
    // The standard makes absence meaningful: requiring vultr-ssh-keys would
    // make every conforming keygen deployment invalid.
    expect(validate.required).not.toContain("vultr-ssh-keys");
    expect(validate.required).not.toContain("vultr-name");
  });

  test("absent machine key selects keygen", () => {
    expect(validate.keygen(fixture())).toBe(true);
    expect(validate.keygen(optout())).toBe(false);
  });

  test("every missing key is reported at once", () => {
    // Exit code 2 means "here is everything that is wrong", not "here is the
    // first thing": an operator should need one run to fix a file, not six.
    const incomplete = fixture();
    delete incomplete["automq-host"];
    delete incomplete["vultr-region"];
    delete incomplete["automq-cluster-id"];
    const errors = validate.stateErrors(incomplete);
    expect(errors.length).toBe(3);
    expect(errors.every((error) => error.endsWith(" is required"))).toBe(true);
  });

  test("the image must be pinned by digest", () => {
    // A tag alone lets a silent retag change behaviour at run time.
    expect(validate.stateErrors(fixture({ "automq-image": "automqinc/automq:1.7.4" }))
      .some((error) => error.includes("pinned by digest"))).toBe(true);
    expect(validate.stateErrors(fixture()).some((error) => error.includes("pinned by digest")))
      .toBe(false);
  });

  test("an even quorum is refused", () => {
    // Four voters tolerate exactly one failure, the same as three, while adding
    // a node that can fail. That is strictly worse, so it is not offered.
    const odd = (count: unknown) =>
      validate.stateErrors(fixture({ "automq-node-count": count }));
    expect(odd(4).some((error) => error.includes("must be odd"))).toBe(true);
    expect(odd(5).some((error) => error.includes("must be odd"))).toBe(false);
    // One node is a legitimate development shape.
    expect(odd(1).some((error) => error.includes("must be odd"))).toBe(false);
    expect(odd(0).some((error) => error.includes("from 1 to 9"))).toBe(true);
    expect(odd("three").some((error) => error.includes("must be an integer"))).toBe(true);
  });

  test("the cluster id must be a real Kafka uuid", () => {
    for (const bad of ["not-a-uuid", "VrUQI4OSR0y5vnTrGiKsx"]) {
      expect(validate.stateErrors(fixture({ "automq-cluster-id": bad }))
        .some((error) => error.includes("base64 UUID"))).toBe(true);
    }
  });

  test("storage must not be shared", () => {
    // The two roles write different key layouts and cannot share a bucket.
    expect(validate.stateErrors(fixture({ "automq-ops-r2-bucket": "automq-fixture-data" }))
      .some((error) => error.includes("must be different buckets"))).toBe(true);
    // And neither may be the state bucket, since AutoMQ writes at the root.
    expect(validate.stateErrors(fixture({ "automq-data-r2-bucket": "fixture-state" }))
      .some((error) => error.includes("must not be the OpenTofu state bucket"))).toBe(true);
  });

  test("listener ports must differ and be ports", () => {
    expect(validate.stateErrors(fixture({ "automq-internal-port": 9092 }))
      .some((error) => error.includes("must differ"))).toBe(true);
    expect(validate.stateErrors(fixture({ "automq-kafka-port": 70000 }))
      .some((error) => error.includes("from 1 to 65535"))).toBe(true);
  });

  test("principals must be distinct", () => {
    // Four principals share one namespace in the metadata log, and three of them
    // are superusers: a collision is a privilege escalation, not a typo.
    expect(validate.stateErrors(fixture({ "automq-admin-user": "automq" }))
      .some((error) => error.includes("must all differ"))).toBe(true);
    expect(validate.stateErrors(fixture()).some((error) => error.includes("must all differ")))
      .toBe(false);
  });

  test("the destroy guard accepts the one-run override", () => {
    // The override arrives through the same COLORS_PAR overlay as every other
    // parameter, so rejecting `false` here would make the documented way to
    // destroy this deployment impossible. The delete-time validator is what
    // refuses a destroy while the guard is still true.
    expect(validate.stateErrors(fixture({ "compute-prevent-destroy": false }))
      .some((error) => error.includes("prevent-destroy"))).toBe(false);
    expect(validate.stateErrors(fixture({ "compute-prevent-destroy": "yes" }))
      .some((error) => error.includes("must be true or false"))).toBe(true);
  });

  test("the shared library validates provider capabilities and firewall requests",()=>{
    expect(validate.stateErrors(fixture({'provider-compute':'digitalocean'}))).toContain(':digitalocean-image is required');
    expect(validate.stateErrors(fixture({'vultr-ssh-sources':[]})).length).toBeGreaterThan(0);
    expect(validate.stateErrors(fixture({'vultr-ssh-sources':['1.2.3.4']})).length).toBeGreaterThan(0);
    expect(validate.stateErrors(fixture({'vultr-kafka-sources':[]}))).toEqual([]);
    expect(validate.stateErrors(fixture({'vultr-vpc-subnet':'10.40.0.1/24'})).length).toBeGreaterThan(0);
  });

  test("the profile overlay is refused", () => {
    expect(validate.envErrors({ COLORS_PAR_PROFILE: "somewhere-else" }).length).toBe(1);
    expect(validate.envErrors({})).toEqual([]);
  });

  test("secrets are asked for only when they are needed", () => {
    const none = fixture();
    // A create needs the storage keys as well as the provider keys.
    expect(validate.secretErrors(none, "create")
      .some((error) => error.includes("AUTOMQ_R2_ACCESS_KEY_ID"))).toBe(true);
    // A delete converges nothing, so demanding storage keys would only lock the
    // exit.
    expect(validate.secretErrors(none, "delete")
      .some((error) => error.includes("AUTOMQ_R2"))).toBe(false);
    expect(validate.secretErrors(none, "delete")
      .some((error) => error.includes("VULTR_API_KEY"))).toBe(false);
  });



  test("tools are checked without touching the network", async () => {
    const runner = async (args: string[]) =>
      args.at(-1) === "curl" ? { exit: 1, out: "", err: "" } : { exit: 0, out: "", err: "" };
    const errors = await validate.runtimeErrors(fixture(), runner);
    expect(errors.some((error) => error.includes("curl"))).toBe(true);
  });


});

// --- the machine keypair -----------------------------------------------------

describe("ssh", () => {
  test("a build never names the operator's home", () => {
    // Committed goldens must mean the same thing on every workstation, so a
    // build renders a fixed placeholder rather than reading ~/.ssh.
    const opts = ssh.withMachineKey(fixture({ "red/event": "build" }));
    expect(opts["ssh-private-key-path"]).toBe("/home/build-placeholder/.ssh/automq-fixture");
    expect(opts["ssh-public-key-path"]).toBe("/home/build-placeholder/.ssh/automq-fixture.pub");
    expect(String(process.env.HOME)).not.toContain("build-placeholder");
  });

  test("a dry-run is held to the same rule as a build", () => {
    // A dry-run is a create that touches nothing; testing the event alone would
    // let it reach the real key path.
    expect(ssh.renderedOnly({ "red/event": "build" })).toBe(true);
    expect(ssh.renderedOnly({ "red/event": "create", "red/dry-run": true })).toBe(true);
    expect(ssh.renderedOnly({ "red/event": "create" })).toBe(false);
  });

  test("opt-out opts pass through untouched", () => {
    const opts = optout({ "red/event": "build" });
    expect(ssh.withMachineKey(opts)).toEqual(opts);
    expect(ssh.withMachineKey(opts)["ssh-private-key-path"]).toBeUndefined();
  });
});

// --- ~/.ssh/config -----------------------------------------------------------

describe("ssh-config", () => {
  const opts = fixture({ profile: "automq-vultr" });

  test("the deployment claims one alias per node and the bare profile", () => {
    // `ssh automq-vultr` is what the standard promises; the numbered aliases are
    // what make a quorum operable, since half of running one is reaching a
    // specific member.
    expect(sshConfig.aliases(opts)).toEqual(
      ["automq-vultr", "automq-vultr-0", "automq-vultr-1", "automq-vultr-2"]);
  });

  test("the identity file stays unexpanded", () => {
    expect(sshConfig.identityFile(opts)).toBe("~/.ssh/automq-vultr");
  });

  test("a foreign stanza is found for any alias, not just the first", () => {
    const lines = "Host something\n  HostName 1.2.3.4\n\nHost automq-vultr-2\n  HostName 5.6.7.8\n"
      .split("\n");
    expect(sshConfig.foreignStanzaLine(lines, "automq-vultr")).toBeUndefined();
    expect(sshConfig.foreignStanzaLine(lines, "automq-vultr-2")).toBe(4);
  });

  test("our own managed block is not foreign for any alias in it", () => {
    // One block, marked with the profile, holding a stanza per node. Deriving
    // the marker from the stanza being searched — which a single-node package
    // can get away with — makes the check hunt for `# BEGIN automq-vultr-0 …`,
    // never find it, and refuse to converge because of a block this package
    // wrote itself.
    const lines = [
      "# BEGIN automq-vultr ANSIBLE MANAGED BLOCK",
      "Host automq-vultr", "  HostName 1.2.3.4",
      "Host automq-vultr-0", "  HostName 1.2.3.4",
      "Host automq-vultr-1", "  HostName 1.2.3.5",
      "Host automq-vultr-2", "  HostName 1.2.3.6",
      "# END automq-vultr ANSIBLE MANAGED BLOCK",
    ];
    for (const alias of sshConfig.aliases(opts)) {
      expect(sshConfig.foreignStanzaLine(lines, alias, "automq-vultr")).toBeUndefined();
    }
  });

  test("a node stanza outside our block is still foreign", () => {
    const lines = [
      "# BEGIN automq-vultr ANSIBLE MANAGED BLOCK",
      "Host automq-vultr", "  HostName 1.2.3.4",
      "# END automq-vultr ANSIBLE MANAGED BLOCK",
      "Host automq-vultr-1", "  HostName 9.9.9.9",
    ];
    expect(sshConfig.foreignStanzaLine(lines, "automq-vultr-1", "automq-vultr")).toBe(5);
  });

  test("a global option above the first Host blocks the run", () => {
    // The block is inserted at BOF, so it would capture such an option into one
    // stanza and silently narrow a setting that applied to every host.
    expect(sshConfig.leadingOptionLine(["ServerAliveInterval 60", "Host x"])).toBe(1);
    expect(sshConfig.leadingOptionLine(["# a comment", "", "Host x", "  User root"]))
      .toBeUndefined();
    // An option below a Host line belongs to that host and is fine.
    expect(sshConfig.leadingOptionLine(["Host x", "  ServerAliveInterval 60"])).toBeUndefined();
  });

  test("the refusal is reported as a failed step", () => {
    const refused = sshConfig.preflight(opts, {
      adoptError: () => "no",
      placementError: () => undefined,
    });
    expect(refused["red/exit"]).toBe(1);
    expect(refused["red/err"]).toBe("no");
  });
});

// --- stages ------------------------------------------------------------------

describe("tools", () => {
  const opts = applied();

  test("the adopted cluster reaches the renderers respelled", () => {
    // ONCE records `vpc_ip` and `ssh_key_id` with underscores — the latter is
    // the SSH Keypair Standard's contract with ONCE's create preflight and must
    // stay verbatim on the params map. The renderers read `vpc-ip`, so the node
    // wrapper respells that one key and nothing else.
    const [node] = tools.nodes(opts);
    expect((opts["colors-compute/cluster"] as any).ssh_key_id).toBe("7692e92a");
    expect(node!["vpc-ip"]).toBe("10.40.0.3");
    expect(node!.vpc_ip).toBeUndefined();
    expect(node!.name).toBe("automq-vultr-0");
  });



  test("the zone is the registrable domain", () => {
    expect(tools.zone(opts)).toBe("example.com");
  });

  test("DNS records are never proxied", () => {
    // Cloudflare's proxy terminates HTTP. Kafka is raw TCP, so a proxied record
    // publishes an address that speaks the wrong protocol entirely.
    const records = JSON.parse(tools.dnsJson(opts, tools.nodes(opts)))
      .resource.cloudflare_dns_record as Record<string, Record<string, unknown>>;
    expect(Object.keys(records).length).toBe(6);
    expect(Object.values(records).every((record) => record.proxied === false)).toBe(true);
    // The bootstrap name carries every node's address.
    expect(new Set(["bootstrap_0", "bootstrap_1", "bootstrap_2"]
      .map((key) => records[key]!.content)))
      .toEqual(new Set(["203.0.113.10", "203.0.113.11", "203.0.113.12"]));
    // Each broker name points at its own node.
    expect(records.broker_2!.content).toBe("203.0.113.12");
    expect(records.broker_2!.name).toBe("b2.automq.example.com");
  });

  test("the inventory carries per-node facts only", () => {
    const hosts = JSON.parse(tools.inventory(opts, tools.nodes(opts)))
      .all.children.automq.hosts as Record<string, Record<string, unknown>>;
    expect(Object.keys(hosts).length).toBe(3);
    // Exactly one node issues certificates, so only one holds the DNS token.
    expect(Object.values(hosts).filter((host) => host.automq_cert_issuer).length).toBe(1);
    expect(hosts["automq-vultr-0"]!.automq_cert_issuer).toBe(true);
    // The quorum string is not per-node: three nodes must not disagree.
    expect(Object.values(hosts).some((host) => "automq_quorum_voters" in host)).toBe(false);
  });

  test("ssh config hosts point the bare alias at node zero", () => {
    const hosts = tools.sshConfigHosts(opts, tools.nodes(opts));
    expect(hosts[0]).toMatchObject({ name: "automq-vultr", ip: "203.0.113.10" });
    expect(hosts.map((host) => host.name)).toEqual(
      ["automq-vultr", "automq-vultr-0", "automq-vultr-1", "automq-vultr-2"]);
    expect(hosts.map((host) => host.ip)).toEqual(
      ["203.0.113.10", "203.0.113.10", "203.0.113.11", "203.0.113.12"]);
  });

  test("the ansible data carries no credential", () => {
    // Secrets reach the host as lookup('env', …) expressions written literally
    // into the playbook. Anything in this map would land in .colors/ and in a
    // committed golden.
    const data = tools.ansibleData(opts);
    for (const [key, value] of Object.entries(data)) {
      if (typeof value !== "string") continue;
      expect(/secret|password|token|access.key/i.test(key)).toBe(false);
    }
    expect(data["quorum-voters"]).toBe("0@10.40.0.3:9093,1@10.40.0.4:9093,2@10.40.0.5:9093");
  });





  test("the ansible stage renders the whole cluster tree", () => {
    const targets = tools.ansibleSpecs(opts).map((spec) => String(spec.target));
    for (const file of ["main.yml", "cleanup.yml", "compose.yml", "server.properties",
                        "store.py", "scram.sh", "cert.sh", "smoke.sh", "inventory.json"]) {
      expect(targets.some((target) => target.endsWith(`/${file}`))).toBe(true);
    }
  });

  test("a delete with no compute in state stops instead of converging", async () => {
    // A readable state without compute adopted nothing: there is nothing to
    // stop, and the cleanup play would only fail against the placeholder
    // addresses.
    const result = await tools.ansibleStep(fixture({ "red/event": "delete" }));
    expect(result["red/exit"]).toBe(0);
  });

  test("each tofu stage keys its own state", () => {
    expect(tools.infrastructureTool).not.toBe(tools.dnsTool);
    for (const tool of [tools.infrastructureTool, tools.dnsTool, tools.ansibleTool,
                        tools.ansibleLocalTool, tools.acceptanceTool]) {
      expect(tool.startsWith("automq-")).toBe(true);
    }
  });
});

// --- the graph ---------------------------------------------------------------

describe("workflow", () => {
  function chain(event: string): string[] {
    const seen: string[] = [];
    let step = "automq/start";
    for (;;) {
      const wired = workflow.wireFn(step, { "red/event": event });
      const next = wired?.[1];
      if (!next) return seen;
      seen.push(next);
      step = next;
    }
  }

  test("create resolves addresses before it needs them", () => {
    // DNS needs the compute output; the brokers advertise names that must
    // already resolve, and the certificate is issued for those names during the
    // play. The order is the dependency, not a preference.
    expect(chain("create")).toEqual([
      "automq/infrastructure", "automq/ssh-config", "automq/dns",
      "automq/ansible", "automq/acceptance",
    ]);
  });

  test("delete unwinds in the order that keeps access", () => {
    // The ssh_config block goes before the destroy and the keypair after it: a
    // stale block is harmless, a key that predeceases its host locks the
    // operator out of machines that still exist. DNS goes before the destroy so
    // no record survives pointing at an address Vultr can hand to someone else.
    expect(chain("delete")).toEqual([
      "automq/ansible", "automq/ssh-config", "automq/dns",
      "automq/infrastructure",
    ]);
  });

  test("validate answers the question without rendering anything", () => {
    // It must work on a fresh checkout with no keypair and no state. Falling
    // through to the create chain would plan a compute stage that reads the
    // machine public key, so the check would fail on exactly the case it exists
    // to serve.
    expect(chain("validate")).toEqual([]);
    expect(workflow.wireFn("automq/infrastructure", { "red/event": "validate" }))
      .toBeUndefined();
  });

  test("the destroy guard is desired state, not a flag", async () => {
    const result = await workflow.startStep(
      { ...fixture(), "red/event": "delete", "compute-prevent-destroy": true }, {});
    expect(result["red/exit"]).toBe(2);
    expect(String(result["red/err"])).toContain("compute destruction is protected");
  });

  test("defaults cover every key an operator should not have to write", () => {
    expect(workflow.defaults["automq-node-count"]).toBe(3);
    expect(workflow.defaults["automq-kafka-port"]).toBe(9092);
    expect(workflow.defaults["provider-compute"]).toBe("vultr");
    // But the guard defaults to protecting the deployment.
    expect(workflow.defaults["compute-prevent-destroy"]).toBe(true);
  });

  test("every side-effecting step is skipped by a dry run", () => {
    for (const step of ["automq/infrastructure", "automq/dns", "automq/ssh-config",
                        "automq/ansible", "automq/acceptance", "automq/ssh-cleanup"]) {
      expect(workflow.sideEffecting).toContain(step);
    }
  });

  // --- the lifecycle against the compute state ------------------------------

  // The compute state is read once per run, through the injectable reader, on
  // a real create or delete. Every lifecycle test stubs it: undefined is a
  // readable state holding no compute, a map is a recorded `params`, and a
  // throw is a backend that cannot be read. The Vultr API probe is stubbed too
  // — these tests are about the state, and they must not reach the network.
  const quiet = async () => [] as string[];
});

describe('shared compute integration',()=>{
 test('partial and unavailable inventories refuse placeholder convergence',()=>{
  expect(()=>cluster.nodes(fixture({'red/event':'create'}))).toThrow('unavailable');
  expect(()=>cluster.nodes(applied(),{...params,nodes:params.nodes.slice(0,2)})).toThrow('missing node');
  expect(()=>cluster.nodes(applied(),{...params,nodes:params.nodes.map((n:any,i:number)=>i===2?{...n,ip:null}:n)})).toThrow('incomplete node');
 });
 test('build writes shared and individual node documents without private keys',async()=>{
  const opts=fixture({'red/event':'build',workdir:home});const result=await tools.infrastructureStep(opts);
  expect(result['red/exit']).toBe(0);expect(result['colors-compute/cluster'].nodes).toHaveLength(3);
  const {readdirSync,existsSync}=await import('node:fs');const dir=tools.toolDir(opts,tools.infrastructureTool);
  expect(readdirSync(join(dir,'nodes')).sort()).toEqual(['0','1','2']);expect(existsSync(join(dir,'shared'))).toBe(true);
  expect(existsSync(join(home,'.ssh'))).toBe(false);expect(result['ssh-private-key-path']).toBe('/home/build-placeholder/.ssh/automq-fixture');
 });
 test('delete adopts only library inventory and stops retired deployments',async()=>{
  const values=fixture({'red/event':'delete','compute-prevent-destroy':false,'vultr-api-key':'fixture','cloudflare-api-token':'fixture','r2-access-key-id':'fixture','r2-secret-access-key':'fixture'});
  const deps={runtimeErrors:async()=>[],reader:async()=>({status:'present',cluster:params,key:{private_key_path:'/owned/key'}})};
  const adopted=await workflow.startStep(values,{},deps);expect(adopted['red/exit']).toBe(0);expect(adopted['colors-compute/cluster']).toEqual(params);expect(adopted['ssh-private-key-path']).toBe('/owned/key');
  const error=await workflow.startStep(values,{}, {...deps,reader:async()=>({status:'error'})});expect(error['red/exit']).toBe(1);
  const retired=await workflow.startStep(values,{}, {...deps,reader:async()=>({status:'destroyed'})});expect(retired['automq/already-destroyed']).toBe(true);
 });
});

test('DNS alone receives R2 backend credential environment',async()=>{
 const {spyOn}=await import('bun:test');const tofu=await import('red/tofu');let captured:any;
 const spy=spyOn(tofu,'tofuWithSpec').mockImplementation(async(opts:any,_specs:any,config:any)=>{captured=config.env;return {...opts,'red/exit':0};});
 const opts=applied({'red/event':'create','r2-access-key-id':'synthetic-r2-id','r2-secret-access-key':'synthetic-r2-secret','cloudflare-api-token':'synthetic-dns'});const before=structuredClone(opts),environment={...process.env};
 try{await tools.dnsStep(opts);expect(captured).toEqual({AWS_ACCESS_KEY_ID:'synthetic-r2-id',AWS_SECRET_ACCESS_KEY:'synthetic-r2-secret',CLOUDFLARE_API_TOKEN:'synthetic-dns'});expect(opts).toEqual(before);expect(process.env).toEqual(environment);expect(validate.tofuEnv(opts,'provider-compute')).toEqual({});}finally{spy.mockRestore();}
});
test('compute credentials are checked by lifecycle after state, not application start',()=>{
 const opts=fixture();expect(validate.secretErrors(opts,'create').some(e=>e.includes('VULTR_API_KEY'))).toBe(false);expect(validate.secretErrors(opts,'delete').some(e=>e.includes('VULTR_API_KEY'))).toBe(false);expect(validate.secretErrors(opts,'validate').some(e=>e.includes('VULTR_API_KEY'))).toBe(true);
});
