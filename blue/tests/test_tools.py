import json
import re

from conftest import PARAMS, applied, fixture
from package_automq_blue import tools

opts = applied()


def test_the_adopted_cluster_reaches_the_renderers_respelled():
    # ONCE records `vpc_ip` and `ssh_key_id` with underscores — the latter is
    # the SSH Keypair Standard's contract with ONCE's create preflight and must
    # stay verbatim on the params map. The renderers read `vpc-ip`, so the node
    # wrapper respells that one key and nothing else.
    node = tools.nodes(opts)[0]
    assert opts["once/cluster"]["ssh_key_id"] == "7692e92a"
    assert node["vpc-ip"] == "10.40.0.3"
    assert "vpc_ip" not in node
    assert node["name"] == "automq-vultr-0"


def test_the_compute_stage_refuses_anything_but_the_whole_cluster():
    # The real create's infrastructure step hands its tofu outputs here. No
    # `params` output at all, or a node set that is partial or incomplete, is
    # exit 1 with ONCE's message rather than a quorum string against
    # 192.0.2.10; the whole cluster lands under `once/cluster`.
    def result(p):
        return {"blue/exit": 0, "tofu/outputs": {"params": p} if p else {}}

    none = tools.resolved_cluster(opts, result(None))
    assert none["blue/exit"] == 1
    assert none["blue/err"] == ("compute produced no params output; refusing to "
                                "converge against the documentation addresses")
    partial = tools.resolved_cluster(opts, result({**PARAMS, "nodes": PARAMS["nodes"][:2]}))
    assert partial["blue/exit"] == 1
    assert partial["blue/err"] == "the compute stage did not report nodes this package declares: 2"
    incomplete = tools.resolved_cluster(opts, result({**PARAMS, "nodes": [
        PARAMS["nodes"][0], PARAMS["nodes"][1], {**PARAMS["nodes"][2], "ip": None}]}))
    assert incomplete["blue/exit"] == 1
    assert "did not report a complete node" in incomplete["blue/err"]
    whole = tools.resolved_cluster(opts, result(PARAMS))
    assert whole["blue/exit"] == 0
    assert whole["once/cluster"] == PARAMS


def test_the_zone_is_the_registrable_domain():
    assert tools.zone(opts) == "example.com"


def test_dns_records_are_never_proxied():
    # Cloudflare's proxy terminates HTTP. Kafka is raw TCP, so a proxied record
    # publishes an address that speaks the wrong protocol entirely.
    records = json.loads(tools.dns_json(opts, tools.nodes(opts)))["resource"]["cloudflare_dns_record"]
    assert len(records) == 6, "three bootstrap records and one per broker"
    assert all(record["proxied"] is False for record in records.values())
    # The bootstrap name carries every node's address.
    assert {records[f"bootstrap_{i}"]["content"] for i in range(3)} == {
        "203.0.113.10", "203.0.113.11", "203.0.113.12"}
    # Each broker name points at its own node.
    assert records["broker_2"]["content"] == "203.0.113.12"
    assert records["broker_2"]["name"] == "b2.automq.example.com"


def test_the_inventory_carries_per_node_facts_only():
    hosts = json.loads(tools.inventory(opts, tools.nodes(opts)))["all"]["children"]["automq"]["hosts"]
    assert len(hosts) == 3
    # Exactly one node issues certificates, so only one holds the DNS token.
    assert len([h for h in hosts.values() if h["automq_cert_issuer"]]) == 1
    assert hosts["automq-vultr-0"]["automq_cert_issuer"] is True
    # The quorum string is not per-node: three nodes must not disagree.
    assert not any("automq_quorum_voters" in host for host in hosts.values())


def test_ssh_config_hosts_point_the_bare_alias_at_node_zero():
    hosts = tools.ssh_config_hosts(opts, tools.nodes(opts))
    assert hosts[0] == {"name": "automq-vultr", "ip": "203.0.113.10"}
    assert [h["name"] for h in hosts] == [
        "automq-vultr", "automq-vultr-0", "automq-vultr-1", "automq-vultr-2"]
    assert [h["ip"] for h in hosts] == [
        "203.0.113.10", "203.0.113.10", "203.0.113.11", "203.0.113.12"]


def test_the_ansible_data_carries_no_credential():
    # Secrets reach the host as lookup('env', …) expressions written literally
    # into the playbook. Anything in this map would land in .colors/ and in a
    # committed golden.
    data = tools.ansible_data(opts)
    assert not [k for k, v in data.items()
                if isinstance(v, str) and re.search(r"secret|password|token|access.key", k, re.I)]
    assert data["quorum-voters"] == "0@10.40.0.3:9093,1@10.40.0.4:9093,2@10.40.0.5:9093"


def test_the_compute_stage_renders_every_value_its_template_names():
    # A template key that is absent renders as empty rather than failing, so the
    # firewall rule shipped `port = ""` and only the provider rejected it.
    data = tools.infrastructure_data(opts)
    assert data["kafka-port"] == 9092
    assert data["node-count"] == 3
    assert data["compute-name"] == "automq-vultr"
    assert all(str(data[k]).strip() for k in
               ["kafka-port", "node-count", "compute-name", "ssh-sources-hcl",
                "kafka-sources-hcl", "controller-port", "internal-port"])
    # Without a rule for these, a Vultr firewall group silently drops TCP on the
    # private interface while still passing ICMP, and the cluster never elects a
    # controller.
    assert data["controller-port"] == 9093
    assert data["internal-port"] == 9094


def test_cidr_lists_survive_both_yaml_and_string_forms():
    assert tools.cidrs({"vultr-ssh-sources": ["0.0.0.0/0", "::/0"]}, "vultr-ssh-sources") == [
        "0.0.0.0/0", "::/0"]
    assert tools.cidrs({"x": "1.2.3.0/24"}, "x") == ["1.2.3.0/24"]


def test_the_ansible_stage_renders_the_whole_cluster_tree():
    targets = [str(s["target"]) for s in tools.ansible_specs(opts)]
    for file in ["main.yml", "cleanup.yml", "compose.yml", "server.properties",
                 "store.py", "scram.sh", "cert.sh", "smoke.sh", "inventory.json"]:
        assert any(target.endswith(f"/{file}") for target in targets), file


async def test_a_delete_with_no_compute_in_state_stops_instead_of_converging():
    # A readable state without compute adopted nothing: there is nothing to
    # stop, and the cleanup play would only fail against the placeholder
    # addresses.
    result = await tools.ansible_step(fixture({"blue/event": "delete"}))
    assert result["blue/exit"] == 0


def test_each_tofu_stage_keys_its_own_state():
    assert tools.infrastructure_tool != tools.dns_tool
    assert all(tool.startswith("automq-") for tool in
               [tools.infrastructure_tool, tools.dns_tool, tools.ansible_tool,
                tools.ansible_local_tool, tools.acceptance_tool])
