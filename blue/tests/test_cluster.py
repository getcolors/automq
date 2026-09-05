import re

from conftest import PARAMS, fixture
from package_automq_blue import cluster
from package_once_blue import compute_cluster as once_cluster

opts = fixture({"profile": "automq-vultr"})


def test_the_spec_describes_one_homogeneous_vultr_cluster():
    # The Compute Cluster Standard's spec-content test: the shape ONCE is handed
    # is data, and this is what that data must say.
    assert once_cluster.spec_errors(cluster.spec) == []
    assert cluster.spec["roles"] == [
        {"role": None, "count_key": "automq-node-count", "count": 3}]
    # The bare profile alias reaches node 0.
    assert once_cluster.entry_id(cluster.spec) == {"role": None, "index": 0}
    assert cluster.spec["sources"] == {"non_empty": ["ssh-sources"],
                                       "may_be_empty": ["kafka-sources"]}
    assert cluster.spec["default"] == "vultr"
    assert list(cluster.spec["registry"]) == ["vultr"]
    # The quorum crosses a VPC this package creates from vultr-vpc-subnet.
    assert cluster.spec["registry"]["vultr"]["network"] == {
        "mode": "created", "key": "vultr-vpc-subnet"}
    # A created network cuts its fallbacks from the CIDR key, not a stand-in.
    assert "fallback_subnet" not in cluster.spec
    assert cluster.spec["registry"]["vultr"]["secrets"] == ["vultr-api-key"]


def test_names_derive_from_one_index():
    # The machine label, the node id and the broker ordinal are one number.
    assert cluster.machine_names(opts) == [
        "automq-vultr-0", "automq-vultr-1", "automq-vultr-2"]
    assert cluster.broker_names(opts) == [
        "b0.automq.example.com", "b1.automq.example.com", "b2.automq.example.com"]


def test_compute_name_prefers_the_profile():
    assert cluster.compute_name(opts) == "automq-vultr"
    assert cluster.compute_name({**opts, "vultr-name": "legacy"}) == "legacy"
    # A blank override is not an override.
    assert cluster.compute_name({**opts, "vultr-name": "  "}) == "automq-vultr"


def test_certificate_covers_the_bootstrap_name_and_every_broker():
    # A client's first connection is to the bootstrap name and every later one
    # is to a broker name, so a SAN list missing either half fails for exactly
    # the client that happens to be routed there.
    assert cluster.certificate_names(opts) == [
        "automq.example.com",
        "b0.automq.example.com", "b1.automq.example.com", "b2.automq.example.com"]


def test_quorum_is_built_from_private_addresses():
    voters = cluster.quorum_voters(opts, cluster.nodes(opts, PARAMS))
    assert voters == "0@10.40.0.3:9093,1@10.40.0.4:9093,2@10.40.0.5:9093"
    assert "203.0.113" not in voters


def test_listeners_bind_privately_and_advertise_publicly():
    node = cluster.nodes(opts, PARAMS)[0]
    assert cluster.listeners(opts, node) == (
        "CONTROLLER://10.40.0.3:9093,INTERNAL://10.40.0.3:9094,EXTERNAL://0.0.0.0:9092")
    # Kafka rejects a controller entry in advertised.listeners.
    assert "CONTROLLER" not in cluster.advertised_listeners(opts, node)
    assert cluster.advertised_listeners(opts, node) == (
        "INTERNAL://10.40.0.3:9094,EXTERNAL://b0.automq.example.com:9092")


def test_a_build_renders_fixed_addresses():
    # ONCE's fallbacks: TEST-NET-1 publicly, the VPC subnet privately, offset
    # 10 — so the goldens mean the same thing on every workstation.
    nodes = cluster.nodes(opts)
    assert len(nodes) == 3
    assert [n["ip"] for n in nodes] == ["192.0.2.10", "192.0.2.11", "192.0.2.12"]
    assert [n["vpc-ip"] for n in nodes] == ["10.40.0.10", "10.40.0.11", "10.40.0.12"]
    assert [n["name"] for n in nodes] == ["automq-vultr-0", "automq-vultr-1", "automq-vultr-2"]
    assert [n["broker-name"] for n in nodes] == [
        "b0.automq.example.com", "b1.automq.example.com", "b2.automq.example.com"]


def test_nodes_on_a_real_run_come_from_state_in_the_renderers_spelling():
    # ONCE hands back every node as recorded, `vpc_ip` and all; this package's
    # templates were written against `vpc-ip`, so the wrapper respells it and
    # adds the broker name. Nothing else is touched: the name is the label the
    # template gave the instance, never recomputed, and extension fields ride
    # through.
    recorded = {**PARAMS, "nodes": [
        {**PARAMS["nodes"][0], "extra": "kept"},
        {**PARAMS["nodes"][1], "name": "renamed-in-console"},
        PARAMS["nodes"][2]]}
    nodes = cluster.nodes(opts, recorded)
    assert [n["ip"] for n in nodes] == ["203.0.113.10", "203.0.113.11", "203.0.113.12"]
    assert [n["vpc-ip"] for n in nodes] == ["10.40.0.3", "10.40.0.4", "10.40.0.5"]
    assert not any("vpc_ip" in n for n in nodes)
    assert nodes[1]["name"] == "renamed-in-console"
    assert nodes[0]["extra"] == "kept"
    assert nodes[1]["broker-name"] == "b1.automq.example.com"


def test_principals_are_distinct_and_the_client_is_not_a_superuser():
    assert cluster.scram_principals(opts) == ["automq-admin", "automq-broker", "automq"]
    # The controller principal is absent from the SCRAM set on purpose.
    assert "automq-controller" not in cluster.scram_principals(opts)
    supers = cluster.super_users(opts)
    assert "User:automq-admin" in supers
    assert "User:automq-controller" in supers
    assert not re.search(r"User:automq;|User:automq$", supers)


def test_client_acls_grant_no_administration():
    acls = cluster.client_acls(opts)
    operations = {op for acl in acls for op in acl["operations"]}
    assert {acl["resource-type"] for acl in acls} == {"topic", "group"}
    assert all(acl["pattern-type"] == "prefixed" for acl in acls)
    assert operations == {"Describe", "Read", "Write"}
    # Nothing that could administer the cluster or bypass the prefix.
    for denied in ("Create", "Alter", "ClusterAction"):
        assert denied not in operations
