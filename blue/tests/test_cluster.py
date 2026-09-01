import re

from conftest import PARAMS, fixture
from package_automq_blue import cluster

opts = fixture({"profile": "automq-vultr"})


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
    # Documentation-range addresses, so goldens mean the same everywhere.
    nodes = cluster.nodes(opts)
    assert len(nodes) == 3
    assert [n["ip"] for n in nodes] == ["192.0.2.10", "192.0.2.11", "192.0.2.12"]
    assert all(re.fullmatch(r"10\.40\.0\.\d+", n["vpc-ip"]) for n in nodes)


def test_a_partial_compute_output_is_an_error_not_a_smaller_cluster():
    # Rendering a two-voter quorum for a three-node cluster produces something
    # that starts and then cannot elect, which is far worse than refusing.
    assert cluster.missing_node_error(opts, PARAMS) is None
    assert "node 2" in cluster.missing_node_error(opts, PARAMS[:2])
    assert "quorum string" in cluster.missing_node_error(opts, [
        {"index": 0, "ip": "1.2.3.4", "vpc-ip": ""},
        {"index": 1, "ip": "1.2.3.5", "vpc-ip": "10.0.0.2"},
        {"index": 2, "ip": "1.2.3.6", "vpc-ip": "10.0.0.3"}])
    # No output at all is a build, not a broken cluster.
    assert cluster.missing_node_error(opts, None) is None


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
