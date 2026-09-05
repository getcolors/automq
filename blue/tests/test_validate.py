from conftest import fixture, optout
from package_automq_blue import validate


def errors(opts) -> list[str]:
    return validate.state_errors(opts)


def matching(opts, needle: str) -> bool:
    return any(needle in error for error in errors(opts))


def test_both_fixtures_are_valid():
    assert errors(fixture()) == []
    assert errors(optout()) == []


def test_the_machine_key_is_not_required():
    # The standard makes absence meaningful: requiring vultr-ssh-keys would make
    # every conforming keygen deployment invalid. `vultr-name` is absent for the
    # same shape of reason (Compute Name Standard §2, §5).
    assert "vultr-ssh-keys" not in validate.required
    assert "vultr-name" not in validate.required


def test_absent_machine_key_selects_keygen():
    assert validate.keygen(fixture()) is True
    assert validate.keygen(optout()) is False


def test_every_missing_key_is_reported_at_once():
    # Exit code 2 means "here is everything that is wrong", not "here is the
    # first thing": an operator should need one run to fix a file, not six.
    incomplete = fixture()
    for key in ("automq-host", "vultr-region", "automq-cluster-id"):
        del incomplete[key]
    reported = errors(incomplete)
    assert len(reported) == 3
    assert all(error.endswith(" is required") for error in reported)


def test_the_image_must_be_pinned_by_digest():
    # A tag alone lets a silent retag change behaviour at run time.
    assert matching(fixture({"automq-image": "automqinc/automq:1.7.4"}), "pinned by digest")
    assert not matching(fixture(), "pinned by digest")


def test_an_even_quorum_is_refused():
    # Four voters tolerate exactly one failure, the same as three, while adding
    # a node that can fail. That is strictly worse, so it is not offered.
    assert matching(fixture({"automq-node-count": 4}), "must be odd")
    assert not matching(fixture({"automq-node-count": 5}), "must be odd")
    # One node is a legitimate development shape.
    assert not matching(fixture({"automq-node-count": 1}), "must be odd")
    assert matching(fixture({"automq-node-count": 0}), "from 1 to 9")
    assert matching(fixture({"automq-node-count": "three"}), "must be an integer")


def test_the_cluster_id_must_be_a_real_kafka_uuid():
    for bad in ("not-a-uuid", "VrUQI4OSR0y5vnTrGiKsx"):
        assert matching(fixture({"automq-cluster-id": bad}), "base64 UUID")


def test_storage_must_not_be_shared():
    # The two roles write different key layouts and cannot share a bucket.
    assert matching(fixture({"automq-ops-r2-bucket": "automq-fixture-data"}),
                    "must be different buckets")
    # And neither may be the state bucket, since AutoMQ writes at the root.
    assert matching(fixture({"automq-data-r2-bucket": "fixture-state"}),
                    "must not be the OpenTofu state bucket")


def test_listener_ports_must_differ():
    assert matching(fixture({"automq-internal-port": 9092}), "must differ")
    assert matching(fixture({"automq-kafka-port": 70000}), "from 1 to 65535")


def test_principals_must_be_distinct():
    # Four principals share one namespace in the metadata log, and three of them
    # are superusers: a collision is a privilege escalation, not a typo.
    assert matching(fixture({"automq-admin-user": "automq"}), "must all differ")
    assert not matching(fixture(), "must all differ")


def test_the_destroy_guard_accepts_the_one_run_override():
    # The override arrives through the same COLORS_PAR overlay as every other
    # parameter, so rejecting `false` here would make the documented way to
    # destroy this deployment impossible. The delete-time validator is what
    # refuses a destroy while the guard is still true.
    assert not matching(fixture({"compute-prevent-destroy": False}), "prevent-destroy")
    assert matching(fixture({"compute-prevent-destroy": "yes"}), "must be true or false")


def test_the_compute_checks_are_the_cluster_standards():
    # Selection, the source lists, the created network's CIDR and the node
    # count are ONCE's over the spec, in ONCE's words. The package's own
    # cluster-shape rules still apply beside them.
    assert errors(fixture({"provider-compute": "digitalocean"})) == [
        ":provider-compute must be one of vultr"]
    assert errors(fixture({"vultr-ssh-sources": []})) == [
        ":vultr-ssh-sources must list at least one CIDR"]
    assert errors(fixture({"vultr-ssh-sources": ["1.2.3.4"]})) == [
        ':vultr-ssh-sources entry "1.2.3.4" is not an IPv4 or IPv6 CIDR']
    # An empty Kafka list means no public Kafka access, not a mistake.
    assert errors(fixture({"vultr-kafka-sources": []})) == []
    # The VPC must be a network, host bits zero.
    assert errors(fixture({"vultr-vpc-subnet": "10.40.0.1/24"})) == [
        ":vultr-vpc-subnet must be a canonical IPv4 network such as 10.40.0.0/24"]
    # A present count that is not a positive integer is refused twice: ONCE's
    # rule and the quorum's.
    reported = errors(fixture({"automq-node-count": "three"}))
    assert ":automq-node-count must be a positive integer" in reported
    assert ":automq-node-count must be an integer" in reported


def test_the_profile_overlay_is_refused():
    assert validate.env_errors({"COLORS_PAR_PROFILE": "somewhere-else"})
    assert validate.env_errors({}) == []


def test_secrets_are_asked_for_only_when_they_are_needed():
    none = fixture()
    # A create needs the storage keys as well as the provider keys.
    assert any("AUTOMQ_R2_ACCESS_KEY_ID" in e for e in validate.secret_errors(none, "create"))
    # A delete converges nothing, so demanding storage keys would only lock the
    # exit.
    assert not any("AUTOMQ_R2" in e for e in validate.secret_errors(none, "delete"))
    assert any("VULTR_API_KEY" in e for e in validate.secret_errors(none, "delete"))


class Result:
    def __init__(self, exit, out="", err=""):
        self.exit, self.out, self.err = exit, out, err


def test_the_api_probe_distinguishes_outage_from_credential():
    # The whole point: a single "check your token" message for every non-2xx
    # sends an operator to rotate a key during a provider outage.
    assert validate.api_error(Result(0, "200")) is None
    assert "rejected" in validate.api_error(Result(0, "401"))
    assert "rejected" in validate.api_error(Result(0, "403"))
    assert "rate-limited" in validate.api_error(Result(0, "429"))
    outage = validate.api_error(Result(0, "503"))
    assert "failure on Vultr's side" in outage
    assert "do not rotate" in outage
    assert "not a credential problem" in validate.api_error(Result(6, "000"))


async def test_tools_are_checked_without_touching_the_network():
    async def runner(args, **_kwargs):
        return Result(1) if args[-1] == "curl" else Result(0)

    assert any("curl" in e for e in await validate.runtime_errors(fixture(), runner))


async def test_a_reachable_api_with_a_rejected_key_stops_the_run():
    async def runner(args, **_kwargs):
        return Result(0, "401") if args[0] == "curl" else Result(0)

    found = await validate.runtime_errors(fixture({"vultr-api-key": "nope"}), runner)
    assert len(found) == 1
    assert "COLORS_PAR_VULTR_API_KEY" in found[0]
