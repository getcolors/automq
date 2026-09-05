import pytest
from blue.workflow import StepError
from conftest import PARAMS, fixture
from package_automq_blue import tools, validate, workflow


def chain(event: str) -> list[str]:
    step, seen = "automq/start", []
    while True:
        wired = workflow.wire_fn(step, {"blue/event": event})
        nxt = wired[1] if wired and len(wired) > 1 else None
        if not nxt:
            return seen
        seen.append(nxt)
        step = nxt


def test_create_resolves_addresses_before_it_needs_them():
    # DNS needs the compute output; the brokers advertise names that must
    # already resolve, and the certificate is issued for those names during the
    # play. The order is the dependency, not a preference.
    assert chain("create") == ["automq/infrastructure", "automq/ssh-config",
                               "automq/dns", "automq/ansible", "automq/acceptance"]


def test_delete_unwinds_in_the_order_that_keeps_access():
    # The ssh_config block goes before the destroy and the keypair after it: a
    # stale block is harmless, a key that predeceases its host locks the operator
    # out of machines that still exist. DNS goes before the destroy so no record
    # survives pointing at an address Vultr can hand to someone else.
    assert chain("delete") == ["automq/ansible", "automq/ssh-config", "automq/dns",
                               "automq/infrastructure", "automq/ssh-cleanup"]


def test_validate_answers_the_question_without_rendering_anything():
    # It must work on a fresh checkout with no keypair and no state. Falling
    # through to the create chain would plan a compute stage that reads the
    # machine public key, so the check would fail on exactly the case it exists
    # to serve.
    assert chain("validate") == []
    assert workflow.wire_fn("automq/infrastructure", {"blue/event": "validate"}) is None


async def test_the_destroy_guard_is_desired_state_not_a_flag():
    result = await workflow.start_step(
        {**fixture(), "blue/event": "delete", "compute-prevent-destroy": True}, {})
    assert result["blue/exit"] == 2
    assert "compute destruction is protected" in result["blue/err"]


def test_defaults_cover_every_key_an_operator_should_not_have_to_write():
    assert workflow.DEFAULTS["automq-node-count"] == 3
    assert workflow.DEFAULTS["automq-kafka-port"] == 9092
    assert workflow.DEFAULTS["provider-compute"] == "vultr"
    # But the guard defaults to protecting the deployment.
    assert workflow.DEFAULTS["compute-prevent-destroy"] is True


def test_every_side_effecting_step_is_skipped_by_a_dry_run():
    for step in ["automq/infrastructure", "automq/dns", "automq/ssh-config",
                 "automq/ansible", "automq/acceptance", "automq/ssh-cleanup"]:
        assert step in workflow.side_effecting


# --- the lifecycle against the compute state ----------------------------------

# The compute state is read once per run, through `tools.state_output`, on a
# real create or delete. Every lifecycle test stubs it: None is a readable state
# holding no compute, a dict is a recorded `params`, and a raise is a backend
# that cannot be read. The Vultr API probe is stubbed too — these tests are
# about the state, and they must not reach the network.

CREDENTIALS = {"vultr-api-key": "v", "cloudflare-api-token": "c",
               "r2-access-key-id": "a", "r2-secret-access-key": "s",
               "automq-r2-access-key-id": "k", "automq-r2-secret-access-key": "z"}


@pytest.fixture
def quiet(monkeypatch):
    async def none(_opts):
        return []
    monkeypatch.setattr(validate, "runtime_errors", none)


@pytest.fixture
def state(monkeypatch, quiet):
    def install(params):
        async def stub(_opts):
            return params
        monkeypatch.setattr(tools, "state_output", stub)
    return install


@pytest.fixture
def unreadable(monkeypatch, quiet):
    # The shape `blue.tofu` raises: the SDK's StepError. Only that is an
    # unreadable backend; anything else propagates as a defect.
    async def boom(_opts):
        raise StepError("tofu output failed: no backend")
    monkeypatch.setattr(tools, "state_output", boom)


def deleting(**overrides) -> dict:
    return {**fixture(), **CREDENTIALS, "blue/event": "delete",
            "compute-prevent-destroy": False, **overrides}


async def test_build_and_dry_run_never_touch_the_state(unreadable):
    # A raising state read proves nothing on these paths reaches the backend,
    # and the machine key stays the placeholder rather than the operator's home.
    for opts in [{**fixture(), "blue/event": "build"},
                 {**fixture(), "blue/event": "create", "blue/dry-run": True},
                 {**fixture(), "blue/event": "delete", "blue/dry-run": True,
                  "compute-prevent-destroy": False}]:
        result = await workflow.start_step(opts, env={})
        assert result["blue/exit"] == 0, result.get("blue/err")
        assert str(result["ssh-public-key-path"]).startswith("/home/build-placeholder")
        # A build renders the fallbacks; it adopts nothing.
        assert "once/cluster" not in result


async def test_a_real_create_requires_the_credentials(state):
    state(None)
    result = await workflow.start_step({**fixture(), "blue/event": "create"}, env={})
    assert result["blue/exit"] == 2
    assert "COLORS_PAR_VULTR_API_KEY" in result["blue/err"]
    assert "COLORS_PAR_CLOUDFLARE_API_TOKEN" in result["blue/err"]
    assert "COLORS_PAR_AUTOMQ_R2_ACCESS_KEY_ID" in result["blue/err"]


async def test_a_provider_switch_is_refused_before_the_credentials(state):
    # Provider switching is a rebuild, never an apply. The validator order is
    # the thing under test: the actionable error, not a missing token for the
    # provider that was just selected.
    state({**PARAMS, "provider": "digitalocean"})
    for event in ["create", "delete"]:
        result = await workflow.start_step(
            {**fixture(), "blue/event": event, "compute-prevent-destroy": False}, env={})
        assert result["blue/exit"] == 2, event
        assert ("state holds a digitalocean machine; set provider-compute back to "
                "digitalocean and delete first") in result["blue/err"]
        assert "required credential is not set" not in result["blue/err"]


async def test_legacy_state_is_accepted_on_the_default_provider(state, tmp_path, monkeypatch):
    # A `params` recorded before this package wrote `provider` — every
    # pre-adoption AutoMQ state — is a Vultr cluster and needs no translation.
    monkeypatch.setenv("HOME", str(tmp_path))
    legacy = {k: v for k, v in PARAMS.items() if k != "provider"}
    state(legacy)
    create = await workflow.start_step({**fixture(), "blue/event": "create"}, env={})
    assert "state holds" not in create["blue/err"]
    assert "required credential is not set" in create["blue/err"]
    delete = await workflow.start_step(deleting(), env={})
    assert delete["blue/exit"] == 0, delete.get("blue/err")
    assert delete["once/cluster"] == legacy


async def test_an_unreadable_backend_counts_as_no_state_on_create(unreadable):
    # A fresh clone has no readable state and must still be able to create.
    result = await workflow.start_step({**fixture(), "blue/event": "create"}, env={})
    assert result["blue/exit"] == 2
    assert "could not read" not in result["blue/err"]
    assert "state holds" not in result["blue/err"]
    assert "COLORS_PAR_VULTR_API_KEY" in result["blue/err"]


async def test_a_real_create_on_a_fresh_work_directory_reports_the_credentials_not_a_crash(
        quiet, tmp_path):
    # No state stub: the real `state_output` runs against a work directory that
    # holds no stage yet, as a fresh clone's does. The SDK's output read raises
    # its StepError there, which ONCE's `read_state` counts as an unreadable
    # state, so the create reports its credentials.
    result = await workflow.start_step(
        {**fixture(), "workdir": str(tmp_path), "blue/event": "create"}, env={})
    assert result["blue/exit"] == 2
    assert "COLORS_PAR_VULTR_API_KEY" in result["blue/err"]
    assert "could not read" not in result["blue/err"]


async def test_an_unreadable_backend_fails_a_real_delete_closed(unreadable):
    # Before adoption a delete proceeded on None here and would have rendered
    # the cleanup play against the documentation addresses.
    result = await workflow.start_step(deleting(), env={})
    assert result["blue/exit"] == 1
    assert "could not read the infrastructure state for the delete cleanup" in result["blue/err"]
    assert "no backend" in result["blue/err"]


async def test_a_real_delete_adopts_the_recorded_cluster(state, tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    state(PARAMS)
    adopted = await workflow.start_step(deleting(), env={})
    assert adopted["blue/exit"] == 0, adopted.get("blue/err")
    # The whole recorded params, extension keys and all.
    assert adopted["once/cluster"] == PARAMS
    assert [n["ip"] for n in tools.nodes(adopted)] == [
        "203.0.113.10", "203.0.113.11", "203.0.113.12"]
    # A readable state without compute adopts nothing, and the cleanup play
    # skips itself.
    state(None)
    empty = await workflow.start_step(deleting(), env={})
    assert empty["blue/exit"] == 0, empty.get("blue/err")
    assert "once/cluster" not in empty


async def test_a_real_delete_refuses_a_state_that_does_not_describe_every_node(state):
    # Three nodes are declared; a state that reports two is not a smaller
    # cluster to tear down but a state that cannot be trusted. ONCE's message,
    # unreworded.
    state({**PARAMS, "nodes": PARAMS["nodes"][:2]})
    partial = await workflow.start_step(deleting(), env={})
    assert partial["blue/exit"] == 1
    assert partial["blue/err"] == "the compute stage did not report nodes this package declares: 2"
    # A node without an address is refused the same way.
    state({**PARAMS, "nodes": [PARAMS["nodes"][0], {**PARAMS["nodes"][1], "vpc_ip": ""},
                               PARAMS["nodes"][2]]})
    incomplete = await workflow.start_step(deleting(), env={})
    assert incomplete["blue/exit"] == 1
    assert ("did not report a complete node (ip, vpc_ip, name, user, sudoer) for 1"
            in incomplete["blue/err"])
