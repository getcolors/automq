from conftest import fixture
from package_automq_blue import workflow


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
