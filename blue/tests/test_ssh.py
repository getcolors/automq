import os

from conftest import fixture, optout
from package_automq_blue import ssh


def test_a_build_never_names_the_operators_home():
    # Committed goldens must mean the same thing on every workstation, so a
    # build renders a fixed placeholder rather than reading ~/.ssh.
    opts = ssh.with_machine_key(fixture({"blue/event": "build"}))
    assert opts["ssh-private-key-path"] == "/home/build-placeholder/.ssh/automq-fixture"
    assert opts["ssh-public-key-path"] == "/home/build-placeholder/.ssh/automq-fixture.pub"
    assert "build-placeholder" not in str(os.environ.get("HOME"))


def test_a_dry_run_is_held_to_the_same_rule_as_a_build():
    # A dry-run is a create that touches nothing; testing the event alone would
    # let it reach the real key path.
    assert ssh.rendered_only({"blue/event": "build"})
    assert ssh.rendered_only({"blue/event": "create", "blue/dry-run": True})
    assert not ssh.rendered_only({"blue/event": "create"})


def test_opt_out_opts_pass_through_untouched():
    opts = optout({"blue/event": "build"})
    assert ssh.with_machine_key(opts) == opts
    # Nothing about the operator's key material is invented.
    assert ssh.with_machine_key(opts).get("ssh-private-key-path") is None
