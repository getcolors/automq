from conftest import fixture
from package_automq_blue import ssh_config

opts = fixture({"profile": "automq-vultr"})


def test_the_deployment_claims_one_alias_per_node_and_the_bare_profile():
    # `ssh automq-vultr` is what the standard promises; the numbered aliases are
    # what make a quorum operable, since half of running one is reaching a
    # specific member.
    assert ssh_config.aliases(opts) == [
        "automq-vultr", "automq-vultr-0", "automq-vultr-1", "automq-vultr-2"]


def test_the_identity_file_stays_unexpanded():
    assert ssh_config.identity_file(opts) == "~/.ssh/automq-vultr"


def test_a_foreign_stanza_is_found_for_any_alias_not_just_the_first():
    lines = ("Host something\n  HostName 1.2.3.4\n\n"
             "Host automq-vultr-2\n  HostName 5.6.7.8\n").splitlines()
    assert ssh_config.foreign_stanza_line(lines, "automq-vultr") is None
    assert ssh_config.foreign_stanza_line(lines, "automq-vultr-2") == 4


def test_our_own_managed_block_is_not_foreign_for_any_alias_in_it():
    # One block, marked with the profile, holding a stanza per node. Deriving
    # the marker from the stanza being searched — which a single-node package
    # can get away with — makes the check hunt for `# BEGIN automq-vultr-0 …`,
    # never find it, and refuse to converge because of a block this package
    # wrote itself.
    lines = ("# BEGIN automq-vultr ANSIBLE MANAGED BLOCK\n"
             "Host automq-vultr\n  HostName 1.2.3.4\n"
             "Host automq-vultr-0\n  HostName 1.2.3.4\n"
             "Host automq-vultr-1\n  HostName 1.2.3.5\n"
             "Host automq-vultr-2\n  HostName 1.2.3.6\n"
             "# END automq-vultr ANSIBLE MANAGED BLOCK\n").splitlines()
    for alias in ssh_config.aliases(opts):
        assert ssh_config.foreign_stanza_line(lines, alias, "automq-vultr") is None, alias


def test_a_node_stanza_outside_our_block_is_still_foreign():
    lines = ("# BEGIN automq-vultr ANSIBLE MANAGED BLOCK\n"
             "Host automq-vultr\n  HostName 1.2.3.4\n"
             "# END automq-vultr ANSIBLE MANAGED BLOCK\n"
             "Host automq-vultr-1\n  HostName 9.9.9.9\n").splitlines()
    assert ssh_config.foreign_stanza_line(lines, "automq-vultr-1", "automq-vultr") == 5


def test_a_global_option_above_the_first_host_blocks_the_run():
    # The block is inserted at BOF, so it would capture such an option into one
    # stanza and silently narrow a setting that applied to every host.
    assert ssh_config.leading_option_line(["ServerAliveInterval 60", "Host x"]) == 1
    assert ssh_config.leading_option_line(["# a comment", "", "Host x", "  User root"]) is None
    # An option below a Host line belongs to that host and is fine.
    assert ssh_config.leading_option_line(["Host x", "  ServerAliveInterval 60"]) is None


def test_the_refusal_is_reported_as_a_failed_step(monkeypatch, tmp_path):
    config = tmp_path / ".ssh" / "config"
    config.parent.mkdir(parents=True)
    config.write_text("Host automq-vultr-1\n  HostName 9.9.9.9\n")
    monkeypatch.setenv("HOME", str(tmp_path))
    refused = ssh_config.preflight(opts)
    assert refused["blue/exit"] == 1
    assert "automq-vultr-1" in refused["blue/err"]


def test_embedded_updater_removes_owned_block_without_host_inventory(tmp_path):
    import json
    import os
    from pathlib import Path
    import subprocess
    import sys
    import textwrap
    roots = Path(__file__).resolve().parents[2]
    plays = [roots / 'green/src/resources/io/github/getcolors/automq/tools/ansible-local/main.yml',
             roots / 'red/resources/tools/ansible-local/main.yml',
             roots / 'blue/src/package_automq_blue/resources/tools/ansible-local/main.yml']
    for index, play in enumerate(plays):
        source = textwrap.dedent(play.read_text().split('          - |\n', 1)[1].split('        stdin:', 1)[0])
        home = tmp_path / str(index)
        config = home / '.ssh/config'
        config.parent.mkdir(parents=True)
        retained = 'Host unrelated\n    User operator\n'
        config.write_text('# BEGIN partial ANSIBLE MANAGED BLOCK\nHost partial\n    HostName 192.0.2.1\n# END partial ANSIBLE MANAGED BLOCK\n' + retained)
        payload = {'host_alias': 'partial', 'keygen': True, 'block_state': 'absent', 'ssh_hosts': []}
        def run(value):
            return subprocess.run([sys.executable, '-c', source], input=json.dumps(value), text=True,
                                  capture_output=True, env={**os.environ, 'HOME': str(home)}, timeout=5)
        first = run(payload)
        assert first.returncode == 0, first.stderr
        assert first.stdout.strip() == 'changed' and config.read_text() == retained
        assert run(payload).stdout.strip() == 'unchanged'
        assert run({**payload, 'block_state': 'present'}).returncode != 0
        assert run({**payload, 'host_alias': 'bad\nalias'}).returncode != 0
        assert run({**payload, 'legacy_marker_prefix': 'bad\nmarker'}).returncode != 0
        malformed = '# BEGIN partial ANSIBLE MANAGED BLOCK\nHost partial\n'
        config.write_text(malformed)
        assert run(payload).returncode != 0
        assert config.read_text() == malformed
