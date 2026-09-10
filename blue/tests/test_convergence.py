"""Execute the host shell tasks with failed firewall and delayed broker probes."""
import os
from pathlib import Path
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
TREES = [ROOT / "green/src/resources/io/github/getcolors/automq/tools",
         ROOT / "red/resources/tools",
         ROOT / "blue/src/package_automq_blue/resources/tools"]


def run_shell(tmp_path, script, commands):
    for name, body in commands.items():
        executable = tmp_path / name
        executable.write_text("#!/bin/bash\n" + body)
        executable.chmod(0o755)
    return subprocess.run(["/bin/bash", "-c", script], text=True, capture_output=True,
                          env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"},
                          timeout=10)


@pytest.mark.parametrize("tree", TREES)
def test_host_firewall_failure_stops_convergence(tree, tmp_path):
    play = yaml.safe_load((tree / "ansible/main.yml").read_text())[0]
    assert play["become"] is True
    assert yaml.safe_load((tree / "ansible/cleanup.yml").read_text())[0]["become"] is True
    task = next(t for t in play["tasks"] if t["name"] == "Open the cluster ports in the host firewall")
    script = task["ansible.builtin.shell"]
    script = "\n".join(line for line in script.splitlines() if "{%" not in line)
    script = script.replace("{{ hostvars[host].automq_vpc_ip }}", "10.42.0.5")
    for key, value in [("kafka-port", "9092"), ("controller-port", "9093"), ("internal-port", "9094")]:
        script = script.replace("<{ " + key + " }>", value)
    result = run_shell(tmp_path, script, {"ufw": 'echo "ERROR: rejected rule"; exit 7\n'})
    assert result.returncode == 7
    assert "CHANGED" not in result.stdout


@pytest.mark.parametrize("tree", TREES)
@pytest.mark.parametrize("recovers", [True, False])
def test_restart_holds_throttle_until_readiness_or_failure(tree, tmp_path, recovers):
    play = yaml.safe_load((tree / "ansible/main.yml").read_text())[0]
    task = next(t for t in play["tasks"] if t["name"].startswith("Converge existing brokers"))
    assert task["throttle"] == 1
    script = task["ansible.builtin.shell"].replace("{{ automq_vpc_ip }}", "10.42.0.5").replace("<{ internal-port }>", "9094").replace("{{ render_config.changed | lower }}", "true")
    calls = tmp_path / "calls"
    mock = f'''echo "$1" >> '{calls}'
if [ "$1" = restart ] || [ "$1" = compose ]; then exit 0; fi
'''
    mock += f"[ $(wc -l < '{calls}') -ge 5 ]\n" if recovers else "exit 1\n"
    result = run_shell(tmp_path, script, {"docker": mock, "sleep": "exit 0\n"})
    assert result.returncode == (0 if recovers else 1)
    assert calls.read_text().splitlines() == ["compose", "restart"] + ["exec"] * (3 if recovers else 40)


@pytest.mark.parametrize("tree", TREES)
def test_config_first_render_and_repeat(tree, tmp_path):
    deployment = tmp_path / "etc"
    (deployment / "secrets").mkdir(parents=True)
    (deployment / "secrets/secrets.env").write_text(
        "AUTOMQ_CONTROLLER_PASSWORD=controller\nAUTOMQ_BROKER_PASSWORD=broker\n"
        "AUTOMQ_KEYSTORE_PASSWORD=keystore\nAUTOMQ_ADMIN_PASSWORD=admin\n"
        "AUTOMQ_CLIENT_PASSWORD=client\n")
    (deployment / "server.properties.in").write_text("password=@BROKER_PASSWORD@\n")
    script = (tree / "ansible/render-config.sh").read_text().replace("/etc/automq", str(deployment))
    result = run_shell(tmp_path, script, {})
    assert result.returncode == 0, result.stderr
    assert "config: changed" in result.stdout
    assert (deployment / "server.properties").read_text() == "password=broker\n"
    repeated = run_shell(tmp_path, script, {})
    assert repeated.returncode == 0, repeated.stderr
    assert "config: changed" not in repeated.stdout


@pytest.mark.parametrize("tree", TREES)
def test_kafka_cli_heap_does_not_inherit_the_broker_heap(tree, tmp_path):
    compose = yaml.safe_load((tree / "ansible/compose.yml").read_text())["services"]["automq"]
    assert compose["environment"]["KAFKA_HEAP_OPTS"] == "<{ automq-heap-opts }>"
    # Execute the actual healthcheck prefix with a daemon-sized inherited heap.
    probe = tmp_path / "probe"
    probe.write_text('#!/bin/bash\nprintf "%s" "$KAFKA_HEAP_OPTS"\n')
    probe.chmod(0o755)
    check = compose["healthcheck"]["test"][1]
    check = check.replace("/opt/automq/kafka/bin/kafka-broker-api-versions.sh", str(probe))
    check = check.replace("{{ automq_vpc_ip }}:<{ internal-port }>", "10.42.0.5:9094")
    check = check.replace(" >/dev/null 2>&1", "")
    result = subprocess.run(["/bin/bash", "-c", check], capture_output=True, text=True,
                            env={**os.environ, "KAFKA_HEAP_OPTS": "-Xms2g -Xmx2g"}, timeout=5)
    assert result.returncode == 0
    assert result.stdout == "-Xmx256m"
    # All operator, acceptance, and Ansible CLI paths must override Docker's
    # inherited daemon environment, including producer stdin and probes.
    executions = []
    for path in tree.rglob("*"):
        if path.suffix in {".sh", ".yml"}:
            executions.extend(line for line in path.read_text().splitlines() if "docker exec " in line)
    assert executions
    assert all("docker exec -e KAFKA_HEAP_OPTS=-Xmx256m " in line for line in executions)
    assert "docker run --rm -e KAFKA_HEAP_OPTS=-Xmx256m" in (tree / "ansible/format.sh").read_text()


@pytest.mark.parametrize("tree", TREES)
def test_compose_recreation_waits_without_a_duplicate_restart(tree, tmp_path):
    play = yaml.safe_load((tree / "ansible/main.yml").read_text())[0]
    task = next(t for t in play["tasks"] if t["name"].startswith("Converge existing brokers"))
    script = task["ansible.builtin.shell"].replace("{{ automq_vpc_ip }}", "10.42.0.5").replace("<{ internal-port }>", "9094").replace("{{ render_config.changed | lower }}", "true")
    calls = tmp_path / "calls"
    result = run_shell(tmp_path, script, {"docker": f'''echo "$1" >> '{calls}'
if [ "$1" = compose ]; then echo "Container automq Started"; fi
exit 0
'''})
    assert result.returncode == 0
    assert calls.read_text().splitlines() == ["compose", "exec"]
    assert "CHANGED" in result.stdout


def test_stopped_formatted_cluster_starts_in_parallel(tmp_path):
    import shutil
    ansible = shutil.which("ansible-playbook")
    if not ansible:
        pytest.skip("Ansible is needed to execute the playbook's recovery condition")
    source = yaml.safe_load((TREES[0] / "ansible/main.yml").read_text())[0]
    choose = next(t for t in source["tasks"] if t["name"].startswith("Choose parallel recovery"))
    expression = choose["ansible.builtin.set_fact"]["automq_parallel_start"]
    expression = expression.replace("ansible_play_hosts", "test_hosts").replace("hostvars", "test_hostvars")
    tasks = []
    for running in range(4):
        tasks.append({"ansible.builtin.set_fact": {"automq_parallel_start": expression},
                      "vars": {"test_hosts": ["a", "b", "c"], "test_hostvars": {
                          name: {"automq_running": {"stdout": "container-id" if index < running else ""}}
                          for index, name in enumerate(["a", "b", "c"])}}})
        tasks.append({"ansible.builtin.assert": {"that": ["automq_parallel_start | bool" if running < 2 else "not (automq_parallel_start | bool)"]}})
    play = tmp_path / "recovery.yml"
    play.write_text(yaml.safe_dump([{"hosts": "localhost", "gather_facts": False, "tasks": tasks}]))
    result = subprocess.run([ansible, "-i", "localhost,", "-c", "local", str(play)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("tree", TREES)
def test_parallel_recovery_applies_changed_config_before_common_readiness(tree, tmp_path):
    play = yaml.safe_load((tree / "ansible/main.yml").read_text())[0]
    task = next(t for t in play["tasks"] if t["name"].startswith("Start the brokers when"))
    script = task["ansible.builtin.shell"].replace("{{ render_config.changed | lower }}", "true")
    calls = tmp_path / "calls"
    result = run_shell(tmp_path, script, {"docker": f'echo "$1" >> "{calls}"\nexit 0\n'})
    assert result.returncode == 0
    assert calls.read_text().splitlines() == ["compose", "restart"]
    assert "CHANGED" in result.stdout
