"""Exercise the public acceptance script without a broker or cloud account."""
import os
from pathlib import Path
import re
import subprocess
import shlex
import socket
import time

import pytest

ROOT = Path(__file__).resolve().parents[2]
TREES = [ROOT / "green/src/resources/io/github/getcolors/automq/tools",
         ROOT / "red/resources/tools", ROOT / "blue/src/package_automq_blue/resources/tools"]
MOCK = r'''#!/usr/bin/env python3
import os, pathlib, sys, signal, time
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
root = pathlib.Path(os.environ['MOCK_ROOT'])
scenario = os.environ['SCENARIO']

def log(text):
    with (root / 'calls').open('a') as f: f.write(text + '\n')

if name == 'ssh':
    command = ' '.join(args)
    if 'automq-credential' in command:
        print('principal: client\npassword: fake-password')
    elif 'docker kill --signal KILL' in command:
        log('kill')
        if scenario == 'kill-failed': sys.exit(1)
        if scenario == 'kill-delay': time.sleep(1)
        if scenario == 'terminated': os.kill(int((root / 'script-pid').read_text()), signal.SIGTERM)
    elif 'docker inspect' in command:
        print('true' if scenario == 'auto-restarted' else 'false')
    elif 'docker start' in command:
        log('start')
        if scenario == 'start-failed' and (root / 'calls').read_text().splitlines().count('start') == 1: sys.exit(1)
    elif 'docker restart' in command:
        log('restart')
        if scenario == 'restart-failed': sys.exit(1)
    elif 'kafka-consumer-groups' in command:
        count_file = root / 'offset-count'
        count = int(count_file.read_text()) if count_file.exists() else 0
        count_file.write_text(str(count + 1))
        group = command.split('--group ')[1].split()[0]
        first, second = (49, 71) if count and scenario == 'lost-offset' else (50, 70)
        print(f'{group} colors-acceptance 0 {first} 120')
        print(f'{group} colors-acceptance 1 {second} 120')
    elif 'kafka-topics' in command and '--describe' in command:
        print('Topic: colors-failover Partition: 0 Leader: 1')
    elif 'kafka-metadata-quorum' in command:
        if '--replication' in command: print('0 directory 10 0 Leader\n1 directory 10 0 Follower')
        else: print('LeaderId: 0')
    elif 'kafka-broker-api-versions' in command:
        print('id: 0\nid: 1\nid: 2')
    elif 'kafka-producer-perf-test' in command:
        if scenario == 'perf-failed': sys.exit(1)
        print('20000 records sent, 5000 records/sec, 1.0 ms avg latency')
    sys.exit(0)
if name == 'kcat':
    if any('wrong-password' in arg for arg in args): sys.exit(1)
    if '-L' in args: print('  broker 0\n  broker 1\n  broker 2'); sys.exit(0)
    if '-G' in args: sys.exit(0)
    topic = args[args.index('-t') + 1]
    if topic.startswith('outside-prefix'): sys.exit(1)
    data = root / topic
    if '-P' in args:
        records = sys.stdin.read()
        with data.open('a') as f: f.write(records)
        sys.exit(0)
    if '-C' in args:
        records = data.read_text() if data.exists() else ''
        if scenario == 'stale-public' and topic == 'colors-acceptance':
            records = ''.join(f'public-old-{i}\n' for i in range(1, 201))
        if scenario == 'duplicate-before' and topic == 'colors-failover':
            lines = records.splitlines()
            if len(lines) >= 100: lines[-1] = lines[0]
            records = '\n'.join(lines) + '\n'
        print(records, end='')
        sys.exit(0)
if name == 'getent': print('203.0.113.1 host'); sys.exit(0)
if name == 'openssl': sys.stdin.read(); sys.exit(0)
'''


@pytest.mark.parametrize("tree", TREES)
@pytest.mark.parametrize("scenario", ["healthy", "kill-delay", "auto-restarted", "kill-failed", "restart-failed", "stale-public", "lost-offset", "perf-failed", "duplicate-before", "terminated", "start-failed"])
def test_acceptance_proves_actions_and_current_records(tree, scenario, tmp_path):
    binaries = tmp_path / "bin"
    binaries.mkdir()
    for tool in ["ssh", "kcat", "getent", "openssl"]:
        path = binaries / tool
        path.write_text(MOCK)
        path.chmod(0o755)
    temporary = tmp_path / "temporary"
    temporary.mkdir()
    values = {"bootstrap-external": "203.0.113.1:9092", "bootstrap-internal": "10.0.0.1:9094",
              "certificate-names-csv": "example.com", "topic-prefix": "colors-", "profile": "test",
              "node-count": "3", "kafka-port": "9092", "automq-tls-mode": "acme", "automq-topic-partitions": "6"}
    source = (tree / "acceptance/acceptance.sh").read_text()
    script = tmp_path / "acceptance.sh"
    source = source.replace('RUN="$(date +%s)-$$"', 'RUN="$(date +%s)-$$"\nprintf "%s" "$$" > "$MOCK_ROOT/script-pid"')
    if scenario == "duplicate-before":
        source = source.replace("DEADLINE=$((SECONDS + 60))", "DEADLINE=$((SECONDS + 1))")
    script.write_text(re.sub(r"<\{ ([^}]+) }>", lambda match: values[match[1]], source))
    env = {**os.environ, "PATH": f"{binaries}:{os.environ['PATH']}", "MOCK_ROOT": str(tmp_path),
           "SCENARIO": scenario, "TMPDIR": str(temporary)}
    result = subprocess.run(["bash", str(script)], env=env, capture_output=True, text=True, timeout=20)
    assert result.returncode == (0 if scenario in ("healthy", "kill-delay") else 143 if scenario == "terminated" else 1), result.stdout + result.stderr
    assert not list(temporary.iterdir()), "the single EXIT trap must always clean temporary files"
    report = (tmp_path / "result.log").read_text()
    assert "fake-password" not in report
    assert "acceptance:" in report
    calls = (tmp_path / "calls").read_text().splitlines() if (tmp_path / "calls").exists() else []
    if scenario in ("kill-failed", "auto-restarted"):
        assert calls == ["kill", "start"], "even an uncertain stop is restored; no fault-success claim follows"
        assert ("failover was not exercised" if scenario == "kill-failed" else "did not remain stopped") in result.stderr
    elif scenario == "terminated":
        assert calls == ["kill", "start"]
    elif scenario == "start-failed":
        assert calls == ["kill", "start", "start"]
    elif scenario == "duplicate-before":
        assert not calls, "an incomplete unique record set must never authorize fault injection"
    else:
        assert calls == ["kill", "start", "restart"]
    if scenario == "kill-delay":
        measured = re.search(r"became writable again ([0-9]+)s", report)
        assert measured and int(measured.group(1)) >= 1, "fault command time belongs in the recovery measurement"
    if scenario == "lost-offset":
        assert "lost or could not verify a committed partition offset" in result.stderr
    if scenario == "perf-failed":
        assert "producer performance workload failed" in result.stderr


@pytest.mark.parametrize("tree", TREES)
def test_phase_deadline_bounds_time_inside_a_client(tree):
    import time
    source = (tree / "acceptance/acceptance.sh").read_text()
    helper = source[source.index("DEADLINE=0\n"):source.index('sent=""; got="";')]
    started = time.monotonic()
    result = subprocess.run(["bash", "-c", helper + '\nDEADLINE=$((SECONDS + 1))\nbounded 10 sleep 20\n'],
                            capture_output=True, text=True, timeout=5)
    assert result.returncode == 124
    assert time.monotonic() - started < 4


@pytest.mark.parametrize('tree', TREES)
@pytest.mark.parametrize('certificate_ip,expected', [('127.0.0.1', 0), ('127.0.0.2', 1)])
def test_private_ca_tls_gate_needs_no_ptr_and_verifies_actual_ip_san(tree, certificate_ip, expected, tmp_path):
    cert, key = tmp_path / 'cert.pem', tmp_path / 'key.pem'
    subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes', '-days', '1',
                    '-subj', '/CN=acceptance-test', '-addext', 'subjectAltName=IP:' + certificate_ip,
                    '-keyout', str(key), '-out', str(cert)], check=True, capture_output=True)
    with socket.socket() as selected:
        selected.bind(('127.0.0.1', 0))
        port = selected.getsockname()[1]
    server = subprocess.Popen(['openssl', 's_server', '-accept', f'127.0.0.1:{port}',
                               '-cert', str(cert), '-key', str(key), '-quiet'],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 5
        while True:
            try:
                with socket.create_connection(('127.0.0.1', port), timeout=.2):
                    break
            except OSError:
                assert time.monotonic() < deadline
                time.sleep(.05)
        binaries = tmp_path / 'bin'
        binaries.mkdir()
        getent = binaries / 'getent'
        getent.write_text('#!/bin/sh\necho called > ' + shlex.quote(str(tmp_path / 'dns-called')) + '\nexit 1\n')
        getent.chmod(0o755)
        source = (tree / 'acceptance/acceptance.sh').read_text()
        phase = source[source.index('for name in "${CERT_NAMES[@]}"; do'):source.index('# --- 8:')]
        phase = phase.replace('<{ automq-tls-mode }>', 'private-ca').replace('<{ kafka-port }>', str(port))
        setup = '''set -uo pipefail
pass=0; fail=0
ok() { pass=$((pass+1)); }
bad() { fail=$((fail+1)); }
bounded() { local seconds="$1"; shift; timeout "$seconds" "$@"; }
CERT_NAMES=(127.0.0.1)
'''
        setup += 'VERIFY_ARGS=(-CAfile ' + shlex.quote(str(cert)) + ')\n'
        result = subprocess.run(['bash', '-c', setup + phase + '\n[ "$fail" -eq 0 ]'],
                                env={**os.environ, 'PATH': str(binaries) + ':' + os.environ['PATH']},
                                capture_output=True, text=True, timeout=10)
        assert result.returncode == expected, result.stdout + result.stderr
        assert not (tmp_path / 'dns-called').exists()
    finally:
        server.terminate()
        server.wait(timeout=5)
