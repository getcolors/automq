"""Exercise real iptables only inside a fresh network namespace."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / 'green/src/resources/io/github/getcolors/automq/tools/ansible/firewall.py'
spec = importlib.util.spec_from_file_location('firewall', HELPER)
firewall = importlib.util.module_from_spec(spec)
spec.loader.exec_module(firewall)
CONFIG = {'profile': 'automq-test', 'private_host': '10.42.0.11',
          'peers': ['10.42.0.11', '10.42.0.12', '10.42.0.13'],
          'kafka_sources': ['198.51.100.0/24'], 'kafka_port': 9092,
          'controller_port': 9093, 'internal_port': 9094}


def integration():
    # Even an accidental direct invocation must never touch the host firewall.
    assert os.readlink('/proc/self/ns/net') != os.readlink(f'/proc/{os.getppid()}/ns/net')
    def ip(*args):
        return firewall.command(list(args))
    ip('-A', 'INPUT', '-p', 'tcp', '--dport', '22', '-j', 'ACCEPT')
    ip('-A', 'INPUT', '-j', 'REJECT', '--reject-with', 'icmp-host-prohibited')
    ip('-N', 'InstanceServices')
    ip('-A', 'OUTPUT', '-d', '169.254.0.0/16', '-j', 'InstanceServices')
    ip('-A', 'InstanceServices', '-p', 'tcp', '--dport', '3260', '-m', 'owner', '--uid-owner', '0', '-j', 'ACCEPT')
    ip('-A', 'InstanceServices', '-d', '169.254.169.254/32', '-p', 'udp', '--dport', '123', '-j', 'ACCEPT')
    baseline = subprocess.check_output(['iptables-save', '-t', 'filter'], text=True)
    # OCI's retained image file omits the UDP module that iptables canonicalizes.
    baseline = baseline.replace('-p udp -m udp --dport', '-p udp --dport')
    ip('-N', 'DOCKER')
    ip('-A', 'FORWARD', '-j', 'DOCKER')
    original_input = ip('-S', 'INPUT').splitlines()
    preserved = {chain: ip('-S', chain) for chain in ('OUTPUT', 'InstanceServices', 'DOCKER', 'FORWARD')}
    first = firewall.apply(CONFIG)
    assert first['changed'] and first['rules'] == 7
    assert firewall.apply(CONFIG)['changed'] is False
    rules = ip('-S', 'INPUT').splitlines()
    assert first['chain'] in rules[1]  # policy precedes rules in -S INPUT
    assert [rules[0], *rules[2:]] == original_input
    assert preserved == {chain: ip('-S', chain) for chain in preserved}
    open_config = {**CONFIG, 'kafka_sources': ['0.0.0.0/0']}
    assert firewall.apply(open_config)['changed']
    assert not firewall.apply(open_config)['changed']  # iptables omits -s 0/0
    private_only = {**CONFIG, 'kafka_sources': []}
    assert firewall.apply(private_only)['rules'] == 6
    assert '--dport 9092' not in ip('-S', first['chain'])
    assert preserved == {chain: ip('-S', chain) for chain in preserved}
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / 'rules.v4'
        path.write_text(baseline)
        ip('-D', 'OUTPUT', '-d', '169.254.0.0/16', '-j', 'InstanceServices')
        ip('-F', 'InstanceServices')  # Simulate reboot loss only in this isolated namespace.
        ip('-X', 'InstanceServices')
        ip('-D', 'INPUT', '-j', 'REJECT', '--reject-with', 'icmp-host-prohibited')
        before_docker = ip('-S', 'DOCKER')
        assert firewall.restore_platform(path)
        stable = ip('-S')
        assert not firewall.restore_platform(path)
        assert not firewall.restore_platform(path)
        assert ip('-S') == stable
        assert ip('-S', 'DOCKER') == before_docker
        assert preserved == {chain: ip('-S', chain) for chain in preserved}
        assert '--dport 9093' in ip('-S', first['chain'])
    print(json.dumps({'passed': True, 'real_iptables': True, 'idempotent': True,
                      'platform_and_docker_preserved': True, 'source_tightening': True}))


class Firewall(unittest.TestCase):
    def test_exact_private_peers_and_public_sources(self):
        _, rules = firewall.desired(CONFIG)
        self.assertEqual(len(rules), 7)
        self.assertIn(('198.51.100.0/24', '10.42.0.11/32', 9092), rules)
        self.assertEqual({source for source, _, port in rules if port == 9093},
                         {'10.42.0.11/32', '10.42.0.12/32', '10.42.0.13/32'})

    def test_invalid_inputs_refuse_before_any_firewall_command(self):
        for change in ({'profile': 'bad\nprofile'}, {'private_host': '192.0.2.1'},
                       {'controller_port': 22, 'internal_port': 22}, {'kafka_sources': ['any']}):
            with patch.object(firewall, 'command') as command:
                with self.assertRaises(ValueError):
                    firewall.apply({**CONFIG, **change})
                command.assert_not_called()

    def test_foreign_chain_rule_refuses_without_mutation(self):
        import hashlib
        chain = 'AUTOMQ_' + hashlib.sha256(CONFIG['profile'].encode()).hexdigest()[:16].upper()
        with patch.object(firewall, 'command', return_value=f'-N {chain}\n-A {chain} -j ACCEPT\n') as command:
            with self.assertRaisesRegex(ValueError, 'foreign rule'):
                firewall.apply(CONFIG)
            command.assert_called_once_with(['-S'])

    def test_foreign_platform_baseline_refuses_before_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'rules.v4'
            path.write_text('*filter\n:DOCKER - [0:0]\nCOMMIT\n')
            with patch.object(firewall, 'command') as command:
                with self.assertRaisesRegex(ValueError, 'foreign chain'):
                    firewall.restore_platform(path)
                command.assert_not_called()

    def test_platform_check_error_does_not_append(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'rules.v4'
            path.write_text('*filter\n:INPUT ACCEPT [0:0]\n:FORWARD ACCEPT [0:0]\n'
                            ':OUTPUT ACCEPT [0:0]\n:InstanceServices - [0:0]\n'
                            '-A InstanceServices -p udp --dport 123 -j ACCEPT\nCOMMIT\n')
            with patch.object(firewall, 'command', return_value='-N InstanceServices\n') as command, \
                    patch.object(firewall.subprocess, 'run', return_value=SimpleNamespace(returncode=2)):
                with self.assertRaisesRegex(RuntimeError, 'existence check failed'):
                    firewall.restore_platform(path)
                command.assert_called_once_with(['-S'])

    def test_real_iptables_in_isolated_namespace(self):
        if not all(shutil.which(name) for name in ('sudo', 'unshare', 'iptables')):
            self.skipTest('requires sudo, unshare and iptables')
        probe = subprocess.run(['sudo', '-n', 'unshare', '--net', 'true'], capture_output=True, text=True)
        if probe.returncode:
            self.skipTest('network namespace privilege unavailable')
        result = subprocess.run(['sudo', '-n', 'unshare', '--net', sys.executable, str(Path(__file__).resolve()), '--integration'],
                                capture_output=True, text=True, timeout=60)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(json.loads(result.stdout)['platform_and_docker_preserved'])


if __name__ == '__main__':
    if sys.argv[1:] == ['--integration']:
        integration()
    else:
        unittest.main()
