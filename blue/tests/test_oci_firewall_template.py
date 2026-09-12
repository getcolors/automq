"""Execute the OCI firewall JSON render with actual Ansible inventory values."""
import html
import json
from pathlib import Path
import re
import shutil
import subprocess

from blue.renderer import render
from blue.scaffold import PRESERVE_JINJA_DELIMITERS
import pytest
import yaml
from test_convergence import TREES


@pytest.mark.parametrize('tree', TREES)
def test_rendered_oci_firewall_intent_uses_peers_and_configured_sources(tree, tmp_path):
    ansible = shutil.which('ansible-playbook')
    if not ansible:
        pytest.skip('requires Ansible')
    source = (tree / 'ansible/main.yml').read_text()
    values = {key: '<{ ' + key + ' }>' for key in re.findall(r'<\{ ([a-z0-9-]+) \}>', source)}
    values.update({'profile': 'automq-test', 'automq-compute-oci': True,
                   'automq-apt-security-mirror': None, 'firewall-kafka-sources': ['198.51.100.0/24'],
                   'kafka-port': 9092, 'controller-port': 9093, 'internal-port': 9094})
    play = yaml.safe_load(html.unescape(render(source, values, PRESERVE_JINJA_DELIMITERS)))[0]
    assert not any(task['name'] == 'Open the cluster ports in the host firewall' for task in play['tasks'])
    preserve = next(task for task in play['tasks'] if task['name'] == 'Preserve OCI platform firewall files during package installation')
    assert preserve['ansible.builtin.debconf']['value'] == 'false'
    assert set(preserve['loop']) == {'iptables-persistent/autosave_v4', 'iptables-persistent/autosave_v6'}
    packages = next(task['ansible.builtin.apt'] for task in play['tasks'] if task['name'] == 'Install base packages')
    assert 'iptables-persistent' in packages['name'] and 'ufw' not in packages['name']
    assert packages['policy_rc_d'] == 101
    restore = next(task['ansible.builtin.systemd_service'] for task in play['tasks'] if task['name'] == 'Enable native OCI firewall restoration on boot')
    assert restore['enabled'] is True and 'state' not in restore
    intent = next(task for task in play['tasks'] if task['name'] == 'Write the OCI firewall intent')
    intent['ansible.builtin.copy']['dest'] = str(tmp_path / '{{ inventory_hostname }}.json')
    hosts = {f'node{i}': {'ansible_connection': 'local', 'automq_vpc_ip': f'10.42.0.{i+1}'} for i in range(3)}
    inventory = tmp_path / 'inventory.json'
    inventory.write_text(json.dumps({'all': {'children': {'automq': {'hosts': hosts}}}}))
    path = tmp_path / 'firewall.yml'
    path.write_text(yaml.safe_dump([{'hosts': 'automq', 'gather_facts': False, 'tasks': [intent]}]))
    result = subprocess.run([ansible, '-i', str(inventory), str(path)], capture_output=True, text=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
    for name, facts in hosts.items():
        config = json.loads((tmp_path / (name + '.json')).read_text())
        assert config == {'profile': 'automq-test', 'private_host': facts['automq_vpc_ip'],
                          'peers': ['10.42.0.1', '10.42.0.2', '10.42.0.3'],
                          'kafka_sources': ['198.51.100.0/24'], 'kafka_port': 9092,
                          'controller_port': 9093, 'internal_port': 9094}
