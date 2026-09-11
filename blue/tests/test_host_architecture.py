"""Run the host architecture gate before selecting Docker and lego artifacts."""
import shutil
import subprocess

import pytest
import yaml
from test_convergence import TREES, load_play


@pytest.mark.parametrize('architecture,expected', [('x86_64', 'amd64'), ('aarch64', 'arm64'), ('armv7l', None)])
def test_host_architecture_gate_and_artifact_mapping(tmp_path, architecture, expected):
    ansible = shutil.which('ansible-playbook')
    if not ansible:
        pytest.skip('Ansible is needed to execute the architecture gate')
    source = load_play(TREES[0])
    tasks = [task for task in source['tasks'] if task['name'] in ['Require a supported host architecture', 'Select the host binary architecture']]
    assert len(tasks) == 2
    if expected:
        tasks.append({'ansible.builtin.assert': {'that': f'automq_host_arch == "{expected}"'}})
    play = tmp_path / 'architecture.yml'
    play.write_text(yaml.safe_dump([{'hosts': 'localhost', 'gather_facts': False, 'vars': {'ansible_architecture': architecture}, 'tasks': tasks}]))
    result = subprocess.run([ansible, '-i', 'localhost,', '-c', 'local', str(play)], capture_output=True, text=True, timeout=30)
    assert (result.returncode == 0) == (expected is not None), result.stdout + result.stderr
    docker = next(t for t in source['tasks'] if t['name'] == 'Add the Docker repository')
    lego = next(t for t in source['tasks'] if t['name'] == 'Install lego')
    assert 'arch={{ automq_host_arch }}' in docker['ansible.builtin.apt_repository']['repo']
    assert '_linux_{{ automq_host_arch }}.tar.gz' in lego['ansible.builtin.unarchive']['src']
