from pathlib import Path

import pytest
from blue.workflow import run

from conftest import PARAMS, fixture
from package_automq_blue import cluster, tools, validate, workflow


def test_create_compute_precedes_downstream_and_delete_unwinds_application_first():
    assert workflow.wire_fn('automq/start', {'blue/event': 'create'})[1] == 'automq/infrastructure'
    assert workflow.wire_fn('automq/infrastructure', {'blue/event': 'create'})[1] == 'automq/ssh-config'
    assert workflow.wire_fn('automq/start', {'blue/event': 'delete'})[1] == 'automq/ansible'
    assert workflow.wire_fn('automq/dns', {'blue/event': 'delete'})[1] == 'automq/infrastructure'
    assert len(workflow.wire_fn('automq/infrastructure', {'blue/event': 'delete'})) == 1


def test_validate_has_no_application_or_compute_successor():
    assert len(workflow.wire_fn('automq/start', {'blue/event': 'validate'})) == 1
    assert workflow.wire_fn('automq/infrastructure', {'blue/event': 'validate'}) is None


async def test_credential_free_build_emits_library_shared_and_node_documents(tmp_path, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('build cannot inspect remote compute')
    monkeypatch.setattr(tools, 'orchestrate', forbidden)
    opts = fixture({'workdir': str(tmp_path), 'blue/event': 'build'})
    result = await tools.infrastructure_step(opts)
    assert result['blue/exit'] == 0, result.get('blue/err')
    assert [node['node_id'] for node in result['colors-compute/cluster']['nodes']] == ['0', '1', '2']
    directory = Path(tools.tool_dir(opts, tools.infrastructure_tool))
    assert list((directory / 'shared').glob('*.tf.json'))
    assert list((directory / 'nodes/2').glob('*.tf.json'))
    assert not (directory / 'main.tf').exists()
    assert result['ssh-private-key-path'] == '/home/build-placeholder/.ssh/automq-fixture'


async def test_compute_adapter_passes_application_policy_and_only_adopts_success(monkeypatch):
    seen = []
    async def operation(opts, topology, requirements):
        seen.append((topology, requirements))
        return {'status': 'ready', 'cluster': PARAMS, 'key': {'private_key_path': '/tmp/owned-key'}}
    monkeypatch.setattr(tools, 'orchestrate', operation)
    result = await tools.infrastructure_step(fixture({'blue/event': 'create'}))
    assert result['colors-compute/cluster'] == PARAMS
    assert result['ssh-private-key-path'] == '/tmp/owned-key'
    assert seen[0][0] == [{'role': None, 'count': 3}]
    policy = seen[0][1]
    assert policy['private'] is True
    assert policy['legacy_state_keys'] == ['automq-fixture/automq-infrastructure.tfstate']
    assert {rule['from_port'] for rule in policy['security']['ingress']} == {22, 9092, 9093, 9094}
    async def refused(*args):
        return {'status': 'error'}
    monkeypatch.setattr(tools, 'orchestrate', refused)
    failed = await tools.infrastructure_step(fixture({'blue/event': 'create'}))
    assert failed['blue/exit'] == 1 and 'colors-compute/cluster' not in failed


async def test_real_delete_adopts_owned_inventory_and_unreadable_refuses(monkeypatch):
    async def no_tools(*args): return []
    monkeypatch.setattr(validate, 'runtime_errors', no_tools)
    monkeypatch.setattr(validate, 'secret_errors', lambda *args: [])
    async def read(*args):
        return {'status': 'present', 'cluster': PARAMS, 'key': {'mode': 'managed', 'private_key_path': '/tmp/owned'}}
    monkeypatch.setattr(workflow, 'read_deployment', read)
    opts = fixture({'blue/event': 'delete', 'compute-prevent-destroy': False})
    result = await workflow.start_step(opts, {})
    assert result['blue/exit'] == 0 and result['colors-compute/cluster'] == PARAMS
    assert result['ssh-private-key-path'] == '/tmp/owned'
    async def refused(*args): return {'status': 'error'}
    monkeypatch.setattr(workflow, 'read_deployment', refused)
    assert (await workflow.start_step(opts, {}))['blue/exit'] != 0


async def test_protected_delete_never_inspects_or_mutates_cloud(monkeypatch):
    async def no_tools(*args): return []
    def forbidden(*args): raise AssertionError('protected delete')
    monkeypatch.setattr(validate, 'runtime_errors', no_tools)
    monkeypatch.setattr(validate, 'secret_errors', lambda *args: [])
    monkeypatch.setattr(workflow, 'read_deployment', forbidden)
    assert (await workflow.start_step(fixture({'blue/event': 'delete'}), {}))['blue/exit'] != 0


def test_application_has_no_provider_allowlist_and_capabilities_fail_closed():
    do = fixture({'provider-compute': 'digitalocean', 'digitalocean-region': 'ams3',
                  'digitalocean-size': 's-2vcpu-4gb', 'digitalocean-image': 'ubuntu-24-04-x64',
                  'automq-ssh-sources': ['0.0.0.0/0'], 'automq-kafka-sources': []})
    assert validate.state_errors(do) == []
    assert validate.state_errors(fixture({'provider-backend': 'local'}))
    assert validate.state_errors(fixture({'vultr-ssh-sources': []}))
    with pytest.raises(ValueError):
        cluster.nodes(fixture({'blue/event': 'create'}))
    missing_private = {**PARAMS, 'nodes': [{**node, 'vpc_ip': None} for node in PARAMS['nodes']]}
    with pytest.raises(ValueError, match='vpc_ip'):
        cluster.nodes(fixture(), missing_private)


async def test_native_sdk_create_carries_library_join_into_application_stages(monkeypatch):
    async def no_tools(*args): return []
    monkeypatch.setattr(validate, 'runtime_errors', no_tools)
    monkeypatch.setattr(validate, 'secret_errors', lambda *args: [])
    monkeypatch.setattr(workflow.ssh_config, 'preflight', lambda opts: {**opts, 'blue/exit': 0})
    async def compute(*args):
        return {'status': 'ready', 'cluster': PARAMS, 'key': {'private_key_path': '/tmp/owned'}}
    monkeypatch.setattr(tools, 'orchestrate', compute)
    visited = []
    def stage(name):
        async def execute(opts):
            visited.append(name)
            assert tools.nodes(opts)[2]['vpc-ip'] == '10.40.0.5'
            assert opts['ssh-private-key-path'] == '/tmp/owned'
            return {**opts, 'blue/exit': 0}
        return execute
    for name in ('ansible_local_step', 'dns_step', 'ansible_step', 'acceptance_step'):
        monkeypatch.setattr(tools, name, stage(name))
    # The app graph dispatches the real compute adapter; application services are
    # stubbed because this test validates wiring rather than cloud/Ansible tools.
    from blue.workflow import workflow as native_workflow
    result = await run(native_workflow(start='automq/start', wire_fn=workflow.wire_fn), fixture({'blue/event': 'create'}))
    assert result['blue/exit'] == 0, result.get('blue/err')
    assert visited == ['ansible_local_step', 'dns_step', 'ansible_step', 'acceptance_step']


def test_delete_inventory_retains_recorded_nodes_after_desired_scale_down():
    result = cluster.nodes(fixture({'blue/event': 'delete', 'automq-node-count': 1}), PARAMS)
    assert [node['index'] for node in result] == [0, 1, 2]


async def test_full_native_build_renders_compute_dns_and_ansible_without_credentials(tmp_path):
    result = await run(workflow.automq_workflow, fixture({'blue/event': 'build', 'workdir': str(tmp_path)}))
    assert result['blue/exit'] == 0, result.get('blue/err')
    assert list(tmp_path.rglob('inventory.json'))
    assert list(tmp_path.rglob('compose.yml'))
    assert list(tmp_path.rglob('*.tf.json'))
