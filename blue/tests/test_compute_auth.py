import os
from copy import deepcopy
import pytest
from package_automq_blue import tools, validate
from conftest import fixture, PARAMS

@pytest.mark.asyncio
async def test_dns_backend_environment_is_separate(monkeypatch):
    values=fixture({'provider-backend':'r2','blue/event':'create','colors-compute/cluster':PARAMS,
                    'r2-access-key-id':'synthetic-id','r2-secret-access-key':'synthetic-secret','cloudflare-api-token':'synthetic-dns'})
    captured={}
    async def execute(opts, specs, **kwargs):
        captured.update(kwargs['env'])
        return {**opts,'blue/exit':0}
    monkeypatch.setattr(tools.tofu,'tofu_with_spec',execute)
    before=deepcopy(values);environment=dict(os.environ)
    await tools.dns_step(values)
    assert captured=={'AWS_ACCESS_KEY_ID':'synthetic-id','AWS_SECRET_ACCESS_KEY':'synthetic-secret','CLOUDFLARE_API_TOKEN':'synthetic-dns'}
    assert values==before and dict(os.environ)==environment
    assert validate.tofu_env(values,'provider-compute')=={}

def test_compute_credentials_deferred_until_state():
    values=fixture({'provider-backend':'r2'})
    for event in ('create','delete'):
        assert not any('VULTR_API_KEY' in error for error in validate.secret_errors(values,event))
    assert any('VULTR_API_KEY' in error for error in validate.secret_errors(values,'validate'))
