import json
from unittest.mock import AsyncMock
from types import SimpleNamespace
import pytest
from package_automq_blue import storage, workflow

OPTS = {"profile": "owned", "workdir": "/tmp", "automq-storage-managed": True, "automq-data-r2-bucket": "owned-data", "automq-ops-r2-bucket": "owned-ops", "automq-r2-region": "eu-central-1"}

def result(exit=0, out="", err=""):
    return SimpleNamespace(exit=exit, out=out, err=err)

@pytest.mark.asyncio
@pytest.mark.parametrize("probe", [result(), result(1, err="(403) Forbidden")])
async def test_refuses_untracked_existing_or_inaccessible_buckets(monkeypatch, probe):
    runner = AsyncMock(side_effect=[result(), result(), probe])
    monkeypatch.setattr(storage.runtime, "exec", runner)
    with pytest.raises(RuntimeError, match="refuses to adopt"):
        await storage.ownership_preflight(OPTS)

@pytest.mark.asyncio
async def test_tracked_bucket_skips_probe_and_absent_bucket_is_accepted(monkeypatch):
    runner = AsyncMock(side_effect=[result(), result(out='aws_s3_bucket.application["data"]\n'), result(out=json.dumps({'values': {'root_module': {'resources': [{'address': 'aws_s3_bucket.application["data"]', 'values': {'bucket': 'owned-data'}}]}}})), result(1, err="(404) Not Found")])
    monkeypatch.setattr(storage.runtime, "exec", runner)
    await storage.ownership_preflight(OPTS)
    assert runner.call_count == 4

def test_storage_lifecycle_order():
    assert workflow.wire_fn("automq/infrastructure", {**OPTS, "blue/event": "create"})[1] == "automq/storage"
    assert workflow.wire_fn("automq/dns", {**OPTS, "blue/event": "delete"})[1] == "automq/storage"
    assert workflow.wire_fn("automq/storage", {**OPTS, "blue/event": "delete"})[1] == "automq/infrastructure"

def test_retired_compute_routes_directly_to_finalization_on_retry():
    opts = {**OPTS, "s3-bucket-mode": "managed", "automq/finalize-only": True}
    assert workflow.next_steps("automq/start", ["automq/ansible"], opts) == [("automq/backend-finalize", opts)]
    assert workflow.next_steps("automq/backend-finalize", [], opts) == []
    assert workflow.wire_fn("automq/infrastructure", {**opts, "blue/event": "delete"})[1] == "automq/backend-finalize"

@pytest.mark.asyncio
async def test_finalization_failure_stops_delete_without_succeeding(monkeypatch):
    monkeypatch.setattr(workflow, "finalize_backend", AsyncMock(side_effect=ValueError("live state")))
    result = await workflow.backend_finalize_step(OPTS)
    assert result["blue/exit"] == 1
    assert workflow.next_steps("automq/backend-finalize", [], result) == []

@pytest.mark.asyncio
async def test_fresh_state_is_allowed_but_unreadable_state_fails_closed(monkeypatch):
    runner = AsyncMock(side_effect=[result(), result(1, err="No state file was found!"), result(1, err="(404) Not Found"), result(1, err="(404) Not Found")])
    monkeypatch.setattr(storage.runtime, "exec", runner)
    await storage.ownership_preflight(OPTS)
    assert runner.call_count == 4
    monkeypatch.setattr(storage.runtime, "exec", AsyncMock(side_effect=[result(), result(1, err="AccessDenied")]))
    with pytest.raises(RuntimeError, match="state operation failed"):
        await storage.ownership_preflight(OPTS)

@pytest.mark.asyncio
async def test_renamed_tracked_bucket_must_not_adopt_existing_destination(monkeypatch):
    state = {"values": {"root_module": {"resources": [{"address": 'aws_s3_bucket.application["data"]', "values": {"bucket": "old-data"}}]}}}
    runner = AsyncMock(side_effect=[result(), result(out='aws_s3_bucket.application["data"]'), result(out=json.dumps(state)), result()])
    monkeypatch.setattr(storage.runtime, "exec", runner)
    with pytest.raises(RuntimeError, match="refuses to adopt"):
        await storage.ownership_preflight(OPTS)
