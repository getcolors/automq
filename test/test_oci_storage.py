"""Native OCI ownership and cleanup boundaries, independent of cloud access."""
import importlib.util
import json
from pathlib import Path
import unittest
from unittest.mock import patch

path = Path(__file__).resolve().parents[1] / 'green/src/resources/io/github/getcolors/automq/tools/storage/oci-storage.py'
spec = importlib.util.spec_from_file_location('oci_storage', path)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
OPTS = {'profile': 'automq-test', 'oci-config-file-profile': 'DEFAULT', 'automq-r2-region': 'eu-frankfurt-1', 'oci-namespace': 'example', 'oci-compartment-id': 'compartment', 'automq-data-r2-bucket': 'data', 'automq-ops-r2-bucket': 'ops', 'compute-prevent-destroy': False}


class Ownership(unittest.TestCase):
    def run_case(self, action, resources=None, listed=None, failures=None, guard=False, live_changes=None):
        calls = []
        def run(args, **kwargs):
            calls.append(args)
            if args == ['tofu', 'state', 'list']:
                return 'owned' if resources else ''
            if args == ['tofu', 'show', '-json']:
                return json.dumps({'values': {'root_module': {'resources': resources}}})
            if 'bucket' in args and 'get' in args:
                return json.dumps({'data': {'id': 'bucket-ocid', 'name': 'data', 'namespace': 'example', 'compartment-id': 'compartment', 'freeform-tags': {'colors-profile': 'automq-test', 'colors-owner': 'automq-storage'}, **(live_changes or {})}})
            if 'bulk-delete' in args:
                return json.dumps({'delete-failures': failures or {}})
            if 'list' in args:
                return json.dumps({'data': listed or []})
            return ''
        with patch.object(module, 'run', side_effect=run):
            module.operate(action, {**OPTS, 'compute-prevent-destroy': guard})
        return calls

    def owned(self, role='data', **changes):
        return {'address': f'oci_objectstorage_bucket.application["{role}"]', 'values': {'name': role, 'namespace': 'example', 'compartment_id': 'compartment', 'bucket_id': 'bucket-ocid', **changes}}

    def test_complete_listing_proves_absence(self):
        calls = self.run_case('preflight')
        self.assertEqual(sum('bucket' in c and 'list' in c for c in calls), 1)
        self.assertTrue(any('--all' in c for c in calls))

    def test_foreign_bucket_refused(self):
        with self.assertRaisesRegex(RuntimeError, 'adopt'):
            self.run_case('preflight', listed=[{'name': 'ops'}])

    def test_owned_pair_does_not_probe(self):
        self.assertFalse(any('oci' in c for c in self.run_case('preflight', resources=[self.owned(), self.owned('ops')])))

    def test_changed_identity_refused_before_cleanup(self):
        with self.assertRaisesRegex(RuntimeError, 'identity changed'):
            self.run_case('cleanup', resources=[self.owned(namespace='other')])

    def test_guard_precedes_every_command(self):
        with patch.object(module, 'run') as run:
            with self.assertRaisesRegex(RuntimeError, 'prevent-destroy'):
                module.operate('cleanup', {**OPTS, 'compute-prevent-destroy': True})
            run.assert_not_called()

    def test_cleanup_only_owned_bucket(self):
        calls = self.run_case('cleanup', resources=[self.owned()])
        deletes = [c for c in calls if 'bulk-delete' in c]
        self.assertEqual(len(deletes), 1)
        self.assertEqual(deletes[0][-1], 'data')

    def test_foreign_same_name_replacement_refused(self):
        with self.assertRaisesRegex(RuntimeError, 'live OCI bucket identity'):
            self.run_case('cleanup', resources=[self.owned()], live_changes={'id': 'replacement-ocid'})
        with self.assertRaisesRegex(RuntimeError, 'live OCI bucket identity'):
            self.run_case('cleanup', resources=[self.owned()], live_changes={'freeform-tags': {}})

    def test_partial_purge_refuses_destroy(self):
        with self.assertRaisesRegex(RuntimeError, 'deletion failed'):
            self.run_case('cleanup', resources=[self.owned()], failures={'object': 'denied'})


if __name__ == '__main__':
    unittest.main()
