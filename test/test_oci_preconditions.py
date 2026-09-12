"""OCI leases use native RSA-signed ETags, not ignored S3 PUT conditions."""
import base64
import importlib.util
import io
import json
from pathlib import Path
import re
import tempfile
import shlex
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import urlparse
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

path = Path(__file__).resolve().parents[1] / 'green/src/resources/io/github/getcolors/automq/tools/ansible/store.py'
spec = importlib.util.spec_from_file_location('oci_store', path)
store = importlib.util.module_from_spec(spec)
spec.loader.exec_module(store)


class Response:
    def __init__(self, etag):
        self.headers = {'etag': etag}
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass


class OciPreconditions(unittest.TestCase):
    def test_native_signatures_and_stale_replacement(self):
        private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem = private.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption())
        environment = {'AUTOMQ_OCI_SIGNING_KEY_B64': base64.b64encode(pem).decode(), 'AUTOMQ_OCI_SIGNING_KEY_ID': 'tenancy/user/fingerprint'}
        client = SimpleNamespace(meta=SimpleNamespace(endpoint_url='https://example.compat.objectstorage.eu-frankfurt-1.oraclecloud.com'))
        state = {'etag': 'native-1', 'body': b'original'}
        methods = []
        def serve(request, timeout):
            methods.append(request.method)
            self.assertEqual(timeout, 60)
            self.assertEqual(urlparse(request.full_url).hostname, 'objectstorage.eu-frankfurt-1.oraclecloud.com')
            self.assertTrue(request.full_url.endswith('/n/example/b/ops/o/_colors%2Fprofile%2Flease.json'))
            headers = {key.lower(): value for key, value in request.header_items()}
            authorization = headers['authorization']
            signed_names = re.search(r'headers="([^"]+)"', authorization).group(1).split()
            values = {**headers, '(request-target)': request.method.lower() + ' ' + urlparse(request.full_url).path}
            signed = '\n'.join(name + ': ' + values[name] for name in signed_names).encode()
            signature = base64.b64decode(re.search(r'signature="([^"]+)"', authorization).group(1))
            private.public_key().verify(signature, signed, padding.PKCS1v15(), hashes.SHA256())
            if request.method == 'PUT':
                self.assertIn('if-match', signed_names)
                self.assertEqual(headers['content-length'], str(len(request.data)))
                self.assertEqual(headers['x-content-sha256'], base64.b64encode(store.hashlib.sha256(request.data).digest()).decode())
                if headers['if-match'] != state['etag']:
                    raise HTTPError(request.full_url, 412, 'PreconditionFailed', {}, io.BytesIO())
                state.update(etag='native-2', body=request.data)
            return Response(state['etag'])
        with patch.dict('os.environ', environment), patch('urllib.request.urlopen', side_effect=serve):
            etag = store._etag(client, 'ops', '_colors/profile/lease.json')
            self.assertEqual(etag, 'native-1')
            self.assertTrue(store._put_if_match(client, 'ops', '_colors/profile/lease.json', {'holder': 'winner'}, etag))
            self.assertFalse(store._put_if_match(client, 'ops', '_colors/profile/lease.json', {'holder': 'loser'}, etag))
        self.assertEqual(json.loads(state['body']), {'holder': 'winner'})
        self.assertEqual(methods, ['HEAD', 'PUT', 'PUT'])

    def test_missing_native_key_fails_before_request(self):
        client = SimpleNamespace(meta=SimpleNamespace(endpoint_url='https://example.compat.objectstorage.eu-frankfurt-1.oraclecloud.com'))
        with patch.dict('os.environ', {}, clear=True), patch('urllib.request.urlopen') as request:
            with self.assertRaises(KeyError):
                store._put_if_match(client, 'ops', 'lease', {}, 'native-etag')
            request.assert_not_called()


class BucketReadiness(unittest.TestCase):
    def modules(self):
        root = Path(__file__).resolve().parents[1]
        for relative in ('green/src/resources/io/github/getcolors/automq/tools/ansible/store.py',
                         'red/resources/tools/ansible/store.py',
                         'blue/src/package_automq_blue/resources/tools/ansible/store.py'):
            module_spec = importlib.util.spec_from_file_location('readiness_store', root / relative)
            module = importlib.util.module_from_spec(module_spec)
            module_spec.loader.exec_module(module)
            yield module

    def fake(self, module, fault=None):
        objects, calls = {}, []
        def error(code):
            return module.ClientError({'Error': {'Code': code, 'Message': 'test response'}}, 'GetObject')
        def listing(**kwargs):
            calls.append(('list', kwargs['Bucket']))
            return {}
        def get(**kwargs):
            bucket, name = kwargs['Bucket'], kwargs['Key']
            calls.append(('get', bucket))
            if fault == ('missing', bucket) and (bucket, name) not in objects:
                raise error('SignatureDoesNotMatch')
            if fault == ('read', bucket) and (bucket, name) in objects:
                raise error('SignatureDoesNotMatch')
            if (bucket, name) not in objects:
                raise error('NoSuchKey')
            return {'Body': io.BytesIO(b'corrupt' if fault == ('bytes', bucket) else objects[bucket, name])}
        def put(**kwargs):
            calls.append(('put', kwargs['Bucket']))
            self.assertIsInstance(kwargs['Body'], bytes)
            self.assertIn(b'\x00\xff', kwargs['Body'])
            objects[kwargs['Bucket'], kwargs['Key']] = kwargs['Body']
        def delete(**kwargs):
            bucket = kwargs['Bucket']
            calls.append(('delete', bucket))
            if fault != ('delete', bucket):
                objects.pop((bucket, kwargs['Key']), None)
        return SimpleNamespace(list_objects_v2=listing, get_object=get, put_object=put, delete_object=delete), objects, calls

    def test_both_buckets_must_complete_missing_read_and_byte_roundtrip_before_cas(self):
        for module in self.modules():
            s3, objects, calls = self.fake(module)
            args = SimpleNamespace(endpoint='unused', region='unused', profile='demo', data_bucket='data', ops_bucket='ops')
            with patch.object(module, 'client', return_value=s3), \
                 patch.object(module, 'put_json', side_effect=[True, False]) as create, \
                 patch.object(module, '_etag', return_value='current'), \
                 patch.object(module, '_put_if_match', side_effect=[True, False]), \
                 patch.object(module, 'get_json', return_value={'value': 'second'}), \
                 patch('sys.stdout', new_callable=io.StringIO):
                module.cmd_preconditions(args)
                self.assertEqual(create.call_count, 2)
            expected = [(op, bucket) for bucket in ('data', 'ops') for op in ('list', 'get', 'put', 'get', 'delete', 'get')]
            self.assertEqual(calls[:12], expected)
            self.assertEqual(objects, {})

    def test_failed_missing_reads_positive_reads_bytes_or_delete_refuse_before_cas(self):
        for module in self.modules():
            for operation in ('missing', 'read', 'bytes', 'delete'):
                for bucket in ('data', 'ops'):
                    with self.subTest(operation=operation, bucket=bucket):
                        s3, objects, calls = self.fake(module, (operation, bucket))
                        args = SimpleNamespace(endpoint='unused', region='unused', profile='demo', data_bucket='data', ops_bucket='ops')
                        with patch.object(module, 'client', return_value=s3), patch.object(module, 'put_json') as cas:
                            with self.assertRaises((module.ClientError, RuntimeError)):
                                module.cmd_preconditions(args)
                            cas.assert_not_called()
                        if operation in ('read', 'bytes'):
                            self.assertIn(('delete', bucket), calls)
                            self.assertEqual(objects, {})

    def test_failed_cleanup_is_retried_before_any_new_probe_or_success(self):
        for module in self.modules():
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'probes.json'
                args = SimpleNamespace(endpoint='unused', region='unused', profile='demo',
                                       data_bucket='data', ops_bucket='ops', probe_ledger=str(path))
                s3, objects, calls = self.fake(module)
                delete = s3.delete_object
                s3.delete_object = lambda **kwargs: (_ for _ in ()).throw(module.ClientError(
                    {'Error': {'Code': 'SignatureDoesNotMatch'}}, 'DeleteObject'))
                with patch.object(module, 'client', return_value=s3), patch.object(module, 'put_json') as cas:
                    with self.assertRaises(module.ClientError):
                        module.cmd_preconditions(args)
                    pending = json.loads(path.read_text())
                    self.assertEqual(len(pending['objects']), 1)
                    self.assertEqual(len(objects), 1)
                    calls_before_retry = list(calls)
                    with self.assertRaises(module.ClientError):
                        module.cmd_preconditions(args)
                    self.assertEqual(calls, calls_before_retry)
                    self.assertEqual(json.loads(path.read_text()), pending)
                    cas.assert_not_called()
                s3.delete_object = delete
                with patch.object(module, 'client', return_value=s3), \
                     patch.object(module, 'put_json', side_effect=[True, False]), \
                     patch.object(module, '_etag', return_value='current'), \
                     patch.object(module, '_put_if_match', side_effect=[True, False]), \
                     patch.object(module, 'get_json', return_value={'value': 'second'}), \
                     patch('sys.stdout', new_callable=io.StringIO):
                    module.cmd_preconditions(args)
                self.assertEqual(objects, {})
                self.assertFalse(path.exists())
                self.assertEqual(calls[len(calls_before_retry)], ('delete', 'data'))

    def test_put_that_lands_then_raises_is_still_cleaned(self):
        for module in self.modules():
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'probes.json'
                args = SimpleNamespace(endpoint='unused', region='unused', profile='demo',
                                       data_bucket='data', ops_bucket='ops', probe_ledger=str(path))
                s3, objects, calls = self.fake(module)
                put = s3.put_object
                def ambiguous_put(**kwargs):
                    self.assertTrue(path.exists(), 'intent must be durable before PUT')
                    put(**kwargs)
                    raise TimeoutError('response lost after write')
                s3.put_object = ambiguous_put
                with patch.object(module, 'client', return_value=s3), patch.object(module, 'put_json') as cas:
                    with self.assertRaises(TimeoutError):
                        module.cmd_preconditions(args)
                    cas.assert_not_called()
                self.assertEqual(objects, {})
                self.assertIn(('delete', 'data'), calls)
                self.assertFalse(path.exists())

    def test_wrong_identity_or_foreign_key_ledger_refuses_before_client_creation(self):
        for module in self.modules():
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'probes.json'
                args = SimpleNamespace(endpoint='unused', region='unused', profile='demo',
                                       data_bucket='data', ops_bucket='ops', probe_ledger=str(path))
                with module.ProbeLedger(args) as ledger:
                    ledger.remember('ops', '_colors/demo/preconditions/' + 'a' * 32 + '.json')
                original = json.loads(path.read_text())
                for wrong in ({**original, 'identity': {**original['identity'], 'endpoint': 'other'}},
                              {**original, 'objects': [{'bucket': 'state', 'key': '_colors/backend-owner.json'}]}):
                    path.write_text(json.dumps(wrong))
                    with patch.object(module, 'client') as client:
                        with self.assertRaises(RuntimeError):
                            module.cmd_preconditions(args)
                        client.assert_not_called()
                    self.assertEqual(json.loads(path.read_text()), wrong)

    def test_conditional_create_rejection_does_not_delete_another_writer(self):
        for module in self.modules():
            s3, objects, calls = self.fake(module)
            args = SimpleNamespace(endpoint='unused', region='unused', profile='demo', data_bucket='data', ops_bucket='ops')
            def competitor(client, bucket, name, payload, **kwargs):
                objects[bucket, name] = b'another writer'
                return False
            with patch.object(module, 'client', return_value=s3), patch.object(module, 'put_json', side_effect=competitor):
                with self.assertRaisesRegex(RuntimeError, 'fresh conditional create'):
                    module.cmd_preconditions(args)
            self.assertEqual(list(objects.values()), [b'another writer'])

    def test_ambiguous_conditional_create_is_recorded_and_cleaned(self):
        for module in self.modules():
            with tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / 'probes.json'
                args = SimpleNamespace(endpoint='unused', region='unused', profile='demo',
                                       data_bucket='data', ops_bucket='ops', probe_ledger=str(path))
                s3, objects, calls = self.fake(module)
                def ambiguous(client, bucket, name, payload, **kwargs):
                    self.assertEqual(json.loads(path.read_text())['objects'], [{'bucket': bucket, 'key': name}])
                    objects[bucket, name] = json.dumps(payload).encode()
                    raise TimeoutError('conditional response lost after write')
                with patch.object(module, 'client', return_value=s3), patch.object(module, 'put_json', side_effect=ambiguous):
                    with self.assertRaises(TimeoutError):
                        module.cmd_preconditions(args)
                self.assertEqual(objects, {})
                self.assertFalse(path.exists())

    def test_wrong_ledger_cli_is_terminal_and_wrapper_only_retries_transient_failures(self):
        root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as directory:
            ledger = Path(directory) / 'probes.json'
            ledger.write_text(json.dumps({'schema': 1, 'identity': {'profile': 'other'}, 'objects': []}))
            for module in self.modules():
                result = subprocess.run([sys.executable, module.__file__, '--profile', 'demo', '--endpoint', 'unused',
                                         'preconditions', '--probe-ledger', str(ledger)], capture_output=True, text=True, timeout=10)
                self.assertEqual(result.returncode, 2)
                self.assertIn('different storage identity', result.stderr)
            source = (root / 'green/src/resources/io/github/getcolors/automq/tools/ansible/main.yml').read_text()
            wrapper = re.search(r"- '(while true;[^\n]+)'", source).group(1)
            program = Path(directory) / 'probe'
            calls = Path(directory) / 'calls'
            for status, count in [(1, 2), (2, 1)]:
                calls.write_text('')
                program.write_text('#!/bin/bash\necho call >> ' + shlex.quote(str(calls)) + '\n'
                                   + 'if [ $(wc -l < ' + shlex.quote(str(calls)) + ') -eq 1 ]; then exit ' + str(status) + '; fi\nexit 0\n')
                program.chmod(0o755)
                script = wrapper.replace('/usr/local/bin/automq-store', shlex.quote(str(program))).replace('sleep 15', 'sleep 0')
                result = subprocess.run(['/bin/bash', '-c', script], capture_output=True, text=True, timeout=5)
                self.assertEqual(result.returncode, 0 if status == 1 else 2)
                self.assertEqual(len(calls.read_text().splitlines()), count)


if __name__ == '__main__':
    unittest.main()
