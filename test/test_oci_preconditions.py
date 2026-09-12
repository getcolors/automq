"""OCI leases use native RSA-signed ETags, not ignored S3 PUT conditions."""
import base64
import importlib.util
import io
import json
from pathlib import Path
import re
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


if __name__ == '__main__':
    unittest.main()
