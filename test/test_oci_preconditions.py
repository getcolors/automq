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


if __name__ == '__main__':
    unittest.main()
