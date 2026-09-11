"""Exercise competing GCS marker and lease writes through real botocore signing."""
import importlib.util
from pathlib import Path
import unittest
from unittest.mock import patch
from botocore.awsrequest import AWSResponse

path = Path(__file__).parents[1] / "green/src/resources/io/github/getcolors/automq/tools/ansible/store.py"
spec = importlib.util.spec_from_file_location("store", path)
store = importlib.util.module_from_spec(spec)
spec.loader.exec_module(store)


class Body:
    def __init__(self, value):
        self.value = value

    def stream(self, amt=None, decode_content=False):
        yield self.value


class GcsPreconditions(unittest.TestCase):
    def test_gcs_uses_generation_and_refuses_missing_generation(self):
        with patch.dict("os.environ", {"AUTOMQ_R2_ACCESS_KEY_ID": "test", "AUTOMQ_R2_SECRET_ACCESS_KEY": "test"}):
            client = store.client("https://storage.googleapis.com", "us-central1")
        with patch.object(client, "head_object", return_value={"ETag": "content-md5", "ResponseMetadata": {"HTTPHeaders": {"x-goog-generation": "42"}}}):
            self.assertEqual(store._etag(client, "bucket", "lease"), "42")
        with patch.object(client, "head_object", return_value={"ETag": "content-md5"}):
            with self.assertRaisesRegex(RuntimeError, "omitted its generation"):
                store._etag(client, "bucket", "lease")

    def test_competing_creates_and_stale_takeover(self):
        with patch.dict("os.environ", {"AUTOMQ_R2_ACCESS_KEY_ID": "test", "AUTOMQ_R2_SECRET_ACCESS_KEY": "test"}):
            client = store.client("https://storage.googleapis.com", "us-central1")
        generation = 0
        seen = []

        def send(request):
            nonlocal generation
            condition = request.headers.get("x-goog-if-generation-match")
            seen.append(condition)
            if condition is None or int(condition) != generation:
                return AWSResponse(request.url, 412, {}, Body(b"<Error><Code>PreconditionFailed</Code></Error>"))
            generation += 1
            return AWSResponse(request.url, 200, {"x-goog-generation": str(generation)}, Body(b""))

        with patch.object(client._endpoint.http_session, "send", side_effect=send):
            self.assertTrue(store.put_json(client, "bucket", "lease", {"holder": "one"}, if_absent=True))
            self.assertFalse(store.put_json(client, "bucket", "lease", {"holder": "two"}, if_absent=True))
            self.assertTrue(store._put_if_match(client, "bucket", "lease", {"holder": "two"}, "1"))
            self.assertFalse(store._put_if_match(client, "bucket", "lease", {"holder": "three"}, "1"))
        self.assertEqual(seen, [b"0", b"0", b"1", b"1"])


if __name__ == "__main__":
    unittest.main()
