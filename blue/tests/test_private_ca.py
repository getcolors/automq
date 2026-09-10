"""Exercise real CA issuance, IP identity checks, and issuer-loss refusal."""
import subprocess
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parents[2]
TREES = [ROOT / 'green/src/resources/io/github/getcolors/automq/tools',
         ROOT / 'red/resources/tools', ROOT / 'blue/src/package_automq_blue/resources/tools']

@pytest.mark.parametrize('tree', TREES)
def test_private_ca_issuance_and_recovery_guard(tree, tmp_path):
    store = tmp_path / 'store'
    store.write_text('''#!/usr/bin/env python3
import pathlib, sys, hashlib
p = pathlib.Path(__file__).with_name('published')
if sys.argv[1] == 'tls-fingerprint': print(p.read_text() if p.exists() else '')
elif sys.argv[1] == 'tls-publish':
    cert = pathlib.Path(sys.argv[sys.argv.index('--cert') + 1])
    fp = hashlib.sha256(cert.read_bytes()).hexdigest()
    p.write_text(fp)
    print(fp)
''')
    store.chmod(0o700)
    source = (tree / 'ansible/cert.sh').read_text()
    for old, new in [('/usr/local/bin/automq-store', str(store)),
                     ('/etc/automq', str(tmp_path / 'automq')),
                     ('<{ automq-tls-mode }>', 'private-ca'),
                     ('<{ profile }>', 'automq-test'),
                     ('<{ automq-letsencrypt-email }>', 'unused@example.com')]:
        source = source.replace(old, new)
    script = tmp_path / 'cert.sh'
    script.write_text(source)
    command = ['bash', str(script), '192.0.2.10,192.0.2.11,192.0.2.12']
    first = subprocess.run(command, capture_output=True, text=True)
    assert first.returncode == 0, first.stderr
    ca = tmp_path / 'automq/ca'
    key = (ca / 'ca.key').read_bytes()
    cert = (ca / 'fullchain.pem').read_bytes()
    assert (ca / 'ca.key').stat().st_mode & 0o077 == 0
    for address, expected in [('192.0.2.11', 0), ('192.0.2.99', 2)]:
        result = subprocess.run(['openssl', 'verify', '-CAfile', str(ca / 'ca.crt'),
                                 '-verify_ip', address, str(ca / 'server.crt')], capture_output=True)
        assert result.returncode == expected
    second = subprocess.run(command, capture_output=True, text=True)
    assert second.returncode == 0, second.stderr
    assert 'already published' in second.stdout
    assert (ca / 'ca.key').read_bytes() == key
    assert (ca / 'fullchain.pem').read_bytes() == cert
    (ca / 'ca.key').unlink()
    refused = subprocess.run(command, capture_output=True, text=True)
    assert refused.returncode != 0
    assert 'restore the existing issuer CA' in refused.stderr
