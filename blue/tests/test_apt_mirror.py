"""The optional mirror must preserve Ubuntu suites and signature verification."""
from pathlib import Path
import re


def test_security_mirror_preserves_signed_sources_and_can_change_again():
    template = (Path(__file__).parents[1] / "src/package_automq_blue/resources/tools/ansible/main.yml").read_text()
    regexp = re.search(r"        regexp: '([^']+)'", template).group(1)
    original = """Types: deb
URIs: http://us-central1.gce.archive.ubuntu.com/ubuntu/
Suites: noble noble-updates noble-backports
Components: main restricted universe multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg

Types: deb
URIs: http://security.ubuntu.com/ubuntu/
Suites: noble-security
Components: main restricted universe multiverse
Signed-By: /usr/share/keyrings/ubuntu-archive-keyring.gpg
"""
    mirror = "http://us-central1.gce.archive.ubuntu.com/ubuntu/"
    updated, count = re.subn(regexp, "URIs: " + mirror, original)
    assert count == 1
    assert updated == original.replace("http://security.ubuntu.com/ubuntu/", mirror)
    assert re.sub(regexp, "URIs: " + mirror, updated) == updated
    assert re.sub(regexp, "URIs: https://security.ubuntu.com/ubuntu/", updated).count("URIs: https://security.ubuntu.com/ubuntu/") == 1
