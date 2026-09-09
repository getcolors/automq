#!/usr/bin/env python3
"""Check that a rendered fixture preserves its declared public IPv6 policy."""
import json
from pathlib import Path
import sys

rules = []
for path in Path(sys.argv[1]).rglob('*.tf.json'):
    document = json.loads(path.read_text())
    if 'vultr_firewall_rule' in document.get('resource', {}):
        rules.extend(document.get('locals', {}).get('ingress', {}).values())
public = [rule for rule in rules if rule.get('ip_type') == 'v6']
assert {str(rule['port']) for rule in public} == {'22', '9092'}
assert all(rule['subnet'] == '::' and rule['subnet_size'] == 0 for rule in public)
