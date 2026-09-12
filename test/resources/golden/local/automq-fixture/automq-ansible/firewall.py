#!/usr/bin/env python3
"""Own one OCI INPUT chain without changing platform, Docker or iSCSI rules."""
import fcntl
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys


def desired(config):
    profile = config['profile']
    if not isinstance(profile, str) or not re.fullmatch(r'[a-z][a-z0-9-]{0,62}', profile):
        raise ValueError('invalid firewall profile')
    host = ipaddress.IPv4Address(config['private_host'])
    if not isinstance(config['peers'], list) or not isinstance(config['kafka_sources'], list):
        raise ValueError('firewall peers and sources must be lists')
    peers = [ipaddress.IPv4Address(value) for value in config['peers']]
    if not peers or host not in peers:
        raise ValueError('firewall peers must include this host')
    ports = [config[key] for key in ('kafka_port', 'internal_port', 'controller_port')]
    if any(type(port) is not int or not 1 <= port <= 65535 for port in ports) or len(set(ports)) != 3:
        raise ValueError('invalid firewall ports')
    networks = [ipaddress.ip_network(value, strict=True) for value in config['kafka_sources']]
    # OCI inventory supplies IPv4 addresses; no IPv6 listener is opened here.
    rules = {(str(network), str(host) + '/32', ports[0]) for network in networks if network.version == 4}
    rules.update((str(peer) + '/32', str(host) + '/32', port) for peer in peers for port in ports[1:])
    return profile, sorted(rules)


def command(args):
    result = subprocess.run(['iptables', '--wait', '10', *args], capture_output=True, text=True, timeout=30)
    if result.returncode:
        raise RuntimeError('OCI firewall command failed')
    return result.stdout


def arguments(rule, owner):
    source, destination, port = rule
    return ['-s', source, '-d', destination, '-p', 'tcp', '-m', 'tcp', '--dport', str(port),
            '-m', 'comment', '--comment', owner, '-j', 'ACCEPT']


def owned_rule(tokens, owner):
    # Refuse unexpected rules in our named chain before modifying anything.
    values = {}
    matches = []
    if len(tokens) % 2:
        raise ValueError('invalid owned firewall rule')
    for option, value in zip(tokens[::2], tokens[1::2]):
        if option == '-m':
            matches.append(value)
        elif option in ('-s', '-d', '-p', '--dport', '--comment', '-j') and option not in values:
            values[option] = value
        else:
            raise ValueError('unexpected rule in OCI firewall chain')
    if (set(matches) != {'tcp', 'comment'} or len(matches) != 2 or values.get('-p') != 'tcp'
            or values.get('-j') != 'ACCEPT' or values.get('--comment') != owner
            or set(values) - {'-s'} != {'-d', '-p', '--dport', '--comment', '-j'}):
        raise ValueError('foreign rule in OCI firewall chain')
    return (str(ipaddress.IPv4Network(values.get('-s', '0.0.0.0/0'))), str(ipaddress.IPv4Network(values['-d'])), int(values['--dport']))


def apply(config):
    profile, rules = desired(config)
    chain = 'AUTOMQ_' + hashlib.sha256(profile.encode()).hexdigest()[:16].upper()
    owner = 'automq:' + profile
    listed = [shlex.split(line) for line in command(['-S']).splitlines()]
    exists = ['-N', chain] in listed
    current = []
    jumps = []
    input_rules = [line for line in listed if line[:2] == ['-A', 'INPUT']]
    jump = ['-m', 'comment', '--comment', owner, '-j', chain]
    for line in listed:
        if line[:2] == ['-A', chain]:
            current.append((owned_rule(line[2:], owner), line[2:]))
        if '-j' in line and line[line.index('-j') + 1] == chain:
            if line[:2] != ['-A', 'INPUT'] or line[2:] != jump:
                raise ValueError('foreign reference to OCI firewall chain')
            jumps.append(line)
    changed = False
    if not exists:
        command(['-N', chain])
        changed = True
    # Add missing grants and remove only our obsolete grants. Never flush a chain.
    for rule in rules:
        if rule not in [entry[0] for entry in current]:
            command(['-A', chain, *arguments(rule, owner)])
            changed = True
    seen = set()
    for rule, args in current:
        if rule not in rules or rule in seen:
            command(['-D', chain, *args])
            changed = True
        seen.add(rule)
    if len(jumps) != 1 or not input_rules or input_rules[0] != ['-A', 'INPUT', *jump]:
        for _ in jumps:
            command(['-D', 'INPUT', *jump])
        command(['-I', 'INPUT', '1', *jump])
        changed = True
    return {'changed': changed, 'chain': chain, 'rules': len(rules)}


if __name__ == '__main__':
    try:
        config = json.loads(Path(sys.argv[1]).read_text())
        profile, _ = desired(config)
        with open('/run/lock/automq-firewall-' + profile + '.lock', 'w') as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            print(json.dumps(apply(config), sort_keys=True))
    except (ValueError, KeyError, TypeError, OSError, RuntimeError, subprocess.TimeoutExpired) as error:
        print('OCI firewall refused: ' + str(error), file=sys.stderr)
        sys.exit(1)
