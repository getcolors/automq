"""Application SSH argument formatting; compute owns key lifecycle."""
from pathlib import Path
from colors_compute.ssh import _mode

build_placeholder_dir = '/home/build-placeholder/.ssh'


def rendered_only(opts):
    return opts.get('blue/event') == 'build' or bool(opts.get('blue/dry-run'))


def with_machine_key(opts):
    if _mode(opts)['mode'] != 'managed':
        return opts
    path = build_placeholder_dir + '/' + opts['profile'] if rendered_only(opts) else opts.get('ssh-private-key-path')
    return {**opts, **({'ssh-private-key-path': path, 'ssh-public-key-path': path + '.pub'} if path else {})}


def identity_args(opts):
    path = opts.get('ssh-private-key-path')
    return ['-i', path, '-o', 'IdentitiesOnly=yes'] if path else []


def private_key_path(opts):
    path = opts.get('ssh-private-key-path')
    if not path:
        raise ValueError('deployment SSH identity unavailable')
    return str(Path(path).absolute())
