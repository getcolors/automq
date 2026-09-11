#!/usr/bin/env python3
"""Refuse bucket adoption and purge only OCI buckets recorded in this stage."""
import json
import subprocess
import sys


def run(args, allow_absent_state=False):
    result = subprocess.run(args, capture_output=True, text=True)
    if allow_absent_state and result.returncode == 1 and 'No state file was found!' in result.stderr:
        return ''
    if result.returncode:
        raise RuntimeError('OCI storage command failed: ' + ' '.join(args[:3]))
    return result.stdout


def list_data(args):
    # OCI CLI emits no JSON for an empty successful paginated list.
    raw = run(args)
    if not raw.strip():
        return []
    data = json.loads(raw)['data']
    if not isinstance(data, list):
        raise RuntimeError('OCI listing returned an invalid data collection')
    return data


def operate(action, opts):
    if action not in ('preflight', 'cleanup'):
        raise ValueError('unknown OCI storage operation')
    if action == 'cleanup' and opts.get('compute-prevent-destroy') is not False:
        raise RuntimeError('compute-prevent-destroy refuses storage cleanup')
    run(['tofu', 'init', '-input=false', '-no-color'])
    state = run(['tofu', 'state', 'list'], allow_absent_state=True)
    resources = json.loads(run(['tofu', 'show', '-json'])).get('values', {}).get('root_module', {}).get('resources', []) if state.strip() else []
    base = ['oci', '--profile', opts['oci-config-file-profile'], '--region', opts['automq-r2-region']]
    if opts.get('oci-auth'):
        base += ['--auth', {'SecurityToken': 'security_token', 'APIKey': 'api_key'}.get(opts['oci-auth'], opts['oci-auth'])]
    location = ['--namespace-name', opts['oci-namespace']]
    # OCI's NotAuthorizedOrNotFound cannot prove absence. A complete successful
    # listing does, and an inaccessible compartment must fail before creation.
    listed = None
    for role in ('data', 'ops'):
        name = opts['automq-' + role + '-r2-bucket']
        address = 'oci_objectstorage_bucket.application["' + role + '"]'
        resource = next((r for r in resources if r.get('address') == address), None)
        values = resource.get('values', {}) if resource else {}
        owned = all(values.get(key) == value for key, value in {
            'name': name, 'namespace': opts['oci-namespace'], 'compartment_id': opts['oci-compartment-id']}.items())
        if resource and not owned:
            raise RuntimeError('OCI bucket identity changed from its recorded state')
        if action == 'preflight' and not owned:
            if listed is None:
                listed = list_data(base + ['os', 'bucket', 'list', '--compartment-id', opts['oci-compartment-id'], '--all'] + location)
            if any(bucket['name'] == name for bucket in listed):
                raise RuntimeError('managed storage refuses to adopt an existing OCI bucket')
        if action == 'cleanup' and owned:
            bucket_args = location + ['--bucket-name', name]
            current = json.loads(run(base + ['os', 'bucket', 'get'] + bucket_args))['data']
            tags = current.get('freeform-tags', {})
            # State alone cannot distinguish a foreign same-name replacement.
            # Compare OCI's immutable bucket OCID before any object mutation.
            if (not values.get('bucket_id') or current.get('id') != values['bucket_id']
                    or current.get('compartment-id') != opts['oci-compartment-id']
                    or current.get('namespace') != opts['oci-namespace']
                    or current.get('name') != name
                    or tags.get('colors-profile') != opts['profile']
                    or tags.get('colors-owner') != 'automq-storage'):
                raise RuntimeError('live OCI bucket identity or ownership does not match state')
            uploads = list_data(base + ['os', 'multipart', 'list', '--all'] + bucket_args)
            for upload in uploads:
                run(base + ['os', 'multipart', 'abort', '--object-name', upload['object'], '--upload-id', upload['upload-id'], '--force'] + bucket_args)
            deleted = json.loads(run(base + ['os', 'object', 'bulk-delete', '--force'] + bucket_args))
            if deleted.get('delete-failures'):
                raise RuntimeError('OCI object deletion failed')


if __name__ == '__main__':
    try:
        operate(sys.argv[1], json.loads(sys.argv[2]))
    except Exception as error:
        print(str(error), file=sys.stderr)
        sys.exit(1)
