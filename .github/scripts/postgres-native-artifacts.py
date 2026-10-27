#!/usr/bin/env python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
"""Export/import the two same-run Native binaries; never compile on a consumer."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import stat
import subprocess
import sys
import time
import zipfile

sys.dont_write_bytecode = True
ROOT = Path.cwd().resolve()
OUT = ROOT / 'postgres-native-parallel'
BUNDLE = ROOT / 'postgres-native-artifact'
IMPORTED = OUT / 'import'
SCHEMA = 'stateknot-postgres-native-artifact-v1'
LIMIT = 256 * 1024 * 1024
MAX_FILES = 2048
DIRECT_CHILD_GRACE = 2
PARTIAL_PREFIX_LIMIT = 1024 * 1024
REPO = 'StateKnot/StateKnot'
PROOFS = ('build.jsonl', 'build.stderr', 'build-exit.json', 'metadata.json',
          'toolchain.json', 'inventory.stdout', 'inventory.stderr', 'binary.json')
SCRIPTS = ('.github/scripts/postgres-native-binaries.py',
           '.github/scripts/postgres-native-artifacts.py',
           '.github/scripts/tests/test_postgres_native_artifacts.py',
           '.github/scripts/build-postgres-native.sh',
           '.github/scripts/run-postgres-native-parallel.sh',
           '.github/scripts/postgres-native-leader.sh',
           '.github/postgres-native-artifact-contract.json', '.github/workflows/ci.yml')


def require(ok, message):
    if not ok:
        raise ValueError(message)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def regular(path):
    info = path.lstat()
    require(stat.S_ISREG(info.st_mode) and info.st_nlink == 1, 'regular unlinked file required: ' + str(path))
    require(path.resolve() == path.absolute(), 'symlink ancestor: ' + str(path))
    return path


def sha(path):
    return digest(regular(path).read_bytes())


def unique(pairs):
    result = {}
    for key, value in pairs:
        require(key not in result, 'duplicate JSON field: ' + key)
        result[key] = value
    return result


def decode(raw):
    return json.loads(raw, object_pairs_hook=unique,
                      parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))


def load(path):
    return decode(regular(path).read_bytes())


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('x') as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write('\n')


def copy(raw, path, executable=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    require(path.parent.resolve() == path.parent.absolute(), 'symlink destination ancestor')
    with path.open('xb') as stream:
        stream.write(raw)
    path.chmod(0o755 if executable else 0o644)


def paths(base):
    result = []
    for path in sorted(base.rglob('*')):
        require(not path.is_symlink(), 'artifact symlink')
        if path.is_dir():
            continue
        regular(path)
        result.append(path.relative_to(base).as_posix())
    return result


def index(base):
    return [{'path': name, 'bytes': (base / name).stat().st_size, 'sha256': sha(base / name),
             'executable': bool((base / name).stat().st_mode & 0o111)} for name in paths(base)]


def legacy():
    path = ROOT / '.github/scripts/postgres-native-binaries.py'
    spec = importlib.util.spec_from_file_location('postgres_native_build', regular(path))
    require(spec is not None and spec.loader is not None, 'Native module loader')
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.guards()
    return module


def retire_direct_child(process):
    # This function and command() are the only owners/waiters of this Popen.
    # An unreaped direct child cannot have its PID reused. Never signal a group
    # or infer that its descendants exited from the direct child's wait result.
    actions = []
    for operation in ('terminate', 'kill'):
        if process.returncode is None:
            try:
                getattr(process, operation)()
                actions.append({'operation': operation, 'returned': True})
            except (OSError, KeyboardInterrupt) as failure:
                actions.append({'operation': operation, 'returned': False,
                                'error': type(failure).__name__})
        try:
            code = process.wait(timeout=DIRECT_CHILD_GRACE)
            actions.append({'operation': 'wait', 'returned': True, 'actual_returncode': code})
            return {'direct_child_reaped': True, 'actual_returncode': code, 'actual_wait_returned': True,
                    'actions': actions, 'descendant_state_unknown': True}
        except (subprocess.TimeoutExpired, OSError, KeyboardInterrupt) as failure:
            actions.append({'operation': 'wait', 'returned': False,
                            'error': type(failure).__name__})
    # An interrupted Popen.wait may already have cached a real waitpid result.
    # Record that result without claiming a completed explicit wait or any
    # descendant state; a missing returncode remains unknown.
    return {'direct_child_reaped': process.returncode is not None,
            'actual_returncode': process.returncode, 'actual_wait_returned': False,
            'actions': actions, 'descendant_state_unknown': True}


def api_environment():
    # Keep API credentials local to import_artifact; legacy git subprocesses
    # must inherit the token-free process environment, not only command()'s copy.
    dedicated = dict(os.environ)
    for secret in ('GH_TOKEN', 'GITHUB_TOKEN'):
        os.environ.pop(secret, None)
    return dedicated


def command(directory, label, argv, env=None, timeout=60):
    require(re.fullmatch(r'[a-z0-9-]+', label), 'command label')
    directory.mkdir(parents=True, exist_ok=True)
    # Live descriptors are outside both uploaded trees. Only immutable snapshots
    # are uploaded; an unknown descendant can keep writing its inherited fd.
    live = ROOT / 'postgres-native-live' / directory.relative_to(ROOT)
    live.mkdir(parents=True, exist_ok=True)
    stdout_path, stderr_path = live / (label + '.stdout.live'), live / (label + '.stderr.live')
    start, mono = time.time(), time.monotonic()
    actual_env = dict(os.environ if env is None else env)
    actual_env['LC_ALL'] = 'C'
    if argv[0] != 'gh':
        for secret in ('GH_TOKEN', 'GITHUB_TOKEN'):
            actual_env.pop(secret, None)
    code, process, error, retirement = None, None, None, None
    with stdout_path.open('xb') as stdout, stderr_path.open('xb') as stderr:
        try:
            process = subprocess.Popen(argv, cwd=ROOT, env=actual_env, stdout=stdout, stderr=stderr)
            code = process.wait(timeout=timeout)
        except (subprocess.TimeoutExpired, KeyboardInterrupt) as failure:
            error = {'type': type(failure).__name__, 'descendant_state_unknown': True,
                     'do_not_replay_output_directory': True}
        except OSError as failure:
            error = {'type': type(failure).__name__, 'message': str(failure),
                     'descendant_state_unknown': process is not None}
        if error and process is not None:
            retirement = retire_direct_child(process)
            code = retirement['actual_returncode']
    partial = bool(error and (process is not None or error['descendant_state_unknown']))
    stream_paths = (('stdout', stdout_path), ('stderr', stderr_path))
    snapshots, streams = {}, {}
    for name, path in stream_paths:
        with path.open('rb') as source:
            snapshots[name] = source.read(PARTIAL_PREFIX_LIMIT if partial else LIMIT + 1)
    if not partial and any(len(raw) > LIMIT for raw in snapshots.values()):
        # Never return an output limit violation to a parser as complete data.
        partial = True
        error = {'type': 'OutputLimitExceeded', 'descendant_state_unknown': True,
                 'do_not_replay_output_directory': True}
        snapshots = {name: raw[:PARTIAL_PREFIX_LIMIT] for name, raw in snapshots.items()}
    for name, path in stream_paths:
        raw = snapshots[name]
        suffix = name + ('.partial' if partial else '')
        snapshot = directory / (label + '.' + suffix)
        copy(raw, snapshot)
        streams[name] = {'path': snapshot.relative_to(ROOT).as_posix(), 'bytes': len(raw),
                         'sha256': digest(raw), 'partial': partial,
                         'saved_is_raw_complete': not partial, 'redactions': 0,
                         'live_path_not_uploaded': path.relative_to(ROOT).as_posix(),
                         'live_bytes_observed_after_snapshot': path.stat().st_size,
                         'prefix_limit': PARTIAL_PREFIX_LIMIT if partial else None}
    wall, monotonic = time.time() - start, time.monotonic() - mono
    succeeded = code == 0 and error is None and not partial and 0 <= wall <= timeout + 5 and abs(wall - monotonic) < 5
    save(directory / (label + '.receipt.json'), {'argv': argv, 'pid': process.pid if process else None,
         'exit_code': code, 'started_unix': start, 'seconds': wall,
         'monotonic_seconds': monotonic, 'execution_error': error,
         'timed_out': bool(error and error['type'] == 'TimeoutExpired'),
         'interrupted': bool(error and error['type'] == 'KeyboardInterrupt'),
         'direct_child_reaped': bool(process is not None and process.returncode is not None),
         'direct_child_retirement': retirement, 'descendants_empty_proved': False,
         'descendant_state_unknown': bool(error and error['descendant_state_unknown']),
         'stdout_bytes': len(snapshots['stdout']), 'stdout_sha256': digest(snapshots['stdout']),
         'stderr_bytes': len(snapshots['stderr']), 'stderr_sha256': digest(snapshots['stderr']),
         'output_streams': streams, 'stream_snapshots_stable': True,
         'partial_unqualified': partial, 'command_succeeded': succeeded})
    if not succeeded:
        save(directory / (label + '.unsealed.json'), {'error': error, 'direct_child_retirement': retirement,
             'partial_unqualified': partial, 'live_streams_not_uploaded': True,
             'qualification_awarded': False})
    require(succeeded, 'actual command failed/clock unstable: ' + label)
    return snapshots['stdout']

def identity(directory):
    require(os.environ.get('GITHUB_REPOSITORY') == REPO, 'fixed repository')
    for key in ('GITHUB_RUN_ID', 'GITHUB_RUN_ATTEMPT'):
        require(re.fullmatch(r'[1-9][0-9]*', os.environ.get(key, '')), 'strict integer ' + key)
    event_name = os.environ.get('GITHUB_EVENT_NAME')
    require(event_name in ('pull_request', 'push', 'workflow_dispatch'), 'supported CI event')
    event = load(Path(os.environ['GITHUB_EVENT_PATH']))
    require(event['repository']['full_name'] == REPO, 'event repository')
    actual = command(directory, 'checkout-head', ['git', 'rev-parse', 'HEAD']).decode().strip()
    require(re.fullmatch(r'[0-9a-f]{40}', actual) and actual == os.environ['GITHUB_SHA'], 'actual checkout SHA')
    command(directory, 'tracked-clean', ['git', 'diff', '--exit-code', 'HEAD', '--'])
    untracked = command(directory, 'untracked-source', ['git', 'ls-files', '--others', '--exclude-standard', '--',
                        'Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'crates', '.github'])
    require(not untracked, 'untracked source cannot enter producer/consumer')
    trigger = event['pull_request']['head']['sha'] if event_name == 'pull_request' else (
        event['after'] if event_name == 'push' else actual)
    require(re.fullmatch(r'[0-9a-f]{40}', trigger), 'trigger head SHA')
    return {'repository': REPO, 'run_id': int(os.environ['GITHUB_RUN_ID']),
            'run_attempt': int(os.environ['GITHUB_RUN_ATTEMPT']), 'event': event_name,
            'trigger_head': trigger, 'checkout_sha': actual, 'workspace': str(ROOT)}


def host(directory):
    require(sys.platform == 'linux' and platform.machine() == 'x86_64', 'Linux x86_64 ABI only')
    for key in ('ImageOS', 'ImageVersion'):
        require(os.environ.get(key), 'actual runner image metadata required: ' + key)
    cargo = command(directory, 'cargo-path', ['rustup', 'which', '--toolchain', '1.88.0', 'cargo']).decode().strip()
    rustc = command(directory, 'rustc-path', ['rustup', 'which', '--toolchain', '1.88.0', 'rustc']).decode().strip()
    verbose = command(directory, 'rustc-verbose', [rustc, '-vV']).decode()
    readelf = command(directory, 'readelf-version', ['readelf', '--version']).decode()
    loader = command(directory, 'ldd-version', ['ldd', '--version']).decode()
    require(verbose.startswith('rustc 1.88.0 ') and 'host: x86_64-unknown-linux-gnu\n' in verbose, 'pinned actual compiler')
    return {'ImageOS': os.environ['ImageOS'], 'ImageVersion': os.environ['ImageVersion'],
            'os_release': regular(Path('/etc/os-release').resolve()).read_text(),
            'machine': platform.machine(), 'cargo': cargo, 'rustc': rustc, 'rustc_verbose': verbose,
            'readelf_version': readelf, 'ldd_version': loader}


def script_binding():
    return [{'path': path, 'sha256': sha(ROOT / path)} for path in SCRIPTS]


def elf_info(directory, label, path, env):
    header = command(directory, label + '-header', ['readelf', '--wide', '--file-header', str(path)], env).decode()
    dynamic = command(directory, label + '-dynamic', ['readelf', '--wide', '--dynamic', str(path)], env).decode()
    versions = command(directory, label + '-versions', ['readelf', '--wide', '--version-info', str(path)], env).decode()
    program = command(directory, label + '-program', ['readelf', '--wide', '--program-headers', str(path)], env).decode()
    require(re.search(r'Class:\s+ELF64\b', header) and re.search(r'Data:\s+2.s complement, little endian', header)
            and re.search(r'Machine:\s+Advanced Micro Devices X86-64', header), 'ELF ABI header')
    require(not re.search(r'\((?:RPATH|RUNPATH|AUDIT|DEPAUDIT|FILTER|AUXILIARY)\)', dynamic), 'ELF search/injection tags unsupported')
    needed = re.findall(r'\(NEEDED\).*?Shared library: \[([^\]]+)\]', dynamic)
    sonames = re.findall(r'\(SONAME\).*?Library soname: \[([^\]]+)\]', dynamic)
    interpreters = re.findall(r'Requesting program interpreter: ([^\]]+)\]', program)
    require(len(sonames) <= 1 and len(interpreters) <= 1
            and all(re.fullmatch(r'[A-Za-z0-9_.+-]+', value) for value in needed + sonames), 'ELF SONAME layout')
    needs, definitions, mode, current = {}, [], None, None
    for line in versions.splitlines():
        if line.startswith('Version needs section '):
            mode = 'needs'
        elif line.startswith('Version definition section '):
            mode = 'definitions'
        elif line.startswith('Version symbols section '):
            mode = None
        if mode == 'needs':
            found = re.search(r'\bFile: (\S+)\s+Cnt:', line)
            if found:
                current = found[1]
                require(current not in needs, 'duplicate ELF version file')
                needs[current] = []
            found = re.search(r'\bName: (\S+)\s+Flags:', line)
            if found:
                require(current is not None, 'ELF version name without file')
                needs[current].append(found[1])
        elif mode == 'definitions':
            found = re.search(r'\bName: (\S+)', line)
            if found:
                definitions.append(found[1])
    return {'needed': needed, 'soname': sonames[0] if sonames else None,
            'interpreter': interpreters[0] if interpreters else None,
            'version_needs': needs, 'version_definitions': sorted(set(definitions))}


def library_kind(path, receipt):
    resolved = path.resolve(strict=True)
    regular(resolved)
    if path.is_relative_to(ROOT / 'target/debug'):
        require(path.absolute() == resolved, 'project runtime DSO symlink unsupported')
        return 'project'
    sysroot = Path(receipt['toolchain']['sysroot'])
    if resolved.is_relative_to(sysroot / 'lib'):
        return 'toolchain'
    require(resolved.is_relative_to(Path('/usr/lib/x86_64-linux-gnu')),
            'runtime DSO outside the fixed Ubuntu GNU library closure: ' + str(path))
    return 'system'


def abi(directory, role, receipt, module):
    env = module.runtime_env(role, receipt)
    raw = command(directory, role + '-ldd', ['ldd', '-r', receipt['binary']], env).decode()
    require(not re.search(r'not found|undefined symbol|version .* not found', raw), 'unresolved runtime ABI')
    resolved = {}
    for line in raw.splitlines():
        line = line.strip()
        if re.fullmatch(r'linux-vdso\.so\.1 \(0x[0-9a-f]+\)', line):
            continue
        found = re.fullmatch(r'(\S+) => (/\S+) \(0x[0-9a-f]+\)', line)
        if found:
            name, path = found.groups()
        else:
            found = re.fullmatch(r'(/\S+) \(0x[0-9a-f]+\)', line)
            require(found is not None, 'unexpected ldd diagnostic: ' + line)
            path = found[1]
            name = Path(path).name
        require(name not in resolved, 'duplicate loader SONAME')
        resolved[name] = path
    require(0 < len(resolved) <= 32, 'bounded actual runtime dependency closure')
    objects = [{'name': '<binary>', 'path': receipt['binary'], 'kind': 'binary'}]
    for name, path in sorted(resolved.items()):
        objects.append({'name': name, 'path': path, 'kind': library_kind(Path(path), receipt)})
    definitions = {}
    for position, obj in enumerate(objects):
        path = Path(obj['path'])
        real = path.resolve(strict=True)
        obj.update(real_path=str(real), bytes=real.stat().st_size, sha256=sha(real),
                   elf=elf_info(directory, role + '-elf-' + str(position), path, env))
        if position:
            require(obj['elf']['soname'] in (None, obj['name']), 'loader SONAME differs from actual object')
            definitions[obj['name']] = set(obj['elf']['version_definitions'])
    root = objects[0]['elf']
    require(root['interpreter'] is not None and root['interpreter'] in resolved.values(), 'actual ELF interpreter closure')
    for obj in objects:
        require(all(name in resolved for name in obj['elf']['needed']), 'missing transitive DT_NEEDED')
        for name, versions in obj['elf']['version_needs'].items():
            require(name in definitions and set(versions) <= definitions[name], 'ELF required symbol versions missing: ' + name)
    return objects


def validate_role(role, directory, module):
    original = module.OUT
    try:
        module.OUT = directory
        receipt = module.select_artifact(role)
    finally:
        module.OUT = original
    saved = load(directory / (role + '-binary.json'))
    source = module.source_binding()
    require(saved == dict(receipt, source=source, validated_no_run_build_exit_code=0,
            build_stdout_sha256=sha(directory / (role + '-build.jsonl')),
            build_stderr_sha256=sha(directory / (role + '-build.stderr')),
            inventory_count=module.ROLES[role][1],
            inventory_stdout_sha256=sha(directory / (role + '-inventory.stdout')),
            inventory_stderr_sha256=sha(directory / (role + '-inventory.stderr'))), 'complete original producer receipt')
    exit_row = load(directory / (role + '-build-exit.json'))
    require(exit_row['build_process_exit_code'] == 0, 'actual producer Cargo success')
    return saved


def inventory(directory, role, receipt, module):
    before = sha(Path(receipt['binary']))
    stdout = command(directory, role + '-list', [receipt['binary'], '--list'], module.runtime_env(role, receipt))
    expected = regular(directory.parent / 'producer' / (role + '-inventory.stdout')).read_bytes()
    require(stdout == expected and sha(Path(receipt['binary'])) == before, 'consumer full inventory differs')
    lines = stdout.decode().splitlines()
    names = [line[:-6] for line in lines if line.endswith(': test')]
    count = module.ROLES[role][1]
    require(len(names) == len(set(names)) == count and len(lines) == count + 2
            and lines[-1] == str(count) + ' tests, 0 benchmarks', 'full 276/231 Native inventory')


def validate_ci_contract(contract, workflow_sha256):
    """Accept only the current three-lane CI policy; artifact-v1 stays unchanged."""
    require(type(contract) is dict
            and contract.get('schema') == 'stateknot-postgres-native-shared-build-ci-contract-v2',
            'explicit current runtime-shards CI contract schema')
    require(contract['expected_jobs'] == 22 and contract['expected_checkouts'] == 20
            and contract['current_candidate_workflow_sha256'] == workflow_sha256
            and contract['full_inventory'] == {'store': 276, 'runtime': 231}
            and 'native_pair_owner_unchanged' not in contract,
            'explicit current runtime-shards CI contract')
    execution = contract['native_execution']
    require(execution['schema'] == 'stateknot-native-runtime-shards-execution-v1'
            and execution['plan_schema'] == 'stateknot-native-runtime-shards-plan-v1'
            and execution['producer_roles'] == ['store', 'runtime']
            and execution['qualification_awarded'] is False, 'current execution schema and original two producer roles')
    require(execution['lanes'] == [
            {'label': 'store', 'role': 'store', 'database': 'stateknot_store_native',
             'owner_fd': 7, 'shard_index': None},
            {'label': 'runtime-0', 'role': 'runtime', 'database': 'stateknot_runtime_native_0',
             'owner_fd': 8, 'shard_index': 0},
            {'label': 'runtime-1', 'role': 'runtime', 'database': 'stateknot_runtime_native_1',
             'owner_fd': 9, 'shard_index': 1}], 'exact three lanes and independent database routing')
    assignment = execution['runtime_assignment']
    require(assignment['inventory_source'] == 'full_inventory.runtime'
            and assignment['assignment_id'] == 'sorted-full-inventory-alternating-v1'
            and assignment['shard_count'] == 2 and assignment['test_threads'] == 1
            and assignment['passed_count'] == 'len(sorted(controlled full_inventory.runtime names)[shard_index::shard_count])'
            and assignment['filtered_count'] == 'full_inventory.runtime - passed_count'
            and assignment['union_equals_complete_inventory'] is True
            and assignment['selected_names_once'] is True and assignment['same_runtime_binary'] is True
            and assignment['same_source_binding_and_profile'] is True
            and assignment['store_passed_count'] == 'full_inventory.store'
            and assignment['store_filtered_count'] == assignment['required_failed_ignored_measured'] == 0,
            'Runtime subset counts derive from one complete controlled inventory')
    owner = execution['owner_protocol']
    require(owner['leader_sessions'] == 3
            and owner['original_two_lane_arrangement_unchanged'] is False
            and owner['states'] == ['unstarted', 'active', 'lost', 'retired']
            and owner['identity_and_retirement_protocol_preserved'] is True
            and owner['leader_lane_allowlist_and_fd_closure_extended'] is True
            and owner['successful_cleanup_requires_all_three_retired'] is True
            and contract['native_owner_protocol']['execution_schema'] == execution['schema']
            and contract['native_owner_protocol']['lane_count'] == 3
            and contract['native_owner_protocol']['legacy_two_lane_arrangement_preserved'] is False,
            'explicit three-lane ownership and successful retirement policy')
    layout, legacy = execution['artifact_layout'], execution['legacy_single_stream_policy']
    require(layout['schema'] == 'stateknot-native-runtime-shards-artifact-layout-v1'
            and layout['artifacts_per_native_job'] == 1 and layout['total_workflow_artifacts'] == 56
            and layout['producer_artifacts'] == 2 and layout['synthetic_runtime_streams_allowed'] is False
            and layout['overlap_keys'] == ['schema', 'observed_unix_ns', 'all_actual_binary_processes_alive',
                                         'processes', 'databases', 'identities']
            and layout['overlap_identity_keys'] == ['pid', 'start_ticks', 'ppid', 'pgrp', 'session', 'exe']
            and legacy['schema'] == 'stateknot-native-single-stream-execution-v1'
            and legacy['current_sharded_evidence_may_be_interpreted_as_legacy'] is False
            and legacy['sealed_v1_policy_and_failures_immutable'] is True,
            'separate current raw lane streams and immutable historical policy')
    current = {row['source_index']: row for row in contract['current_60_step_mapping']}
    require(current[4]['execution_schema'] == execution['schema']
            and current[5]['artifact_layout_schema'] == layout['schema'], 'current execution and upload mapping')


def export():
    require(os.environ.get('GITHUB_JOB') == 'postgres-native-build', 'single producer job')
    require(not BUNDLE.exists(), 'export cannot replay')
    BUNDLE.mkdir()
    module = legacy()
    identity_row = identity(BUNDLE / 'checks')
    host_row = host(BUNDLE / 'checks')
    require((OUT / 'cold-target.txt').read_text() in ('absent\n', 'empty\n'), 'actual cold producer target')
    for name in ('cold-target.txt', 'build-before.df'):
        copy(regular(OUT / name).read_bytes(), BUNDLE / 'producer' / name)
    source = module.source_binding()
    roles, runtime_files, native_search_dirs = {}, {}, set()
    for role in module.ROLES:
        receipt = validate_role(role, OUT, module)
        require(receipt['toolchain']['cargo'] == host_row['cargo']
                and receipt['toolchain']['rustc'] == host_row['rustc']
                and receipt['toolchain']['rustc_verbose'] + '\n' == host_row['rustc_verbose'], 'actual producer toolchain')
        for directory in receipt['dynamic_library_prefix']:
            path = Path(directory)
            if path.is_relative_to(ROOT / 'target/debug/build'):
                relative = path.relative_to(ROOT).as_posix()
                require(re.fullmatch(r'target/debug/build/[A-Za-z0-9_-]+/out', relative), 'fixed Cargo native search directory')
                native_search_dirs.add(relative)
        observed = abi(BUNDLE / 'checks', role, receipt, module)
        roles[role] = {'binary_sha256': receipt['binary_sha256'], 'binary_bytes': receipt['binary_bytes'],
                       'inventory': module.ROLES[role][1], 'runtime': observed}
        for obj in observed:
            if obj['kind'] in ('binary', 'project'):
                relative = Path(obj['path']).relative_to(ROOT).as_posix()
                require(relative.startswith('target/debug/'), 'only selected project runtime files')
                runtime_files[relative] = {'sha256': obj['sha256'], 'bytes': obj['bytes'],
                                           'executable': obj['kind'] == 'binary'}
        for suffix in PROOFS:
            copy(regular(OUT / (role + '-' + suffix)).read_bytes(), BUNDLE / 'producer' / (role + '-' + suffix))
    for name, row in sorted(runtime_files.items()):
        copy(regular(ROOT / name).read_bytes(), BUNDLE / 'payload' / name, row['executable'])
        require(sha(BUNDLE / 'payload' / name) == row['sha256'], 'runtime payload changed')
    require(source == module.source_binding(), 'source changed during export')
    for role, data in roles.items():
        receipt = load(OUT / (role + '-binary.json'))
        require(abi(BUNDLE / 'after', role, receipt, module) == data['runtime'], 'producer DSO/binary changed during export')
    contract = load(ROOT / '.github/postgres-native-artifact-contract.json')
    validate_ci_contract(contract, sha(ROOT / '.github/workflows/ci.yml'))
    save(BUNDLE / 'manifest.json', {'schema': SCHEMA, 'identity': identity_row, 'host': host_row,
         'profile': module.PROFILE, 'scripts': script_binding(), 'source': source,
         'producer_cold_target': (OUT / 'cold-target.txt').read_text().strip(),
         'roles': roles, 'runtime_files': runtime_files, 'native_search_dirs': sorted(native_search_dirs),
         'files': index(BUNDLE),
         'producer_job': 'postgres-native-build', 'consumer_local_Cargo_build': False})
    manifest_sha256 = sha(BUNDLE / 'manifest.json')
    with Path(os.environ['GITHUB_OUTPUT']).open('a') as output:
        output.write('manifest_sha256=' + manifest_sha256 + '\n')
    exported = {'schema': 'stateknot-postgres-native-export-v1', 'identity': identity_row,
                'producer_job': 'postgres-native-build', 'source_manifest_sha256': source['manifest_sha256'],
                'source_count': len(source['files']), 'bundle_manifest_sha256': manifest_sha256}
    # Keep this outside the sealed bundle: no manifest/receipt hash self-cycle.
    save(OUT / 'export.receipt.json', exported)
    print('STATEKNOT_NATIVE_EXPORT_V1=' + json.dumps(exported, sort_keys=True, separators=(',', ':')), flush=True)


def extract(archive, destination):
    with zipfile.ZipFile(archive) as bundle:
        names, total = set(), 0
        require(len(bundle.infolist()) <= MAX_FILES * 2, 'bounded artifact entry count')
        for info in bundle.infolist():
            name = info.filename
            path = PurePosixPath(name)
            require(name and re.fullmatch(r'[A-Za-z0-9_./+-]+', name) and not path.is_absolute()
                    and all(part not in ('', '.', '..') for part in name.rstrip('/').split('/'))
                    and str(path) == name.rstrip('/'), 'unsafe artifact path')
            require(name.rstrip('/') not in names, 'duplicate artifact path')
            names.add(name.rstrip('/'))
            kind = stat.S_IFMT(info.external_attr >> 16)
            require(kind in (0, stat.S_IFREG, stat.S_IFDIR) and not info.flag_bits & 1, 'unsupported artifact member')
            total += info.file_size
            require(total <= LIMIT and info.file_size <= LIMIT, 'bounded expanded artifact')
            if info.is_dir():
                (destination / path).mkdir(parents=True, exist_ok=True)
            else:
                copy(bundle.read(info), destination / path)


def verify_files(extracted, manifest):
    expected = manifest['files']
    require(len(expected) <= MAX_FILES and paths(extracted) == sorted([row['path'] for row in expected] + ['manifest.json']), 'complete artifact closure')
    for row in expected:
        path = extracted / row['path']
        require(path.stat().st_size == row['bytes'] and sha(path) == row['sha256']
                and type(row['executable']) is bool, 'artifact indexed bytes')
    return expected


def import_artifact(args):
    gh_env = api_environment()
    require(os.environ.get('GITHUB_JOB') == 'postgres-native', 'only original Native consumer jobs')
    require(not IMPORTED.exists() and not OUT.exists(), 'import cannot replay')
    require(not (ROOT / 'target').exists() or ((ROOT / 'target').is_dir()
            and not (ROOT / 'target').is_symlink() and not any((ROOT / 'target').iterdir())), 'fresh consumer target before import')
    require(re.fullmatch(r'[1-9][0-9]*', args.artifact_id), 'artifact integer ID')
    match = re.fullmatch(r'(?:sha256:)?([0-9a-f]{64})', args.artifact_digest)
    require(match is not None and re.fullmatch(r'[0-9a-f]{64}', args.manifest_sha256), 'explicit producer hashes')
    OUT.mkdir()
    IMPORTED.mkdir()
    module = legacy()
    ident = identity(IMPORTED / 'identity')
    observed_host = host(IMPORTED / 'host')
    endpoint = 'repos/' + REPO + '/actions/artifacts/' + args.artifact_id
    metadata = decode(command(IMPORTED, 'artifact-api', ['gh', 'api', '--hostname', 'github.com', '--method', 'GET', endpoint], env=gh_env))
    expected_name = 'postgres-native-build-' + str(ident['run_id']) + '-' + str(ident['run_attempt'])
    require(metadata['id'] == int(args.artifact_id) and metadata['name'] == expected_name
            and metadata['expired'] is False and type(metadata['size_in_bytes']) is int
            and 0 < metadata['size_in_bytes'] <= LIMIT and metadata['digest'] == 'sha256:' + match[1]
            and metadata['workflow_run']['id'] == ident['run_id']
            and metadata['workflow_run']['head_sha'] == ident['trigger_head'], 'same-run/trigger-head official artifact')
    archive = command(IMPORTED, 'artifact-zip', ['gh', 'api', '--hostname', 'github.com', '--method', 'GET', endpoint + '/zip'], env=gh_env)
    require(len(archive) == metadata['size_in_bytes'] and digest(archive) == match[1], 'API/action/zip digest and size agree')
    extracted = IMPORTED / 'artifact'
    extracted.mkdir()
    extract(IMPORTED / 'artifact-zip.stdout', extracted)
    require(sha(extracted / 'manifest.json') == args.manifest_sha256, 'producer manifest job output')
    manifest = load(extracted / 'manifest.json')
    require(manifest['schema'] == SCHEMA and manifest['identity'] == ident
            and manifest['producer_job'] == 'postgres-native-build' and manifest['consumer_local_Cargo_build'] is False
            and manifest['profile'] == module.PROFILE and manifest['scripts'] == script_binding()
            and manifest['source'] == module.source_binding() and set(manifest['roles']) == set(module.ROLES),
            'same actual checkout/source/scripts/profile producer binding')
    expected = verify_files(extracted, manifest)
    require(manifest['producer_cold_target'] in ('absent', 'empty')
            and regular(extracted / 'producer/cold-target.txt').read_text() == manifest['producer_cold_target'] + '\n',
            'preserved actual cold producer target')
    runtime = manifest['runtime_files']
    require(2 <= len(runtime) <= 64, 'bounded selected runtime file set')
    selected = {str(Path(obj['path']).relative_to(ROOT)) for data in manifest['roles'].values()
                for obj in data['runtime'] if obj['kind'] in ('binary', 'project')}
    require(set(runtime) == selected, 'only actual binary/DSO closure, never unrelated target cache')
    require({row['path'] for row in expected if row['path'].startswith('payload/')} == {'payload/' + name for name in runtime}, 'no unrelated target cache in artifact')
    for name, row in runtime.items():
        path = PurePosixPath(name)
        require(str(path) == name and name.startswith('target/debug/') and '..' not in path.parts
                and not path.is_absolute() and type(row['executable']) is bool, 'selected relative runtime path')
        raw = regular(extracted / 'payload' / name).read_bytes()
        require(len(raw) == row['bytes'] and digest(raw) == row['sha256'], 'runtime payload index')
        copy(raw, ROOT / name, row['executable'])
    native_dirs = manifest['native_search_dirs']
    require(len(native_dirs) == len(set(native_dirs)) <= 32, 'bounded Cargo runtime search directories')
    for directory in native_dirs:
        require(re.fullmatch(r'target/debug/build/[A-Za-z0-9_-]+/out', directory), 'only fixed native search directory')
        (ROOT / directory).mkdir(parents=True, exist_ok=True)
    producer = extracted / 'producer'
    snapshots = {}
    for role in module.ROLES:
        receipt = validate_role(role, producer, module)
        require(receipt['toolchain']['cargo'] == observed_host['cargo']
                and receipt['toolchain']['rustc'] == observed_host['rustc']
                and receipt['toolchain']['rustc_verbose'] + '\n' == observed_host['rustc_verbose'], 'same pinned consumer compiler paths/version')
        data = manifest['roles'][role]
        require(data['binary_sha256'] == receipt['binary_sha256'] and data['binary_bytes'] == receipt['binary_bytes']
                and data['inventory'] == module.ROLES[role][1], 'producer selected binary/inventory')
        snapshots[role] = abi(IMPORTED / 'admission', role, receipt, module)
        producer_objects = {obj['path']: obj for obj in data['runtime']}
        for obj in snapshots[role]:
            if obj['kind'] in ('binary', 'project', 'toolchain'):
                require(obj['path'] in producer_objects and obj['sha256'] == producer_objects[obj['path']]['sha256']
                        and obj['elf'] == producer_objects[obj['path']]['elf'], 'transported/pinned-toolchain ELF differs')
        copy(regular(producer / (role + '-binary.json')).read_bytes(), OUT / (role + '-binary.json'))
        # inventory() reads the preserved original producer output, never invents a build.
        directory = IMPORTED / 'consumer'
        (IMPORTED / 'producer').mkdir(exist_ok=True)
        copy(regular(producer / (role + '-inventory.stdout')).read_bytes(), IMPORTED / 'producer' / (role + '-inventory.stdout'))
        inventory(directory, role, receipt, module)
    require(manifest['source'] == module.source_binding(), 'source changed during import')
    save(IMPORTED / 'import.json', {'schema': SCHEMA + '-import', 'identity': ident,
         'artifact_id': int(args.artifact_id), 'artifact_zip_sha256': match[1],
         'manifest_sha256': args.manifest_sha256, 'producer_host': manifest['host'],
         'consumer_host': observed_host, 'consumer_initial_target': 'absent-or-empty',
         'producer_receipts_preserved': True, 'consumer_local_Cargo_build': False,
         'consumer_admission_runtime': snapshots, 'source': module.source_binding(),
         'role_receipt_sha256': {role: sha(OUT / (role + '-binary.json')) for role in module.ROLES}})
    copy(b'imported-same-run\n', OUT / 'cold-target.txt')


def verify(phase):
    require(phase in ('before', 'after'), 'fixed verification phase')
    module = legacy()
    receipt = load(IMPORTED / 'import.json')
    artifact = IMPORTED / 'artifact'
    require(sha(artifact / 'manifest.json') == receipt['manifest_sha256']
            and receipt['consumer_local_Cargo_build'] is False and receipt['source'] == module.source_binding(), 'import still bound')
    manifest = load(artifact / 'manifest.json')
    verify_files(artifact, manifest)
    require(manifest['scripts'] == script_binding(), 'Native scripts/workflow changed across pair')
    for directory in manifest['native_search_dirs']:
        path = ROOT / directory
        require(path.is_dir() and path.resolve() == path, 'native runtime search directory changed')
    current = {}
    for role in module.ROLES:
        require(sha(OUT / (role + '-binary.json')) == receipt['role_receipt_sha256'][role], 'original producer receipt changed')
        binary = module.load_bound(role)
        current[role] = abi(IMPORTED / phase, role, binary, module)
    require(current == receipt['consumer_admission_runtime'], 'consumer binary/DSO closure changed across pair')
    save(IMPORTED / (phase + '.json'), {'schema': SCHEMA + '-verification',
         'phase': phase, 'actual_runtime_closure': current, 'all_source_binary_DSO_unchanged': True,
         'producer_manifest_sha256': receipt['manifest_sha256'], 'consumer_local_Cargo_build': False})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='mode', required=True)
    sub.add_parser('export')
    imported = sub.add_parser('import')
    imported.add_argument('--artifact-id', required=True)
    imported.add_argument('--artifact-digest', required=True)
    imported.add_argument('--manifest-sha256', required=True)
    verified = sub.add_parser('verify')
    verified.add_argument('phase', choices=('before', 'after'))
    args = parser.parse_args()
    if args.mode == 'export':
        export()
    elif args.mode == 'import':
        import_artifact(args)
    else:
        verify(args.phase)


if __name__ == '__main__':
    main()
