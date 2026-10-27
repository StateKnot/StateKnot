#!/usr/bin/env python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
"""Build and execute only this repository's two default PostgreSQL test targets."""
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

ROOT = Path.cwd().resolve()
OUT = ROOT / 'postgres-native-parallel'
ROLES = {'store': ('stateknot-store-postgres', 276), 'runtime': ('stateknot-runtime', 231)}
PROFILE = {'CARGO_INCREMENTAL': '0', 'CARGO_PROFILE_DEV_DEBUG': '0',
           'CARGO_PROFILE_TEST_DEBUG': '0', 'CARGO_PROFILE_TEST_OPT_LEVEL': '1',
           'CARGO_PROFILE_TEST_DEBUG_ASSERTIONS': 'true', 'CARGO_PROFILE_TEST_OVERFLOW_CHECKS': 'true'}
TOOLCHAIN_ENV = {'PATH', 'LD_LIBRARY_PATH', 'CARGO_HOME', 'RUSTUP_HOME', 'RUSTUP_TOOLCHAIN',
                 'RUSTUP_RECURSION_COUNT', 'RUST_RECURSION_COUNT'}


def require(ok, message):
    if not ok:
        raise ValueError(message)


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def unique(pairs):
    row = {}
    for key, value in pairs:
        require(key not in row, 'duplicate JSON field: ' + key)
        row[key] = value
    return row


def reject(value):
    raise ValueError('nonstandard JSON constant: ' + value)


def decode(raw):
    return json.loads(raw, object_pairs_hook=unique, parse_constant=reject)


def save(path, data):
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + '\n')


def guards():
    require(sys.platform == 'linux', 'native binary execution requires Linux')
    require(all(os.environ.get(k) == v for k, v in PROFILE.items()), 'six-variable test profile differs')
    require(os.environ.get('STATEKNOT_REQUIRE_POSTGRES_TESTS') == '1', 'mandatory PostgreSQL mode absent')
    for key, value in os.environ.items():
        if key.startswith(('CARGO_TARGET_', 'CARGO_BUILD_', 'CARGO_PROFILE_')):
            require(key in PROFILE, 'unsupported Cargo configuration environment: ' + key)
    for key in ('RUSTFLAGS', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTDOCFLAGS', 'RUST_MIN_STACK', 'RUSTC',
                'RUSTC_BOOTSTRAP', 'RUSTC_WRAPPER', 'RUSTC_WORKSPACE_WRAPPER', 'CARGO_TARGET_DIR',
                'CARGO_MAKEFLAGS', 'MAKEFLAGS'):
        require(not os.environ.get(key), 'unsupported build/runtime override: ' + key)
    cargo_home = Path(os.environ.get('CARGO_HOME', str(Path.home() / '.cargo')))
    configs = [cargo_home / name for name in ('config', 'config.toml')]
    configs += [parent / '.cargo' / name for parent in (ROOT, *ROOT.parents) for name in ('config', 'config.toml')]
    require(not any(path.exists() or path.is_symlink() for path in configs), 'Cargo configuration files unsupported')
    require(not any(path.is_symlink() for path in (ROOT / 'target', ROOT / 'target/debug', ROOT / 'target/debug/deps')),
            'symlink target layout unsupported')


def source_binding():
    paths = subprocess.check_output(['git', 'ls-files', '-z', '--', 'Cargo.toml', 'Cargo.lock',
                                    'rust-toolchain.toml', 'crates']).split(b'\0')
    rows = []
    for encoded in paths:
        if encoded:
            relative = os.fsdecode(encoded)
            path = ROOT / relative
            require(path.is_file() and not path.is_symlink(), 'source input is not a regular file: ' + relative)
            rows.append({'path': relative, 'bytes': path.stat().st_size, 'sha256': sha(path)})
    require(len(rows) > 100 and any(row['path'] == 'Cargo.lock' for row in rows), 'source inventory incomplete')
    return {'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
            'files': rows, 'manifest_sha256': hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()}


def bridge(role, cargo, rustc):
    # The baseline travels only through an anonymous pipe. Persist only the allowed
    # rustup delta, never unrelated environment values such as credentials or DB URLs.
    baseline = decode(sys.stdin.buffer.read())
    actual = dict(os.environ)
    delta = {key: actual.get(key) for key in baseline.keys() | actual.keys() if baseline.get(key) != actual.get(key)}
    require(set(delta) <= TOOLCHAIN_ENV, 'unexpected rustup environment change: ' + ','.join(sorted(set(delta) - TOOLCHAIN_ENV)))
    require(actual.get('RUSTUP_TOOLCHAIN', '').startswith('1.88.0-'), 'rustup did not select 1.88.0')
    require(cargo == str(Path(cargo).resolve()) and rustc == str(Path(rustc).resolve()), 'toolchain executable symlink unsupported')
    def query(argv):
        return subprocess.check_output(argv, text=True).strip()
    version, verbose = query([cargo, '-V']), query([rustc, '-vV'])
    require(version.startswith('cargo 1.88.0 ') and verbose.startswith('rustc 1.88.0 '), 'pinned compiler/Cargo version differs')
    sysroot = Path(query([rustc, '--print', 'sysroot']))
    target_libdir = Path(query([rustc, '--print', 'target-libdir']))
    require(target_libdir.is_relative_to(sysroot) and target_libdir.is_dir(), 'sysroot target library layout differs')
    metadata = subprocess.check_output([cargo, 'metadata', '--no-deps', '--format-version=1', '--locked'])
    (OUT / (role + '-metadata.json')).write_bytes(metadata)
    save(OUT / (role + '-toolchain.json'), {'delta': delta, 'cargo': cargo, 'rustc': rustc,
         'cargo_version': version, 'rustc_verbose': verbose, 'sysroot': str(sysroot), 'target_libdir': str(target_libdir)})
    package = ROLES[role][0]
    argv = [cargo, 'test', '-p', package, '--test', 'postgres', '--locked', '--no-run', '--message-format=json']
    os.execve(cargo, argv, actual)


def runtime_env(role, receipt):
    env = dict(os.environ)
    for key, value in receipt['toolchain']['delta'].items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    package = receipt['package']
    version = re.fullmatch(r'(\d+)\.(\d+)\.(\d+)(?:-([0-9A-Za-z.-]+))?', package['version'])
    require(version is not None, 'unsupported package version')
    env.update({'CARGO': receipt['toolchain']['cargo'], 'CARGO_MANIFEST_DIR': str(Path(package['manifest_path']).parent),
                'CARGO_MANIFEST_PATH': package['manifest_path'], 'CARGO_PKG_NAME': package['name'],
                'CARGO_PKG_VERSION': package['version'], 'CARGO_PKG_VERSION_MAJOR': version[1],
                'CARGO_PKG_VERSION_MINOR': version[2], 'CARGO_PKG_VERSION_PATCH': version[3],
                'CARGO_PKG_VERSION_PRE': version[4] or ''})
    for key, field in {'DESCRIPTION': 'description', 'HOMEPAGE': 'homepage', 'REPOSITORY': 'repository',
                       'LICENSE': 'license', 'LICENSE_FILE': 'license_file', 'RUST_VERSION': 'rust_version',
                       'README': 'readme'}.items():
        env['CARGO_PKG_' + key] = package[field] or ''
    env['CARGO_PKG_AUTHORS'] = ':'.join(package['authors'])
    # Cargo 1.88 Compilation::fill_env: BTreeSet native dirs (filter to output),
    # deps, profile root, sysroot target libdir, then inherited rustup dylib path.
    prefix = receipt['dynamic_library_prefix']
    inherited = env.get('LD_LIBRARY_PATH', '').split(':') if env.get('LD_LIBRARY_PATH') else []
    paths = inherited if inherited[:len(prefix)] == prefix else prefix + inherited
    env['LD_LIBRARY_PATH'] = ':'.join(paths)
    return env


def select_artifact(role):
    name, expected_count = ROLES[role]
    manifest = ROOT / 'crates' / name / 'Cargo.toml'
    metadata = decode((OUT / (role + '-metadata.json')).read_bytes())
    require(metadata['workspace_root'] == str(ROOT) and metadata['target_directory'] == str(ROOT / 'target'), 'unknown workspace/target layout')
    packages = [pkg for pkg in metadata['packages'] if pkg['name'] == name and pkg['manifest_path'] == str(manifest)]
    require(len(packages) == 1, 'package metadata is ambiguous')
    package = packages[0]
    require(not any(target['kind'] == ['custom-build'] for target in package['targets']), 'root package build-script runtime env unsupported')
    rows = [decode(line) for line in (OUT / (role + '-build.jsonl')).read_bytes().splitlines()]
    require(len([row for row in rows if row.get('reason') == 'build-finished']) == 1 and
            rows[-1] == {'reason': 'build-finished', 'success': True}, 'missing successful terminal Cargo build-finished')
    artifacts = [row for row in rows if row.get('reason') == 'compiler-artifact' and row.get('package_id') == package['id']
                 and row.get('target', {}).get('name') == 'postgres' and row['target']['kind'] == ['test']]
    require(len(artifacts) == 1, 'expected exactly one package postgres test artifact')
    artifact = artifacts[0]
    require(artifact['fresh'] is False and artifact['manifest_path'] == str(manifest) and artifact['features'] == [] and
            artifact['target']['src_path'] == str(manifest.parent / 'tests/postgres.rs') and
            artifact['target']['crate_types'] == ['bin'], 'default package test artifact differs')
    profile = artifact['profile']
    require(profile['opt_level'] == '1' and type(profile['debuginfo']) is int and profile['debuginfo'] == 0 and
            profile['debug_assertions'] is True and profile['overflow_checks'] is True and profile['test'] is True,
            'test profile artifact differs')
    binary = Path(artifact['executable'])
    require(binary.parent == ROOT / 'target/debug/deps' and re.fullmatch(r'postgres-[0-9a-f]{16}', binary.name) and
            binary.is_file() and not binary.is_symlink() and os.access(binary, os.X_OK) and
            artifact['filenames'] == [str(binary)], 'unknown native binary layout')
    # Rust PathBuf::Ord compares components, not whole-path strings. Preserve
    # kind prefixes during this ordering, then strip them exactly as Cargo does.
    linked = sorted({path for row in rows if row.get('reason') == 'build-script-executed' for path in row['linked_paths']},
                    key=lambda path: Path(path).parts)
    native = []
    for path in linked:
        if '=' in path and path.split('=', 1)[0] in ('native', 'crate', 'dependency', 'framework', 'all'):
            path = path.split('=', 1)[1]
        directory = Path(path)
        require(directory.anchor == '/' and ':' not in path and str(directory) == path and '..' not in directory.parts,
                'unknown linked-path layout')
        if directory.is_relative_to(ROOT / 'target/debug'):
            require(directory.is_dir() and directory.resolve() == directory, 'linked output directory missing/symlink')
            native.append(str(directory))
    toolchain = decode((OUT / (role + '-toolchain.json')).read_bytes())
    require(':' not in str(ROOT), 'colon in workspace path unsupported')
    receipt = {'role': role, 'package': package, 'selected_artifact': artifact, 'toolchain': toolchain,
               'dynamic_library_prefix': native + [str(ROOT / 'target/debug/deps'), str(ROOT / 'target/debug'), toolchain['target_libdir']],
               'binary': str(binary), 'binary_sha256': sha(binary), 'binary_bytes': binary.stat().st_size,
               'expected_inventory': expected_count}
    return receipt


def build(role):
    guards()
    require(OUT.is_dir(), 'monitor output directory absent')
    before = source_binding()
    cargo = subprocess.check_output(['rustup', 'which', '--toolchain', '1.88.0', 'cargo'], text=True).strip()
    rustc = subprocess.check_output(['rustup', 'which', '--toolchain', '1.88.0', 'rustc'], text=True).strip()
    argv = ['rustup', 'run', '1.88.0', 'python3', str(Path(__file__).resolve()), 'bridge', role, cargo, rustc]
    started = time.monotonic()
    with (OUT / (role + '-build.jsonl')).open('wb') as stdout, (OUT / (role + '-build.stderr')).open('wb') as stderr:
        completed = subprocess.run(argv, input=json.dumps(dict(os.environ)).encode(), stdout=stdout, stderr=stderr)
    save(OUT / (role + '-build-exit.json'), {'build_process_exit_code': completed.returncode,
         'seconds': time.monotonic() - started, 'argv': ['cargo', 'test', '-p', ROLES[role][0], '--test', 'postgres',
         '--locked', '--no-run', '--message-format=json']})
    require(completed.returncode == 0, 'sequential Cargo no-run build failed: ' + role)
    receipt = select_artifact(role)
    require(before == source_binding(), 'source/lock changed during build')
    receipt['source'] = before
    receipt['validated_no_run_build_exit_code'] = 0
    receipt['build_stdout_sha256'] = sha(OUT / (role + '-build.jsonl'))
    receipt['build_stderr_sha256'] = sha(OUT / (role + '-build.stderr'))
    env = runtime_env(role, receipt)
    with (OUT / (role + '-inventory.stdout')).open('wb') as stdout, (OUT / (role + '-inventory.stderr')).open('wb') as stderr:
        inventory = subprocess.run([receipt['binary'], '--list'], cwd=Path(receipt['package']['manifest_path']).parent,
                                   env=env, stdout=stdout, stderr=stderr)
    raw = (OUT / (role + '-inventory.stdout')).read_bytes()
    lines = raw.decode('utf-8', errors='strict').splitlines()
    names = [line[:-6] for line in lines if line.endswith(': test')]
    count = ROLES[role][1]
    require(inventory.returncode == 0 and len(names) == len(set(names)) == count and
            lines[-1] == str(count) + ' tests, 0 benchmarks' and len(lines) == count + 2,
            'full native inventory differs: ' + role)
    receipt.update({'inventory_count': count, 'inventory_stdout_sha256': sha(OUT / (role + '-inventory.stdout')),
                    'inventory_stderr_sha256': sha(OUT / (role + '-inventory.stderr'))})
    save(OUT / (role + '-binary.json'), receipt)
    require(before == source_binding() and sha(Path(receipt['binary'])) == receipt['binary_sha256'], 'build/inventory binding changed')
    print(role + ': separate Cargo no-run build and full binary inventory verified', flush=True)


def load_bound(role):
    guards()
    receipt = decode((OUT / (role + '-binary.json')).read_bytes())
    require(receipt['role'] == role and receipt['source'] == source_binding() and
            sha(Path(receipt['binary'])) == receipt['binary_sha256'], 'source/lock/binary binding changed: ' + role)
    return receipt


# Explicit execution successor. The original two-role/full-harness verbs below
# remain exact legacy policies; a shard never masquerades as a full Runtime run.
EXEC_SCHEMA = 'stateknot-native-runtime-shards-execution-v1'
PLAN_SCHEMA = 'stateknot-native-runtime-shards-plan-v1'
LANES = ('store', 'runtime-0', 'runtime-1')
DATABASES = {'store': 'stateknot_store_native', 'runtime-0': 'stateknot_runtime_native_0',
             'runtime-1': 'stateknot_runtime_native_1'}


def save_new(path, value):
    with path.open('x') as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True) + '\n')


def names_sha(names):
    return hashlib.sha256(json.dumps(names, separators=(',', ':')).encode()).hexdigest()


def parse_inventory(raw, expected):
    lines = raw.decode('utf-8', errors='strict').splitlines()
    names = [line[:-6] for line in lines if line.endswith(': test')]
    require(len(names) == len(set(names)) == expected and len(lines) == expected + 2
            and lines[-1] == str(expected) + ' tests, 0 benchmarks', 'exact inventory names/summary')
    require(all(re.fullmatch(r'[A-Za-z0-9_:]+', name) for name in names)
            and lines == [name + ': test' for name in names] + ['', str(expected) + ' tests, 0 benchmarks'],
            'closed inventory names/lines')
    return names


def parse_harness(raw, selected, total):
    text = raw.decode('utf-8', errors='strict')
    count = len(selected)
    require(0 < count <= total and len(set(selected)) == count, 'nonempty unique selected cases')
    require([line for line in text.splitlines() if line.startswith('running ')] == ['running ' + str(count) + ' tests'],
            'one exact running line')
    observed = re.findall(r'^test ([A-Za-z0-9_:]+) \.\.\. ok$', text, re.MULTILINE)
    require(len(observed) == count and len(set(observed)) == count
            and sorted(observed) == sorted(selected)
            and [line for line in text.splitlines() if line.startswith('test ') and not line.startswith('test result:')]
                == ['test ' + name + ' ... ok' for name in observed], 'selected case results exactly once')
    summaries = re.findall(r'^test result: ok[.] (\d+) passed; (\d+) failed; (\d+) ignored; (\d+) measured; (\d+) filtered out; finished in ([0-9]+(?:[.][0-9]+)?)s$', text, re.MULTILINE)
    require(len(summaries) == 1 and len([line for line in text.splitlines() if line.startswith('test result:')]) == 1
            and summaries[0][:5] == (str(count), '0', '0', '0', str(total - count)),
            'exact actual passed/filtered harness summary')
    seconds = float(summaries[0][5])
    require(0 <= seconds < 1800, 'actual harness within unchanged pair cap')
    return seconds


def capture_selection(receipt, label, argv):
    stdout, stderr = OUT / (label + '.stdout'), OUT / (label + '.stderr')
    with stdout.open('xb') as out, stderr.open('xb') as err:
        result = subprocess.run(argv, cwd=Path(receipt['package']['manifest_path']).parent,
                                env=runtime_env('runtime', receipt), stdout=out, stderr=err, timeout=90)
    require(result.returncode == 0 and stderr.read_bytes() == b'', 'actual list success')
    return stdout.read_bytes()


def plan_runtime_shards():
    receipt = load_bound('runtime')
    count = ROLES['runtime'][1]
    producer = OUT / 'import/producer/runtime-inventory.stdout'
    original = producer.read_bytes()
    require(sha(producer) == receipt['inventory_stdout_sha256'], 'preserved producer inventory SHA')
    actual = capture_selection(receipt, 'runtime-shards-inventory', [receipt['binary'], '--list'])
    require(actual == original, 'actual consumer inventory bytes equal producer')
    names = sorted(parse_inventory(actual, count))
    lanes = []
    for index in (0, 1):
        selected = names[index::2]
        label = 'runtime-' + str(index)
        listed = capture_selection(receipt, label + '-selection',
                                   [receipt['binary'], *selected, '--exact', '--list'])
        require(parse_inventory(listed, len(selected)) == selected, 'actual exact shard selection')
        lanes.append({'label': label, 'index': index, 'role': 'runtime', 'database': DATABASES[label],
                      'passed': len(selected), 'filtered': count - len(selected), 'names': selected,
                      'argv': [receipt['binary'], *selected, '--exact', '--test-threads=1'],
                      'selection_stdout_sha256': sha(OUT / (label + '-selection.stdout')),
                      'selection_stderr_sha256': sha(OUT / (label + '-selection.stderr'))})
    save_new(OUT / 'runtime-shards-plan.json', {'schema': PLAN_SCHEMA, 'execution_schema': EXEC_SCHEMA,
             'assignment': 'sorted-full-inventory-alternating-v1', 'runtime_binary_sha256': receipt['binary_sha256'],
             'runtime_receipt_sha256': sha(OUT / 'runtime-binary.json'), 'source': receipt['source'],
             'profile': PROFILE, 'producer_inventory_sha256': sha(producer),
             'actual_inventory_sha256': sha(OUT / 'runtime-shards-inventory.stdout'),
             'inventory_count': count, 'all_names': names, 'lanes': lanes})
    load_shard_plan()
    save_new(OUT / 'execution-schema.json', {'schema': EXEC_SCHEMA,
             'plan_sha256': sha(OUT / 'runtime-shards-plan.json'), 'lanes': list(LANES), 'pair_cap_seconds': 1800})


def load_shard_plan():
    receipt = load_bound('runtime')
    artifact = receipt['selected_artifact']
    profile = artifact['profile']
    require(artifact['features'] == [] and profile['opt_level'] == '1'
            and type(profile['debuginfo']) is int and profile['debuginfo'] == 0
            and profile['debug_assertions'] is True and profile['overflow_checks'] is True and profile['test'] is True
            and receipt['expected_inventory'] == receipt['inventory_count'] == ROLES['runtime'][1],
            'unchanged default-feature native Runtime profile/inventory')
    plan = decode((OUT / 'runtime-shards-plan.json').read_bytes())
    require(set(plan) == {'schema', 'execution_schema', 'assignment', 'runtime_binary_sha256',
            'runtime_receipt_sha256', 'source', 'profile', 'producer_inventory_sha256',
            'actual_inventory_sha256', 'inventory_count', 'all_names', 'lanes'}, 'closed shard plan')
    count = ROLES['runtime'][1]
    producer = OUT / 'import/producer/runtime-inventory.stdout'
    actual = OUT / 'runtime-shards-inventory.stdout'
    names = sorted(parse_inventory(actual.read_bytes(), count))
    require(plan['schema'] == PLAN_SCHEMA and plan['execution_schema'] == EXEC_SCHEMA
            and plan['assignment'] == 'sorted-full-inventory-alternating-v1'
            and plan['runtime_binary_sha256'] == receipt['binary_sha256']
            and plan['runtime_receipt_sha256'] == sha(OUT / 'runtime-binary.json')
            and plan['source'] == receipt['source'] and plan['profile'] == PROFILE
            and plan['producer_inventory_sha256'] == sha(producer) == receipt['inventory_stdout_sha256']
            and plan['actual_inventory_sha256'] == sha(actual) and actual.read_bytes() == producer.read_bytes()
            and plan['inventory_count'] == count and plan['all_names'] == names
            and (OUT / 'runtime-shards-inventory.stderr').read_bytes() == b''
            and len(plan['lanes']) == 2, 'complete original shard plan binding')
    for index, lane in enumerate(plan['lanes']):
        label, selected = 'runtime-' + str(index), names[index::2]
        require(set(lane) == {'label', 'index', 'role', 'database', 'passed', 'filtered', 'names', 'argv',
                             'selection_stdout_sha256', 'selection_stderr_sha256'}, 'closed lane plan')
        require(lane['label'] == label and type(lane['index']) is int and lane['index'] == index and lane['role'] == 'runtime'
                and lane['database'] == DATABASES[label] and lane['names'] == selected
                and lane['passed'] == len(selected) and lane['filtered'] == count - len(selected)
                and lane['argv'] == [receipt['binary'], *selected, '--exact', '--test-threads=1']
                and lane['selection_stdout_sha256'] == sha(OUT / (label + '-selection.stdout'))
                and lane['selection_stderr_sha256'] == sha(OUT / (label + '-selection.stderr'))
                and (OUT / (label + '-selection.stderr')).read_bytes() == b''
                and parse_inventory((OUT / (label + '-selection.stdout')).read_bytes(), len(selected)) == selected,
                'exact deterministic lane binding')
    require(set(plan['lanes'][0]['names']).isdisjoint(plan['lanes'][1]['names'])
            and sorted(plan['lanes'][0]['names'] + plan['lanes'][1]['names']) == names, 'exhaustive disjoint union')
    return receipt, plan


def run_runtime_shard(index):
    receipt, plan = load_shard_plan()
    lane = plan['lanes'][index]
    require_database_route(os.environ, lane['label'])
    env = runtime_env('runtime', receipt)
    fields = Path('/proc/self/stat').read_text().rsplit(') ', 1)[1].split()
    execution = {'pid': os.getpid(), 'start_ticks': fields[19], 'binary': receipt['binary'],
                 'binary_sha256': receipt['binary_sha256'], 'cwd': str(Path(receipt['package']['manifest_path']).parent),
                 'argv': lane['argv'], 'started_unix_ns': time.time_ns(),
                 'runtime_environment': {key: value for key, value in env.items()
                    if key.startswith('CARGO_PKG_') or key in ('CARGO', 'CARGO_MANIFEST_DIR', 'CARGO_MANIFEST_PATH',
                        'LD_LIBRARY_PATH', *TOOLCHAIN_ENV)},
                 'execution_schema': EXEC_SCHEMA, 'lane': lane['label'], 'logical_role': 'runtime',
                 'database': lane['database'], 'plan_sha256': sha(OUT / 'runtime-shards-plan.json'),
                 'selected_names_sha256': names_sha(lane['names'])}
    save_new(OUT / (lane['label'] + '-execution.json'), execution)
    os.chdir(Path(receipt['package']['manifest_path']).parent)
    os.execve(receipt['binary'], lane['argv'], env)


def require_database_route(env, lane):
    from urllib.parse import urlsplit
    route = urlsplit(env.get('STATEKNOT_TEST_DATABASE_URL', ''))
    require(route.scheme == 'postgres' and route.hostname == '127.0.0.1' and route.port == 5432
            and route.username == 'postgres' and route.path == '/' + DATABASES[lane]
            and route.query == route.fragment == '', 'fixed local lane database route')
    return DATABASES[lane]


def process_identity(pid):
    require(type(pid) is int and pid > 0, 'positive process PID')
    proc = Path('/proc/' + str(pid))
    fields = (proc / 'stat').read_text().rsplit(') ', 1)[1].split()
    require(len(fields) >= 20 and fields[0] not in ('Z', 'X'), 'live original process')
    return {'pid': pid, 'start_ticks': fields[19], 'ppid': int(fields[1]),
            'pgrp': int(fields[2]), 'session': int(fields[3]), 'exe': os.readlink(proc / 'exe')}


def overlap_sharded():
    runtime, plan = load_shard_plan()
    store = load_bound('store')
    pids = read_tsv(OUT / 'pids.tsv', 2)
    require(all(re.fullmatch(r'[1-9][0-9]*', row[0]) for row in pids.values())
            and len({row[0] for row in pids.values()}) == len(LANES), 'distinct positive leaders')
    deadline = time.monotonic() + 8
    while True:
        try:
            observed, identities, databases = {}, {}, {}
            for lane in LANES:
                role = 'store' if lane == 'store' else 'runtime'
                receipt = store if role == 'store' else runtime
                row = decode((OUT / (lane + '-execution.json')).read_bytes())
                leader = int(pids[lane][0])
                owner = OUT / (lane + '-owner')
                saved_leader = (owner / 'leader-identity').read_text()
                helper = (owner / 'helper-pid').read_text()
                require(re.fullmatch(str(leader) + r' [0-9]+\n', saved_leader)
                        and re.fullmatch(r'[1-9][0-9]*\n', helper), 'original leader/helper identities')
                owner_identity = process_identity(leader)
                helper_identity = process_identity(int(helper))
                binary_identity = process_identity(row['pid'])
                require(owner_identity['ppid'] == os.getppid()
                        and owner_identity['start_ticks'] == saved_leader.strip().split()[1]
                        and helper_identity['ppid'] == leader and binary_identity['ppid'] == int(helper)
                        and all(item['pgrp'] == item['session'] == leader
                                for item in (owner_identity, helper_identity, binary_identity))
                        and owner_identity['exe'] == helper_identity['exe']
                        and binary_identity['start_ticks'] == row['start_ticks']
                        and binary_identity['exe'] == row['binary'] == receipt['binary']
                        and sha(Path(binary_identity['exe'])) == row['binary_sha256'] == receipt['binary_sha256'],
                        'actual live binary in original monitor/leader/helper process tree')
                proc = Path('/proc/' + str(row['pid']))
                argv = (proc / 'cmdline').read_bytes().split(b'\0')
                require(argv[-1] == b'' and [part.decode('utf-8', errors='strict') for part in argv[:-1]] == row['argv']
                        and os.readlink(proc / 'cwd') == row['cwd'], 'actual binary argv/cwd')
                actual_env = {}
                for entry in (proc / 'environ').read_bytes().split(b'\0'):
                    if entry:
                        key, value = entry.decode('utf-8', errors='strict').split('=', 1)
                        require(key not in actual_env, 'unique actual environment keys')
                        actual_env[key] = value
                selected_env = {key: value for key, value in actual_env.items()
                    if key.startswith('CARGO_PKG_') or key in ('CARGO', 'CARGO_MANIFEST_DIR', 'CARGO_MANIFEST_PATH',
                        'LD_LIBRARY_PATH', *TOOLCHAIN_ENV)}
                expected_env = runtime_env(role, receipt)
                require(selected_env == row['runtime_environment'] == {key: value for key, value in expected_env.items()
                    if key.startswith('CARGO_PKG_') or key in ('CARGO', 'CARGO_MANIFEST_DIR', 'CARGO_MANIFEST_PATH',
                        'LD_LIBRARY_PATH', *TOOLCHAIN_ENV)}
                        and all(actual_env.get(key) == value for key, value in PROFILE.items())
                        and actual_env.get('STATEKNOT_REQUIRE_POSTGRES_TESTS') == '1'
                        and not actual_env.get('RUST_MIN_STACK'), 'actual runtime environment/profile/default stack')
                databases[lane] = require_database_route(actual_env, lane)
                observed[lane] = row
                identities[lane] = {'leader': owner_identity, 'helper': helper_identity, 'binary': binary_identity}
            require(len({item['pid'] for lane in identities.values() for item in lane.values()}) == len(LANES) * 3,
                    'distinct leader/helper/actual execution PIDs')
            # The observed route proof contains only these fixed database names.
            # Actual credentials are read transiently and never saved in receipts.
            save_new(OUT / 'native-binary-overlap-sharded.json', {'schema': EXEC_SCHEMA,
                     'observed_unix_ns': time.time_ns(), 'all_actual_binary_processes_alive': True,
                     'processes': observed, 'databases': databases, 'identities': identities})
            return
        except (OSError, ValueError):
            require(time.monotonic() < deadline, 'three actual native binaries did not overlap')
            time.sleep(0.05)


def assert_groups_retired(leader_pids):
    require(len(leader_pids) == len(set(leader_pids)) == len(LANES)
            and all(type(pid) is int and pid > 0 for pid in leader_pids), 'three original leader PIDs')
    groups = {str(pid) for pid in leader_pids}
    for proc in Path('/proc').glob('[0-9]*/stat'):
        try:
            fields = proc.read_text().rsplit(') ', 1)[1].split()
        except (FileNotFoundError, ProcessLookupError):
            continue
        require(fields[2] not in groups and fields[3] not in groups, 'original owner group/session still has a child')


def read_tsv(path, width):
    rows = [line.split('\t') for line in path.read_text().splitlines()]
    require(len(rows) == 3 and all(len(row) == width for row in rows)
            and {row[0] for row in rows} == set(LANES), 'exact three-lane ' + path.name)
    return {row[0]: row[1:] for row in rows}


def verify_sharded():
    runtime, plan = load_shard_plan()
    store = load_bound('store')
    imported = decode((OUT / 'import/import.json').read_bytes())
    abi = imported['consumer_admission_runtime']
    manifest_sha = imported['manifest_sha256']
    require(imported['schema'] == 'stateknot-postgres-native-artifact-v1-import'
            and imported['source'] == store['source'] == runtime['source']
            and imported['consumer_local_Cargo_build'] is False and imported['producer_receipts_preserved'] is True
            and imported['role_receipt_sha256'] == {role: sha(OUT / (role + '-binary.json')) for role in ROLES}
            and re.fullmatch(r'[0-9a-f]{64}', manifest_sha) and set(abi) == set(ROLES)
            and all(type(abi[role]) is list and abi[role] for role in ROLES), 'same imported binary/source/ABI admission')
    for phase in ('before', 'after'):
        actual = decode((OUT / ('import/' + phase + '.json')).read_bytes())
        require(actual.get('all_source_binary_DSO_unchanged') is True
                and actual.get('consumer_local_Cargo_build') is False and actual == {
                'schema': 'stateknot-postgres-native-artifact-v1-verification', 'phase': phase,
                'actual_runtime_closure': abi, 'all_source_binary_DSO_unchanged': True,
                'producer_manifest_sha256': manifest_sha, 'consumer_local_Cargo_build': False},
                'actual before/after binary/DSO ABI closure')
    plan_sha = sha(OUT / 'runtime-shards-plan.json')
    require(decode((OUT / 'execution-schema.json').read_bytes()) == {'schema': EXEC_SCHEMA,
            'plan_sha256': plan_sha, 'lanes': list(LANES), 'pair_cap_seconds': 1800}, 'explicit execution policy')
    require((OUT / 'created-databases.txt').read_text() == ''.join(DATABASES[lane] + '\n' for lane in LANES), 'three independent DBs')
    overlap = decode((OUT / 'native-binary-overlap-sharded.json').read_bytes())
    require(set(overlap) == {'schema', 'observed_unix_ns', 'all_actual_binary_processes_alive', 'processes', 'databases', 'identities'}
            and overlap['schema'] == EXEC_SCHEMA and overlap['all_actual_binary_processes_alive'] is True
            and type(overlap['observed_unix_ns']) is int and overlap['observed_unix_ns'] > 0
            and set(overlap['processes']) == set(overlap['identities']) == set(LANES)
            and overlap['databases'] == DATABASES, 'three actual binary overlap/DB proof')
    pids = read_tsv(OUT / 'pids.tsv', 2)
    require(all(re.fullmatch(r'[1-9][0-9]*', row[0]) for row in pids.values())
            and len({row[0] for row in pids.values()}) == len(LANES), 'distinct positive leaders')
    for name in ('suite-exits.tsv', 'leader-exits.tsv'):
        require(read_tsv(OUT / name, 2) == {lane: ['0'] for lane in LANES}, 'actual successful suite/leader waits')
    require(read_tsv(OUT / 'retire-requests.tsv', 2) == {lane: ['suite-complete'] for lane in LANES}, 'normal original owner retirement')
    require(all(row[-1] == '0' and row[0] == pids[lane][0]
                for lane, row in read_tsv(OUT / 'retired.tsv', 3).items()), 'all owned groups retired')
    require((OUT / 'cleanup.tsv').read_text() == 'monitor_exit\t0\n', 'monitor cleanup success')
    require(not (OUT / 'ownership-errors.tsv').exists() or (OUT / 'ownership-errors.tsv').stat().st_size == 0, 'ownership errors')
    require(not (OUT / 'runtime.stdout').exists() and not (OUT / 'runtime.stderr').exists()
            and not (OUT / 'runtime-execution.json').exists(), 'no synthetic legacy Runtime evidence')
    completions = read_tsv(OUT / 'lane-completions.tsv', 2)
    results, process_ids, start_times, end_times, monitor_pids = {}, [], [], [], set()
    for label in LANES:
        role = 'store' if label == 'store' else 'runtime'
        receipt = store if role == 'store' else runtime
        total = ROLES[role][1]
        if role == 'store':
            producer = OUT / 'import/producer/store-inventory.stdout'
            require(sha(producer) == store['inventory_stdout_sha256'], 'preserved complete Store inventory SHA')
            selected = parse_inventory(producer.read_bytes(), total)
        else:
            selected = plan['lanes'][int(label[-1])]['names']
        execution = decode((OUT / (label + '-execution.json')).read_bytes())
        keys = {'pid', 'start_ticks', 'binary', 'binary_sha256', 'cwd', 'argv', 'started_unix_ns', 'runtime_environment'}
        if role == 'runtime':
            keys |= {'execution_schema', 'lane', 'logical_role', 'database', 'plan_sha256', 'selected_names_sha256'}
        require(set(execution) == keys and execution == overlap['processes'][label], 'closed actual execution/overlap receipt')
        require(type(execution['pid']) is int and execution['pid'] > 0
                and type(execution['start_ticks']) is str and re.fullmatch(r'[0-9]+', execution['start_ticks'])
                and execution['binary'] == receipt['binary'] and execution['binary_sha256'] == receipt['binary_sha256'], 'actual lane binary identity')
        expected_argv = [receipt['binary'], '--test-threads=1'] if role == 'store' else plan['lanes'][int(label[-1])]['argv']
        env = runtime_env(role, receipt)
        expected_environment = {key: value for key, value in env.items()
            if key.startswith('CARGO_PKG_') or key in ('CARGO', 'CARGO_MANIFEST_DIR', 'CARGO_MANIFEST_PATH',
                'LD_LIBRARY_PATH', *TOOLCHAIN_ENV)}
        require(execution['argv'] == expected_argv and execution['cwd'] == str(Path(receipt['package']['manifest_path']).parent)
                and type(execution['started_unix_ns']) is int and 0 < execution['started_unix_ns'] <= overlap['observed_unix_ns']
                and execution['runtime_environment'] == expected_environment and not env.get('RUST_MIN_STACK'),
                'fixed actual argv/cwd/Cargo environment/default stack')
        if role == 'runtime':
            require(execution['execution_schema'] == EXEC_SCHEMA and execution['lane'] == label
                    and execution['logical_role'] == role and execution['database'] == DATABASES[label]
                    and execution['plan_sha256'] == plan_sha and execution['selected_names_sha256'] == names_sha(selected), 'exact runtime lane routing')
        command = 'run store' if role == 'store' else 'run-runtime-shard ' + label[-1]
        require((OUT / (label + '-command.txt')).read_text()
                == 'python3 .github/scripts/postgres-native-binaries.py ' + command + '\n', 'fixed original lane command')
        seconds = parse_harness((OUT / (label + '.stdout')).read_bytes(), selected, total)
        owner = OUT / (label + '-owner')
        identity, helper = (owner / 'leader-identity').read_text(), (owner / 'helper-pid').read_text()
        require(re.fullmatch(re.escape(pids[label][0]) + r' [0-9]+\n', identity)
                and re.fullmatch(r'[1-9][0-9]*\n', helper)
                and (owner / 'cleanup-complete').read_text() == identity
                and (owner / 'cleanup-reason').read_text() == 'parent-cleanup\n'
                and (owner / 'helper-wait-status').read_text() == '0\n'
                and (owner / 'suite-status').read_text() == '0\n'
                and not (owner / 'forced-kill').exists() and not (owner / 'control').exists(), 'true original owner/helper cleanup/waits')
        identities = overlap['identities'][label]
        require(set(identities) == {'leader', 'helper', 'binary'}, 'closed observed process tree')
        for item in identities.values():
            require(set(item) == {'pid', 'start_ticks', 'ppid', 'pgrp', 'session', 'exe'}
                    and all(type(item[key]) is int and item[key] > 0 for key in ('pid', 'ppid', 'pgrp', 'session'))
                    and type(item['start_ticks']) is str and re.fullmatch(r'[0-9]+', item['start_ticks'])
                    and type(item['exe']) is str and item['exe'].startswith('/'), 'closed actual process identity')
        leader_identity, helper_identity, binary_identity = identities['leader'], identities['helper'], identities['binary']
        require(leader_identity['pid'] == int(pids[label][0])
                and identity == str(leader_identity['pid']) + ' ' + leader_identity['start_ticks'] + '\n'
                and helper_identity['pid'] == int(helper) and helper_identity['ppid'] == leader_identity['pid']
                and int(leader_identity['start_ticks']) <= int(helper_identity['start_ticks']) <= int(binary_identity['start_ticks'])
                and binary_identity['pid'] == execution['pid'] and binary_identity['ppid'] == helper_identity['pid']
                and binary_identity['start_ticks'] == execution['start_ticks'] and binary_identity['exe'] == execution['binary']
                and leader_identity['exe'] == helper_identity['exe']
                and all(item['pgrp'] == item['session'] == leader_identity['pid'] for item in identities.values()),
                'original observed leader/helper/binary identity relation')
        monitor_pids.add(leader_identity['ppid'])
        require(re.fullmatch(r'[1-9][0-9]*', completions[label][0]), 'positive actual completion timestamp')
        end = int(completions[label][0])
        require(overlap['observed_unix_ns'] <= end
                and 0 <= end - execution['started_unix_ns'] < 1800 * 1_000_000_000
                and seconds * 1_000_000_000 <= end - execution['started_unix_ns'] + 1_000_000_000, 'bounded actual lane wall time')
        process_ids.extend((int(pids[label][0]), int(helper.strip()), execution['pid']))
        start_times.append(execution['started_unix_ns'])
        end_times.append(end)
        results[label] = {'passed': len(selected), 'filtered': total - len(selected), 'test_seconds': seconds,
                          'stdout_sha256': sha(OUT / (label + '.stdout')), 'stderr_sha256': sha(OUT / (label + '.stderr'))}
    require(len(monitor_pids) == 1 and monitor_pids.isdisjoint(process_ids)
            and len(set(process_ids)) == len(LANES) * 3 and max(end_times) - min(start_times) < 1800 * 1_000_000_000,
            'distinct leader/helper/actual executions and unchanged pair cap')
    hashes = (OUT / 'log-sha256.txt').read_text().splitlines()
    require(len(hashes) == len(LANES) * 2 and all(hashes.count(sha(OUT / (lane + '.' + stream)) + '  postgres-native-parallel/' + lane + '.' + stream) == 1
            for lane in LANES for stream in ('stdout', 'stderr')), 'all six raw output hashes')
    from calendar import timegm
    pair = []
    for filename in ('pair-started.txt', 'pair-completed.txt'):
        raw = (OUT / filename).read_text()
        require(re.fullmatch(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z\n', raw), 'actual UTC pair timestamp')
        pair.append(timegm(time.strptime(raw.strip(), '%Y-%m-%dT%H:%M:%SZ')) * 1_000_000_000)
    require(pair[0] <= min(start_times) and max(end_times) < pair[1] + 1_000_000_000
            and 0 <= pair[1] - pair[0] < 1800 * 1_000_000_000, 'unchanged full pair wall-time cap')
    assert_groups_retired([int(pids[lane][0]) for lane in LANES])
    save_new(OUT / 'native-runtime-shards-verification.json', {'schema': EXEC_SCHEMA, 'plan_sha256': plan_sha,
             'full_runtime_inventory': ROLES['runtime'][1], 'disjoint_exhaustive_actual_results': True,
             'all_three_live_owners_retired': True, 'results': results, 'synthetic_full_runtime_summary': False})


def main():
    require(len(sys.argv) >= 2, 'mode required')
    mode = sys.argv[1]
    if mode == 'plan-runtime-shards':
        require(len(sys.argv) == 2, 'plan-runtime-shards takes no arguments')
        plan_runtime_shards()
    elif mode == 'run-runtime-shard':
        require(len(sys.argv) == 3 and sys.argv[2] in ('0', '1'), 'fixed runtime shard index')
        run_runtime_shard(int(sys.argv[2]))
    elif mode == 'overlap-sharded':
        require(len(sys.argv) == 2, 'overlap-sharded takes no arguments')
        overlap_sharded()
    elif mode == 'verify-sharded':
        require(len(sys.argv) == 2, 'verify-sharded takes no arguments')
        verify_sharded()
    elif mode == 'bridge':
        require(len(sys.argv) == 5 and sys.argv[2] in ROLES, 'invalid bridge arguments')
        bridge(sys.argv[2], sys.argv[3], sys.argv[4])
    elif mode in ('build', 'run'):
        require(len(sys.argv) == 3 and sys.argv[2] in ROLES, 'only fixed store/runtime roles supported')
        role = sys.argv[2]
        if mode == 'build':
            build(role)
        else:
            receipt = load_bound(role)
            env = runtime_env(role, receipt)
            fields = Path('/proc/self/stat').read_text().rsplit(') ', 1)[1].split()
            save(OUT / (role + '-execution.json'), {'pid': os.getpid(), 'start_ticks': fields[19],
                 'binary': receipt['binary'], 'binary_sha256': receipt['binary_sha256'],
                 'cwd': str(Path(receipt['package']['manifest_path']).parent),
                 'argv': [receipt['binary'], '--test-threads=1'], 'started_unix_ns': time.time_ns(),
                 'runtime_environment': {key: value for key, value in env.items()
                    if key.startswith('CARGO_PKG_') or key in ('CARGO', 'CARGO_MANIFEST_DIR', 'CARGO_MANIFEST_PATH',
                        'LD_LIBRARY_PATH', *TOOLCHAIN_ENV)}})
            os.chdir(Path(receipt['package']['manifest_path']).parent)
            os.execve(receipt['binary'], [receipt['binary'], '--test-threads=1'], env)
    elif mode == 'overlap':
        require(len(sys.argv) == 2, 'overlap takes no arguments')
        deadline = time.monotonic() + 8
        while True:
            observed = {}
            try:
                for role in ROLES:
                    row = decode((OUT / (role + '-execution.json')).read_bytes())
                    fields = Path('/proc/' + str(row['pid']) + '/stat').read_text().rsplit(') ', 1)[1].split()
                    require(fields[0] not in ('Z', 'X') and fields[19] == row['start_ticks'], 'binary identity is no longer alive')
                    require(os.readlink('/proc/' + str(row['pid']) + '/exe') == row['binary'], 'native binary has not execed')
                    observed[role] = row
                save(OUT / 'native-binary-overlap.json', {'observed_unix_ns': time.time_ns(),
                     'both_actual_binary_processes_alive': True, 'processes': observed})
                break
            except (OSError, ValueError):
                require(time.monotonic() < deadline, 'two actual native binaries did not overlap')
                time.sleep(0.05)
    elif mode == 'verify':
        require(len(sys.argv) == 2, 'verify takes no arguments')
        for role in ROLES:
            load_bound(role)
    else:
        raise ValueError('unknown mode')


if __name__ == '__main__':
    main()
