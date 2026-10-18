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
ROLES = {'store': ('stateknot-store-postgres', 264), 'runtime': ('stateknot-runtime', 193)}
PREBUILDS = (
    ('provider_native_postgres', ('cargo', 'test', '-p', 'stateknot-integrations', '--test',
                                 'provider_native_postgres', '--locked', '--no-run')),
    ('deepseek_agent', ('cargo', 'test', '-p', 'stateknot', '--example',
                        'deepseek_agent', '--locked', '--no-run')),
)
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
    binary = Path(receipt['binary'])
    require(binary.parent == ROOT / 'target/debug/deps' and
            re.fullmatch(r'postgres-[0-9a-f]{16}', binary.name) and binary.is_file() and
            not binary.is_symlink() and os.access(binary, os.X_OK) and
            binary.stat().st_size == receipt['binary_bytes'], 'bound native binary layout changed: ' + role)
    require(receipt['role'] == role and receipt['source'] == source_binding() and
            sha(binary) == receipt['binary_sha256'], 'source/lock/binary binding changed: ' + role)
    return receipt


def prebuild():
    # Only Cargo compilation overlaps the native pair. The original later
    # workflow commands still execute their real test harnesses in order.
    overlap = decode((OUT / 'native-binary-overlap.json').read_bytes())
    started_unix_ns = time.time_ns()
    require(overlap['both_actual_binary_processes_alive'] is True and
            type(overlap['observed_unix_ns']) is int and overlap['observed_unix_ns'] <= started_unix_ns,
            'actual native-pair overlap must precede downstream compilation')
    fields = Path('/proc/self/stat').read_text().rsplit(') ', 1)[1].split()
    save(OUT / 'compile-execution.json', {'pid': os.getpid(), 'start_ticks': fields[19],
         'started_unix_ns': started_unix_ns, 'native_overlap_observed_unix_ns': overlap['observed_unix_ns'],
         'argv': [list(argv) for _, argv in PREBUILDS], 'mode': 'cargo-no-run'})

    def originals():
        bindings = {}
        for role in ROLES:
            receipt = load_bound(role)
            bindings[role] = {key: receipt[key] for key in ('binary', 'binary_sha256', 'binary_bytes', 'source')}
        return bindings

    for target, argv in PREBUILDS:
        before = originals()
        save(OUT / (target + '-prebuild-originals-before.json'), before)
        started = time.monotonic()
        started_unix_ns = time.time_ns()
        with (OUT / (target + '-prebuild.stdout')).open('wb') as stdout, \
                (OUT / (target + '-prebuild.stderr')).open('wb') as stderr:
            completed = subprocess.run(argv, cwd=ROOT, stdout=stdout, stderr=stderr)
        save(OUT / (target + '-prebuild-exit.json'), {'build_process_exit_code': completed.returncode,
             'seconds': time.monotonic() - started, 'started_unix_ns': started_unix_ns,
             'completed_unix_ns': time.time_ns(), 'argv': list(argv), 'mode': 'cargo-no-run',
             'stdout_sha256': sha(OUT / (target + '-prebuild.stdout')),
             'stderr_sha256': sha(OUT / (target + '-prebuild.stderr'))})
        after = originals()
        save(OUT / (target + '-prebuild-originals-after.json'), after)
        require(before == after, 'native source/binary changed during downstream compilation: ' + target)
        require(completed.returncode == 0, 'downstream Cargo no-run build failed: ' + target)
        print(target + ': downstream Cargo no-run build passed; both original native bindings unchanged', flush=True)


def main():
    require(len(sys.argv) >= 2, 'mode required')
    mode = sys.argv[1]
    if mode == 'bridge':
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
    elif mode == 'prebuild':
        require(len(sys.argv) == 3 and sys.argv[2] == 'downstream', 'only fixed downstream prebuilds supported')
        prebuild()
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
