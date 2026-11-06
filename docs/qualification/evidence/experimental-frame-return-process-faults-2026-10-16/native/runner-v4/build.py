#!/usr/bin/env python3
"""Build/save the default-feature Return fault Native target. No database execution."""
import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
DAY = HERE.parent
REPO = HERE.parents[2]
TARGET = REPO / 'target/r1-oct15-native-opt1'
OVERRIDES = ('RUST_MIN_STACK', 'CARGO_ENCODED_RUSTFLAGS', 'RUSTC_WRAPPER',
             'RUSTC_WORKSPACE_WRAPPER', 'RUSTC_BOOTSTRAP', 'RUSTC')
CASES = {
    'store': ('stateknot-store-postgres', 264, 'frame_barriers::nested_entry_after_scoped_barrier_authenticates_parent_successor_and_usage_floor'),
    'runtime': ('stateknot-runtime', 191, 'nested_driver::frame_return_process_faults::frame_return_commit_loss_survives_real_kill_and_cold_higher_fence_driver'),
}
sha = lambda p: hashlib.sha256(Path(p).read_bytes()).hexdigest()
obj = lambda p: json.loads(Path(p).read_bytes())
canon = lambda x: json.dumps(x, sort_keys=True, separators=(',', ':')).encode()


def write(out, name, value):
    (out / name).write_text(json.dumps(value, indent=2) + '\n')


def common_env():
    assert not any(os.environ.get(k) for k in OVERRIDES), 'compiler/stack override'
    env = {k: v for k, v in os.environ.items() if not k.startswith(('STATEKNOT_', 'CARGO_PROFILE_'))}
    for k in OVERRIDES:
        env.pop(k, None)
    # Same six CI test-profile values; DEV opt-level is deliberately unmodified.
    env.update(CARGO_TARGET_DIR=str(TARGET), CARGO_INCREMENTAL='0',
               CARGO_PROFILE_DEV_DEBUG='0', CARGO_PROFILE_TEST_DEBUG='0',
               CARGO_PROFILE_TEST_OPT_LEVEL='1', CARGO_PROFILE_TEST_DEBUG_ASSERTIONS='true',
               CARGO_PROFILE_TEST_OVERFLOW_CHECKS='true', RUSTFLAGS='-D warnings',
               RUSTDOCFLAGS='-D warnings')
    return env


def env_allowlist(env):
    return {k: env[k] for k in sorted(env) if k.startswith('CARGO_PROFILE_') or
            k in ('CARGO_TARGET_DIR', 'CARGO_INCREMENTAL', 'RUSTFLAGS', 'RUSTDOCFLAGS')}


def check_source(source, manifest_path, compiled_path):
    assert source.resolve() == source and source.is_relative_to(DAY) and source != REPO
    m, ci = obj(manifest_path), obj(compiled_path)
    assert m['count'] == len(m['files']) == 932
    assert hashlib.sha256(canon(m['files'])).hexdigest() == m['source_tree_sha256']
    actual = {p.relative_to(source).as_posix() for p in source.rglob('*') if p.is_file()}
    assert actual == {r['path'] for r in m['files']}
    for r in m['files']:
        p = source / r['path']
        assert not p.is_symlink() and (p.stat().st_size, sha(p)) == (r['bytes'], r['sha256']), r['path']
    assert ci['count'] == len(ci['files']) == 541
    assert ci['files'] == sorted(ci['files'], key=lambda r: r['path'])
    assert hashlib.sha256(canon(ci['files'])).hexdigest() == ci['sha256']
    source_rows = {r['path']: r for r in m['files']}
    assert all(source_rows[r['path']] == r for r in ci['files'])
    return m, ci


def disk(out, label):
    raw = subprocess.check_output(['df', '-k', str(REPO)])
    (out / (label + '.df')).write_bytes(raw)
    assert int(raw.decode().splitlines()[-1].split()[3]) >= 50 * 1024**2
    caches = [{'path': str(p), 'logical_kib': int(subprocess.check_output(['du', '-sk', str(p)]).split()[0])}
              for p in sorted((REPO / 'target').glob('r1-oct15*')) if p.is_dir()]
    total = sum(r['logical_kib'] for r in caches)
    write(out, label + '-caches.json', {'caches': caches, 'total_logical_kib': total,
          'not_APFS_reclaimable_space': True, 'cleanup': False})
    assert total <= 20 * 1024**2


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--plan', type=Path, default=DAY / 'graph-frame-return-process-fault-qualification-v3/scope.json')
    p.add_argument('--role', choices=('candidate',), required=True)
    p.add_argument('--package', choices=CASES, required=True)
    p.add_argument('--source-manifest', type=Path, required=True)
    p.add_argument('--compiled-inputs', type=Path, required=True)
    p.add_argument('--previous-compile-receipt', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    out = a.out.resolve(); out.mkdir(parents=True, exist_ok=False)
    plan = obj(a.plan); source = DAY / plan['source_directory']
    m, ci = check_source(source, a.source_manifest, a.compiled_inputs)
    assert m['source_tree_sha256'] == plan['source_tree_sha256']
    assert ci['sha256'] == plan['compiled_inputs_sha256']
    assert TARGET.resolve() == TARGET and not TARGET.is_symlink()
    env = common_env(); disk(out, 'before')
    previous = obj(a.previous_compile_receipt)
    assert previous['environment_allowlist']['CARGO_TARGET_DIR'] == str(TARGET)
    assert 'cargo' in previous['argv'][0] and 'start_unix' in previous and 'seconds' in previous
    cutoff_ns = int((previous['start_unix'] + previous['seconds']) * 1_000_000_000)
    stamp = time.time_ns(); assert stamp > cutoff_ns, 'previous compile has not ended'
    mtimes = []
    for row in ci['files']:
        path = source / row['path']; before = path.stat()
        os.utime(path, ns=(before.st_atime_ns, stamp))
        after = path.stat()
        assert after.st_mtime_ns > cutoff_ns
        assert (after.st_size, sha(path)) == (row['bytes'], row['sha256'])
        mtimes.append({'path': row['path'], 'sha256': row['sha256'],
                       'before_mtime_ns': before.st_mtime_ns, 'after_mtime_ns': after.st_mtime_ns})
    write(out, 'freshness.json', {'previous_compile_receipt': str(a.previous_compile_receipt.resolve()),
          'previous_compile_receipt_sha256': sha(a.previous_compile_receipt),
          'previous_compile_end_unix_ns': cutoff_ns, 'refresh_unix_ns': stamp,
          'all541_bytes_unchanged': True, 'all541_mtimes_after_previous_compile': True, 'files': mtimes})
    check_source(source, a.source_manifest, a.compiled_inputs)
    package, count, case = CASES[a.package]
    argv = ['cargo', '+1.88.0', 'test', '-p', package, '--test', 'postgres', '--locked',
            '--no-run', '--message-format=json']
    started = time.time()
    with (out / 'build.jsonl').open('wb') as stdout, (out / 'build.stderr').open('wb') as stderr:
        process = subprocess.Popen(argv, cwd=source, env=env, stdout=stdout, stderr=stderr)
        running = {'argv': argv, 'cwd': str(source), 'pid': process.pid, 'runner_pid': os.getpid(),
                   'start_unix': started, 'runner_sha256': sha(__file__), 'role': a.role,
                   'package': a.package, 'source_tree': m['source_tree_sha256'],
                   'compiled_inputs_sha256': ci['sha256'], 'source_manifest': str(a.source_manifest.resolve()),
                   'source_manifest_sha256': sha(a.source_manifest), 'compiled_inputs': str(a.compiled_inputs.resolve()),
                   'compiled_manifest_sha256': sha(a.compiled_inputs), 'environment_allowlist': env_allowlist(env),
                   'overrides_present': {k: k in env for k in OVERRIDES},
                   'DEV_OPT_LEVEL_override_present': 'CARGO_PROFILE_DEV_OPT_LEVEL' in env,
                   'cache_CACHEDIR_TAG_exists': (TARGET / 'CACHEDIR.TAG').is_file(),
                   'cache_TAG_not_fabricated_and_no_cleanup': True, 'database_execution': False,
                   'freshness_sha256': sha(out / 'freshness.json')}
        write(out, 'build.running.json', running); print(json.dumps(running), flush=True)
        code = process.wait()
    after_m, after_ci = check_source(source, a.source_manifest, a.compiled_inputs)
    receipt = {**running, 'exit_code': code, 'seconds': time.time() - started,
               'stdout_sha256': sha(out / 'build.jsonl'), 'stderr_sha256': sha(out / 'build.stderr'),
               'source_before_after_exact': after_m == m and after_ci == ci}
    write(out, 'build.receipt.json', receipt); disk(out, 'after')
    if code:
        raise SystemExit(code)
    artifacts = [json.loads(line) for line in (out / 'build.jsonl').read_text().splitlines()
                 if json.loads(line).get('reason') == 'compiler-artifact']
    write(out, 'compiler-artifacts.json', artifacts)
    wanted = [('stateknot-core', 'stateknot_core', 'lib'),
              ('stateknot-store-postgres', 'stateknot_store_postgres', 'lib')]
    if a.package == 'runtime':
        wanted.append(('stateknot-runtime', 'stateknot_runtime', 'lib'))
    wanted.append((package, 'postgres', 'test'))
    selected = []
    for crate, name, kind in wanted:
        found = [r for r in artifacts if Path(r['manifest_path']) == source / 'crates' / crate / 'Cargo.toml'
                 and r['target']['name'] == name and r['target']['kind'] == [kind]]
        assert len(found) == 1, (crate, name, len(found))
        item = found[0]; selected.append(item)
        assert item['fresh'] is False, (crate, name, 'fresh artifact rejected')
        assert item['features'] == []
        profile = item['profile']
        assert profile['opt_level'] == '1' and profile['debuginfo'] == 0
        assert profile['debug_assertions'] is True and profile['overflow_checks'] is True
        assert profile['test'] == (kind == 'test')
    executable = Path(selected[-1]['executable']).resolve()
    assert executable.is_relative_to(TARGET) and executable.is_file()
    saved = out / 'binaries' / (a.package + '-postgres'); saved.parent.mkdir()
    shutil.copy2(executable, saved); assert sha(executable) == sha(saved)
    inventory = subprocess.run([str(saved), '--list'], cwd=source, env=env, capture_output=True)
    (out / 'inventory.stdout').write_bytes(inventory.stdout); (out / 'inventory.stderr').write_bytes(inventory.stderr)
    names = re.findall(rb'^([^\n]+): test$', inventory.stdout, re.M)
    assert inventory.returncode == 0 and len(names) == len(set(names)) == count and case.encode() in names
    summary = {'role': a.role, 'package': a.package, 'source': str(source),
               'source_manifest': str(a.source_manifest.resolve()), 'compiled_inputs': str(a.compiled_inputs.resolve()),
               'source_tree': m['source_tree_sha256'], 'compiled_inputs_sha256': ci['sha256'],
               'selected_artifacts_all_fresh_false': selected, 'all_compiler_artifacts_sha256': sha(out / 'compiler-artifacts.json'),
               'saved_executable': str(saved), 'binary_sha256': sha(saved), 'binary_bytes': saved.stat().st_size,
               'inventory_count': count, 'exact_case': case, 'expected_filtered': count - 1,
               'inventory_exit_code': inventory.returncode, 'inventory_sha256': sha(out / 'inventory.stdout'),
               'build_receipt_sha256': sha(out / 'build.receipt.json'), 'database_executions': 0,
               'cold_or_fullCI_qualification': False}
    write(out, 'build-summary.json', summary)
    print(json.dumps({'saved_binary': str(saved), 'sha256': summary['binary_sha256'],
                      'fresh_false_artifacts': len(selected), 'inventory': count}), flush=True)


if __name__ == '__main__':
    main()
