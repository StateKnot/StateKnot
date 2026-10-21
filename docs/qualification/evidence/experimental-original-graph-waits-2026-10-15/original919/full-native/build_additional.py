from pathlib import Path
import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time

root = Path('/Users/jiawy/Documents/RustAgentFramework')
base = root / '.local-verification/2026-10-15'
integration = base / 'elapsed36-attempt-proof-integration'
output = integration / 'full-native-v1'
source = base / 'elapsed36-attempt-proof-source'
target = root / 'target/r1-oct15-elapsed36-lint'
out = output / 'fresh-additional-build'
out.mkdir(exist_ok=False)
(out / 'runner.py').write_bytes(Path(__file__).read_bytes())
manifest = json.loads((integration / 'source-manifest.json').read_text())


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def check_source(label):
    files = [dict(path=str(p.relative_to(source)), bytes=p.stat().st_size, sha256=sha(p))
             for p in sorted(source.rglob('*')) if p.is_file()]
    assert files == manifest['files'] and len(files) == 919
    write(out / (label + '-source-manifest.json'), dict(source_tree_sha256=manifest['source_tree_sha256'], files=files))


try:
    check_source('before')
    assert target.resolve().parent == root / 'target' and target.resolve().name == 'r1-oct15-elapsed36-lint'
    assert (target / 'CACHEDIR.TAG').read_text().startswith('Signature: 8a477f597d28d172789f06886806bc55')
    assert not subprocess.check_output(['git', 'ls-files', '--', str(target.relative_to(root))], cwd=root).strip()
    (out / 'before.df').write_bytes(subprocess.check_output(['df', '-k', str(root)]))
    (out / 'cache-size-before.txt').write_bytes(subprocess.check_output(['du', '-sk', str(target)]))
    opened = subprocess.run(['lsof', '+D', str(target)], text=True, capture_output=True)
    (out / 'cache-open-files.txt').write_text(opened.stdout + opened.stderr)
    assert opened.returncode == 1 and not opened.stdout.strip() and not opened.stderr.strip(), 'Cache activity uncertain; no cleaning performed.'
    lock = target / 'debug/.cargo-lock'
    lock_proof = dict(path=str(lock), exists=lock.exists(), all_target_open_files_empty=True)
    if lock.exists():
        with lock.open('rb') as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        lock_proof['nonblocking_exclusive_lock_obtained_and_released'] = True
    write(out / 'cache-safety.json', lock_proof)
    env = os.environ.copy()
    env.update(CARGO_TARGET_DIR=str(target), CARGO_INCREMENTAL='0', CARGO_PROFILE_DEV_DEBUG='0',
               CARGO_PROFILE_TEST_DEBUG='0', RUSTFLAGS='-D warnings', RUSTDOCFLAGS='-D warnings')
    assert 'RUST_MIN_STACK' not in env
    for key in list(env):
        if key.startswith('STATEKNOT_TEST_') or key in ['STATEKNOT_REQUIRE_POSTGRES_TESTS',
                'STATEKNOT_PROOF_BASE_PROBE', 'STATEKNOT_SQL37_TRACE_DIR']:
            env.pop(key)
    for tool in ['cargo', 'rustc']:
        (out / (tool + '-version.txt')).write_bytes(subprocess.check_output([tool, '--version'], env=env))
    clean = ['cargo', 'clean', '--manifest-path', str(source / 'Cargo.toml'), '--target-dir', str(target),
             '-p', 'stateknot-integrations', '-p', 'stateknot']
    with (out / 'workspace-package-clean.log').open('w') as log:
        result = subprocess.run(clean, env=env, cwd=source, stdout=log, stderr=subprocess.STDOUT)
    assert result.returncode == 0
    (out / 'after-clean.df').write_bytes(subprocess.check_output(['df', '-k', str(root)]))
    cases = [('stateknot-integrations', 'stateknot-integrations', '--test', 'provider_native_postgres'),
             ('stateknot-deepseek_agent', 'stateknot', '--example', 'deepseek_agent')]
    receipts = []
    for label, package, kind, name in cases:
        command = ['cargo', 'test', '--manifest-path', str(source / 'Cargo.toml'), '-p', package,
                   kind, name, '--all-features', '--locked', '--no-run', '--message-format=json']
        started = time.time()
        stdout_path = out / (label + '-build.stdout.jsonl')
        stderr_path = out / (label + '-build.stderr.log')
        with stdout_path.open('w') as stdout, stderr_path.open('w') as stderr:
            process = subprocess.Popen(command, cwd=source, env=env, stdout=stdout, stderr=stderr)
            write(out / 'running.json', dict(pid=process.pid, command=command, started_unix=started))
            process.wait()
        artifacts = []
        if process.returncode == 0:
            for line in stdout_path.read_text().splitlines():
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if event.get('reason') != 'compiler-artifact' or not event.get('executable'):
                    continue
                if event['target']['name'] != name or ('/' + package + '#') not in event['package_id']:
                    continue
                assert event.get('fresh') is False
                saved = out / label
                shutil.copy2(event['executable'], saved)
                listing = subprocess.check_output([str(saved), '--list', '--format', 'terse'], text=True)
                (out / (label + '--list.txt')).write_text(listing)
                names = [line[:-6] for line in listing.splitlines() if line.endswith(': test')]
                assert names and len(names) == len(set(names))
                artifacts.append(dict(package=package, package_id=event['package_id'], name=name,
                      path=str(saved), sha256=sha(saved), fresh=False, exact_test_count=len(names), exact_names=names))
            assert len(artifacts) == 1
        receipt = dict(source=str(source), source_tree_sha256=manifest['source_tree_sha256'], source_files=919,
              command=command, exit_code=process.returncode, seconds=time.time() - started,
              artifacts=artifacts, target=str(target), clean=clean, cache_safety_confirmed=True,
              strict_environment={key: env[key] for key in ['CARGO_INCREMENTAL', 'CARGO_PROFILE_DEV_DEBUG',
                    'CARGO_PROFILE_TEST_DEBUG', 'RUSTFLAGS', 'RUSTDOCFLAGS']}, no_native_execution=True)
        write(out / (label + '-receipt.json'), receipt)
        receipts.append(receipt)
        print(json.dumps(dict(label=label, exit_code=process.returncode, seconds=receipt['seconds'],
              artifacts=artifacts)), flush=True)
        assert process.returncode == 0
    check_source('after')
    (out / 'after.df').write_bytes(subprocess.check_output(['df', '-k', str(root)]))
    (out / 'cache-size-after.txt').write_bytes(subprocess.check_output(['du', '-sk', str(target)]))
    write(out / 'receipt.json', dict(exit_code=0, builds=receipts, source_unchanged=True, source_files=919,
          source_tree_sha256=manifest['source_tree_sha256'], target_retained=True))
except BaseException as error:
    write(out / 'build-failed.json', dict(error=repr(error), recorded_unix=time.time()))
    raise
