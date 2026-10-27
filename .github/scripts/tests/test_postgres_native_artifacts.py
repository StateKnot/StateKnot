#!/usr/bin/env python3
# Copyright 2026 StateKnot contributors
# SPDX-License-Identifier: Apache-2.0
"""Bounded artifact decoding failures, without Cargo, binaries, credentials or a DB."""
import importlib.util
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
import stat
import tempfile
import unittest
from unittest.mock import patch
import zipfile

MODULE = Path(__file__).resolve().parents[1] / 'postgres-native-artifacts.py'
SPEC = importlib.util.spec_from_file_location('native_artifacts', MODULE)
artifacts = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(artifacts)


class ArtifactDecodeTests(unittest.TestCase):
    def archive(self, directory, entries):
        path = directory / 'input.zip'
        with zipfile.ZipFile(path, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
            for name, data, mode in entries:
                entry = zipfile.ZipInfo(name)
                entry.create_system = 3
                entry.external_attr = mode << 16
                archive.writestr(entry, data)
        return path

    def test_regular_payload_and_directory_are_copied_without_zip_modes(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw).resolve()
            archive = self.archive(directory, [('payload/', b'', stat.S_IFDIR | 0o755),
                ('payload/binary', b'exact bytes', stat.S_IFREG | 0o755)])
            output = directory / 'out'
            output.mkdir()
            artifacts.extract(archive, output)
            self.assertEqual((output / 'payload/binary').read_bytes(), b'exact bytes')
            self.assertFalse((output / 'payload/binary').stat().st_mode & 0o111)

    def test_path_escape_and_aliases_are_rejected(self):
        for name in ('../escape', '/absolute', r'payload\escape', 'payload/../escape',
                     './payload', 'payload//file', 'C:/file', 'payload/./file'):
            with self.subTest(name=name), tempfile.TemporaryDirectory() as raw:
                directory = Path(raw).resolve()
                archive = self.archive(directory, [(name, b'bad', stat.S_IFREG | 0o644)])
                output = directory / 'out'
                output.mkdir()
                with self.assertRaises(ValueError):
                    artifacts.extract(archive, output)

    def test_symlink_member_is_rejected(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw).resolve()
            archive = self.archive(directory, [('link', b'/outside', stat.S_IFLNK | 0o777)])
            output = directory / 'out'
            output.mkdir()
            with self.assertRaises(ValueError):
                artifacts.extract(archive, output)

    def test_duplicate_file_directory_alias_is_rejected(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw).resolve()
            archive = self.archive(directory, [('same/', b'', stat.S_IFDIR | 0o755),
                                              ('same', b'bad', stat.S_IFREG | 0o644)])
            output = directory / 'out'
            output.mkdir()
            with self.assertRaises(ValueError):
                artifacts.extract(archive, output)

    def test_expanded_limit_is_checked_before_reading_member(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw).resolve()
            archive = self.archive(directory, [('large', b'12345', stat.S_IFREG | 0o644)])
            output = directory / 'out'
            output.mkdir()
            with patch.object(artifacts, 'LIMIT', 4), self.assertRaises(ValueError):
                artifacts.extract(archive, output)
            self.assertEqual(list(output.iterdir()), [])

    def test_indexed_bytes_and_complete_closure_are_required(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw).resolve()
            artifacts.copy(b'one', directory / 'one')
            expected = artifacts.index(directory)
            artifacts.save(directory / 'manifest.json', {'files': expected})
            self.assertEqual(artifacts.verify_files(directory, {'files': expected}), expected)
            (directory / 'one').write_bytes(b'two')
            with self.assertRaises(ValueError):
                artifacts.verify_files(directory, {'files': expected})
            (directory / 'one').write_bytes(b'one')
            artifacts.copy(b'extra', directory / 'extra')
            with self.assertRaises(ValueError):
                artifacts.verify_files(directory, {'files': expected})


class CommandLifecycleTests(unittest.TestCase):
    def run_child(self, directory, label, program, *, timeout=0.25, interrupt=False):
        ready = directory / 'ready'
        first = True
        original_wait = subprocess.Popen.wait

        def observed_wait(process, *args, **kwargs):
            nonlocal first
            if first:
                first = False
                deadline = time.monotonic() + 4
                while not ready.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                if interrupt:
                    # Inject only the interruption at this actual child's first
                    # wait; all retirement waits and return codes remain real.
                    raise KeyboardInterrupt()
            return original_wait(process, *args, **kwargs)

        with patch.object(artifacts, 'ROOT', directory), \
             patch.object(artifacts, 'DIRECT_CHILD_GRACE', 0.2), \
             patch.object(artifacts, 'PARTIAL_PREFIX_LIMIT', 128), \
             patch.object(subprocess.Popen, 'wait', observed_wait):
            with self.assertRaises(ValueError):
                artifacts.command(directory / 'checks', label,
                                  [sys.executable, '-u', '-c', program], timeout=timeout)
        self.assertTrue(ready.exists(), 'real child reached its ready marker before the actual wait')
        receipt = artifacts.load(directory / 'checks' / (label + '.receipt.json'))
        self.assertTrue(receipt['direct_child_reaped'])
        self.assertTrue(receipt['direct_child_retirement']['direct_child_reaped'])
        self.assertFalse(receipt['descendants_empty_proved'])
        self.assertTrue(receipt['descendant_state_unknown'])
        self.assertFalse(receipt['command_succeeded'])
        self.assertTrue(receipt['partial_unqualified'])
        with self.assertRaises(ChildProcessError):
            os.waitpid(receipt['pid'], os.WNOHANG)
        for row in receipt['output_streams'].values():
            raw = (directory / row['path']).read_bytes()
            self.assertLessEqual(len(raw), 128)
            self.assertEqual((len(raw), artifacts.digest(raw)), (row['bytes'], row['sha256']))
            self.assertTrue(row['partial'])
            self.assertFalse(row['saved_is_raw_complete'])
        self.assertTrue((directory / 'checks' / (label + '.unsealed.json')).exists())
        return receipt

    def test_normal_command_keeps_exact_stable_raw_streams(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw).resolve()
            with patch.object(artifacts, 'ROOT', directory):
                output = artifacts.command(directory / 'checks', 'normal',
                    [sys.executable, '-c', 'import sys; sys.stdout.write("actual stdout"); sys.stderr.write("actual stderr")'])
            self.assertEqual(output, b'actual stdout')
            receipt = artifacts.load(directory / 'checks/normal.receipt.json')
            self.assertEqual(receipt['exit_code'], 0)
            self.assertTrue(receipt['direct_child_reaped'])
            self.assertTrue(receipt['command_succeeded'])
            self.assertFalse(receipt['partial_unqualified'])
            for name, expected in (('stdout', b'actual stdout'), ('stderr', b'actual stderr')):
                row = receipt['output_streams'][name]
                self.assertEqual((directory / row['path']).read_bytes(), expected)
                self.assertEqual(row['sha256'], artifacts.digest(expected))
                self.assertTrue(row['saved_is_raw_complete'])
            with self.assertRaises(ChildProcessError):
                os.waitpid(receipt['pid'], os.WNOHANG)

    def test_real_timeout_terminates_and_waits_the_direct_child(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw).resolve()
            program = ('import pathlib,time; print("prefix"*1000, flush=True); '
                       + 'pathlib.Path(' + repr(str(directory / 'ready')) + ').write_text("ready"); time.sleep(30)')
            receipt = self.run_child(directory, 'timeout', program)
            self.assertTrue(receipt['timed_out'])
            self.assertFalse(receipt['interrupted'])
            self.assertEqual(receipt['exit_code'], -signal.SIGTERM)

    def test_real_timeout_escalates_and_actually_waits_after_kill(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw).resolve()
            program = ('import pathlib,signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); '
                       + 'pathlib.Path(' + repr(str(directory / 'ready')) + ').write_text("ready"); time.sleep(30)')
            receipt = self.run_child(directory, 'kill', program)
            self.assertEqual(receipt['exit_code'], -signal.SIGKILL)
            operations = [row['operation'] for row in receipt['direct_child_retirement']['actions']]
            self.assertEqual(operations, ['terminate', 'wait', 'kill', 'wait'])

    def test_interrupted_first_wait_still_retires_the_real_child(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw).resolve()
            program = ('import pathlib,time; pathlib.Path('
                       + repr(str(directory / 'ready')) + ').write_text("ready"); time.sleep(30)')
            receipt = self.run_child(directory, 'interrupt', program, interrupt=True)
            self.assertFalse(receipt['timed_out'])
            self.assertTrue(receipt['interrupted'])
            self.assertEqual(receipt['exit_code'], -signal.SIGTERM)

    def test_unknown_descendant_can_grow_live_file_but_not_partial_snapshot(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw).resolve()
            descendant = ('import pathlib,time\n'
                + 'pathlib.Path(' + repr(str(directory / 'ready')) + ').write_text("ready")\n'
                + 'for _ in range(60):\n'
                + '    print("descendant"*100,flush=True)\n'
                + '    time.sleep(0.05)\n'
                + 'pathlib.Path(' + repr(str(directory / 'done')) + ').write_text("done")\n')
            parent = ('import subprocess,sys,time; subprocess.Popen([sys.executable,"-u","-c",'
                      + repr(descendant) + ']); time.sleep(30)')
            receipt = self.run_child(directory, 'descendant', parent)
            row = receipt['output_streams']['stdout']
            snapshot, live = directory / row['path'], directory / row['live_path_not_uploaded']
            frozen = snapshot.read_bytes()
            before = live.stat().st_size
            deadline = time.monotonic() + 5
            while not (directory / 'done').exists() and time.monotonic() < deadline:
                time.sleep(0.02)
            self.assertTrue((directory / 'done').exists(), 'finite descendant finished writing its marker')
            self.assertGreater(live.stat().st_size, before)
            self.assertEqual(snapshot.read_bytes(), frozen)
            self.assertEqual(artifacts.digest(frozen), row['sha256'])
            self.assertTrue(str(live).endswith('.live'))
            self.assertFalse(any(name.endswith('.live') for name in artifacts.paths(directory / 'checks')))
            # This marker proves continued output, not descendant wait/reap or an empty tree.
            self.assertFalse(receipt['descendants_empty_proved'])

    def test_api_tokens_are_local_and_legacy_style_subprocesses_inherit_none(self):
        with tempfile.TemporaryDirectory() as raw:
            directory = Path(raw).resolve()
            program = 'import os; print(any(key in os.environ for key in ("GH_TOKEN", "GITHUB_TOKEN")))'
            with patch.dict(os.environ, {'GH_TOKEN': 'synthetic-test-only', 'GITHUB_TOKEN': 'synthetic-test-only'}):
                dedicated = artifacts.api_environment()
                self.assertNotIn('GH_TOKEN', os.environ)
                self.assertNotIn('GITHUB_TOKEN', os.environ)
                self.assertIn('GH_TOKEN', dedicated)
                self.assertEqual(subprocess.check_output([sys.executable, '-c', program]).strip(), b'False')
                with patch.object(artifacts, 'ROOT', directory):
                    self.assertEqual(artifacts.command(directory / 'checks', 'no-token',
                                     [sys.executable, '-c', program], env=dedicated).strip(), b'False')
            # No gh/API request is mocked or run by this credential-flow test.


if __name__ == '__main__':
    unittest.main()
