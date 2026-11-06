import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / 'src' / 'controller' / 'image_paths.sh'
HEADER = b'devin-base-paths-v1\0'
UNUSUAL_NAME = 'space quote" slash\\ newline\n'
RAW_NAME = os.fsdecode(b'nonutf8-\xff')


def build_fixture(root, external):
    os.makedirs(os.path.join(root, 'a', 'b'))
    os.makedirs(os.path.join(root, 'empty'))
    with open(os.path.join(root, 'a', 'file'), 'w', encoding='utf-8') as file:
        file.write('x')
    os.link(os.path.join(root, 'a', 'file'), os.path.join(root, 'a', 'b', 'hard'))
    os.symlink('missing-target', os.path.join(root, 'dangling'))
    with open(os.path.join(external, 'outside'), 'w', encoding='utf-8') as file:
        file.write('y')
    os.symlink(external, os.path.join(root, 'external-link'))
    Path(root, UNUSUAL_NAME).touch()
    Path(root, RAW_NAME).touch()


EXPECTED = {
    '/a', '/a/b', '/a/file', '/a/b/hard',
    '/empty', '/dangling', '/external-link',
    '/' + UNUSUAL_NAME, '/' + RAW_NAME,
}


class ImagePathsTest(unittest.TestCase):
    def read_inventory(self, output):
        inventory = Path(output).read_bytes()
        self.assertTrue(inventory.startswith(HEADER))
        self.assertTrue(inventory.endswith(b'\0'))
        entries = inventory[len(HEADER):].split(b'\0')[:-1]
        self.assertTrue(all(entry.startswith(b'./') for entry in entries))
        return {os.fsdecode(entry[1:]) for entry in entries}

    def test_inventory_covers_own_paths_only(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as external, \
                tempfile.TemporaryDirectory() as out:
            build_fixture(root, external)
            output = os.path.join(out, 'base-paths.nul')
            subprocess.run(['sh', SCRIPT, root, output], check=True, capture_output=True)
            self.assertEqual(self.read_inventory(output), EXPECTED)

    def test_scan_errors_propagate(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as out, \
                tempfile.TemporaryDirectory() as bin_dir:
            find = Path(bin_dir, 'find')
            find.write_text("#!/bin/sh\nprintf './partial\\000'\nexit 1\n")
            find.chmod(0o755)
            output = os.path.join(out, 'base-paths.nul')
            result = subprocess.run(
                ['sh', SCRIPT, root, output],
                env={**os.environ, 'PATH': bin_dir + os.pathsep + os.environ['PATH']},
                capture_output=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(os.path.exists(output))
            self.assertFalse(os.path.exists(output + '.tmp'))

    def test_main_writes_versioned_output(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as external, \
                tempfile.TemporaryDirectory() as out:
            build_fixture(root, external)
            output = os.path.join(out, 'base-paths.nul')
            subprocess.run(['sh', SCRIPT, root, output], check=True, capture_output=True)
            self.assertEqual(self.read_inventory(output), EXPECTED)
            mode = stat.S_IMODE(os.stat(output).st_mode)
            self.assertEqual(mode, 0o600)
            parent_mode = stat.S_IMODE(os.stat(out).st_mode)
            self.assertEqual(parent_mode, 0o700)
            self.assertFalse(os.path.exists(output + '.tmp'))


if __name__ == '__main__':
    unittest.main()
