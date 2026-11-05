import importlib.util
import json
import os
import stat
import sys
import tempfile
import unittest
import unittest.mock

spec = importlib.util.spec_from_file_location(
    'image_paths',
    os.path.join(os.path.dirname(__file__), '..', 'src', 'controller', 'image_paths.py'),
)
image_paths = importlib.util.module_from_spec(spec)
spec.loader.exec_module(image_paths)


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


EXPECTED = {
    '/a', '/a/b', '/a/file', '/a/b/hard',
    '/empty', '/dangling', '/external-link',
}


class ImagePathsTest(unittest.TestCase):
    def test_inventory_covers_own_paths_only(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as external:
            build_fixture(root, external)
            paths = image_paths.image_paths(root)
            self.assertEqual(set(paths), EXPECTED)
            self.assertEqual(paths, sorted(paths))

    def test_scan_errors_propagate(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as external:
            build_fixture(root, external)
            with unittest.mock.patch.object(
                image_paths.os, 'scandir', side_effect=PermissionError('denied')
            ):
                with self.assertRaises(PermissionError):
                    image_paths.image_paths(root)

    def test_main_writes_versioned_output(self):
        with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as external, \
                tempfile.TemporaryDirectory() as out:
            build_fixture(root, external)
            output = os.path.join(out, 'base-paths.json')
            argv = ['image_paths.py', root, output]
            with unittest.mock.patch.object(sys, 'argv', argv):
                image_paths.main()
            with open(output, encoding='utf-8') as file:
                inventory = json.load(file)
            self.assertEqual(inventory['version'], 1)
            self.assertEqual(set(inventory['paths']), EXPECTED)
            mode = stat.S_IMODE(os.stat(output).st_mode)
            self.assertEqual(mode, 0o600)
            parent_mode = stat.S_IMODE(os.stat(out).st_mode)
            self.assertEqual(parent_mode, 0o700)
            self.assertFalse(os.path.exists(output + '.tmp'))


if __name__ == '__main__':
    unittest.main()
