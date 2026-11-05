import json
import os
import sys


def image_paths(root: str) -> list[str]:
    device = os.lstat(root).st_dev
    paths = []
    pending = [root]
    while pending:
        directory = pending.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                st = entry.stat(follow_symlinks=False)
                if st.st_dev != device:
                    continue
                paths.append('/' + os.path.relpath(entry.path, root))
                if entry.is_dir(follow_symlinks=False):
                    pending.append(entry.path)
    return sorted(paths)


def main() -> None:
    root, output = sys.argv[1:]
    os.chmod(os.path.dirname(output), 0o700)
    paths = image_paths(root)
    temporary = output + '.tmp'
    with open(temporary, 'w', encoding='utf-8') as file:
        os.fchmod(file.fileno(), 0o600)
        json.dump({'version': 1, 'paths': paths}, file)
    os.replace(temporary, output)


if __name__ == '__main__':
    main()
