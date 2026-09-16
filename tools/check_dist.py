#!/usr/bin/env python3
"""Verify built sdist/wheel artifacts before they are installed or uploaded.

Run as::

    python tools/check_dist.py dist --expected-version 1.1.11

Checks, in order:

* exactly one wheel and one sdist are present;
* both file names carry the expected version;
* the wheel declares that version in its METADATA;
* the wheel ships only the sqlalchemy_drill packages -- in particular it must
  not ship the top-level ``test`` package, which would install into
  site-packages and shadow CPython's stdlib ``test``;
* the sdist carries the test suite, its non-Python fixtures and the
  requirements files, so a source release can be tested.

Only the standard library is used so this can run before anything is
installed.
"""
import argparse
import posixpath
import sys
import tarfile
import zipfile
from email.parser import Parser

EXPECTED_WHEEL_TOP_LEVEL = {'sqlalchemy_drill'}

REQUIRED_SDIST_PATHS = (
    'setup.py',
    'MANIFEST.in',
    'README.md',
    'LICENSE',
    'CHANGELOG.md',
    'requirements/common.txt',
    'requirements/test.txt',
    'test/test_sqlalchemy2_reflection.py',
    'tools/test_check_dist.py',
    'test/test_sadrill.py',
    'test/test_dbapi_compliance.py',
    'test/conftest.py',
    'test/__init__.py',
    'test/dbapi20.py',
    'ivy.xml',
    'resolve.sh',
    'test/drill-override.conf',
    'test/htpasswd',
    'test/test_odbc.ini',
    'tools/check_dist.py',
)


class CheckFailed(Exception):
    pass


def _fail(message):
    raise CheckFailed(message)


def _check_archive_paths(names):
    normalized = [posixpath.normpath(name.rstrip('/')) for name in names]
    if any(canonical != raw.rstrip('/') or raw.startswith('/') or "\\" in raw
           or '..' in raw.split('/') for raw, canonical in zip(names, normalized)):
        _fail('archive contains non-canonical or unsafe paths')
    if len(normalized) != len(set(normalized)):
        _fail('archive contains colliding extraction destinations')


def check_wheel(wheel_path, expected_version):
    with zipfile.ZipFile(wheel_path) as archive:
        names = archive.namelist()
        _check_archive_paths(names)
        if len(names) != len(set(names)):
            _fail('wheel contains duplicate paths')
        if any(name.startswith('/') or '..' in name.split('/') for name in names):
            _fail('wheel contains unsafe paths')

        dist_info = f'sqlalchemy_drill-{expected_version}.dist-info'
        metadata_name = f'{dist_info}/METADATA'
        if metadata_name not in names:
            _fail(f'{wheel_path.name} has no {metadata_name}; '
                  f'found {sorted(set(n.split("/")[0] for n in names))}')

        metadata = Parser().parsestr(
            archive.read(metadata_name).decode('utf-8'))
        if metadata.get_all('Name') != ['sqlalchemy_drill']:
            _fail('wheel METADATA must name sqlalchemy_drill exactly once')
        if metadata.get_all('Version') != [expected_version]:
            _fail(f'wheel METADATA Version is {metadata["Version"]!r}, '
                  f'expected {expected_version!r}')

        top_level = {
            name.split('/')[0] for name in names
            if not name.startswith(dist_info + "/")
        }
        if top_level != EXPECTED_WHEEL_TOP_LEVEL:
            _fail(f'wheel ships top-level {sorted(top_level)}, '
                  f'expected {sorted(EXPECTED_WHEEL_TOP_LEVEL)}')

        # Belt and braces: catch a test module smuggled in under any path.
        leaked = [
            name for name in names
            if name == 'test' or name.startswith('test/')
            or '/test/' in name
        ]
        if leaked:
            _fail(f'wheel leaks test files: {leaked[:10]}')

    print(f'  wheel  {wheel_path.name}: version {expected_version}, '
          f'top-level {sorted(EXPECTED_WHEEL_TOP_LEVEL)}, no test package')


def check_sdist(sdist_path, expected_version):
    root = f'sqlalchemy_drill-{expected_version}'
    with tarfile.open(sdist_path) as archive:
        members = archive.getmembers()
        names = archive.getnames()
        _check_archive_paths(names)
        if len(names) != len(set(names)):
            _fail('sdist contains duplicate paths')
        for member in members:
            if (not member.isfile() and not member.isdir()) or '..' in member.name.split('/'):
                _fail(f'unsafe sdist member: {member.name}')
        metadata_path = f'{root}/PKG-INFO'
        if metadata_path not in names or not archive.getmember(metadata_path).isfile():
            _fail('sdist has no regular root PKG-INFO')
        metadata = Parser().parsestr(archive.extractfile(metadata_path).read().decode('utf-8'))
        if (metadata.get_all('Name') != ['sqlalchemy_drill']
                or metadata.get_all('Version') != [expected_version]):
            _fail('sdist PKG-INFO name/version mismatch')
        for path in REQUIRED_SDIST_PATHS:
            name = f'{root}/{path}'
            if name not in names or not archive.getmember(name).isfile():
                _fail(f'sdist missing regular file: {path}')

    roots = {name.split('/')[0] for name in names}
    if roots != {root}:
        _fail(f'sdist root is {sorted(roots)}, expected {[root]}')

    present = {posixpath.relpath(name, root) for name in names}
    missing = [path for path in REQUIRED_SDIST_PATHS if path not in present]
    if missing:
        _fail(f'sdist is missing {missing}')

    print(f'  sdist  {sdist_path.name}: root {root}, '
          f'{len(REQUIRED_SDIST_PATHS)} required paths present')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('dist_dir', help='directory holding the built artifacts')
    parser.add_argument('--expected-version', required=True)
    args = parser.parse_args(argv)

    # pathlib import is local so --help works on a broken tree.
    from pathlib import Path

    dist_dir = Path(args.dist_dir)
    if not dist_dir.is_dir():
        print(f'no such directory: {dist_dir}', file=sys.stderr)
        return 2

    artifacts = sorted(dist_dir.iterdir())
    print(f'checking {len(artifacts)} artifact(s) in {dist_dir} '
          f'against version {args.expected_version}')

    try:
        expected_names = {
            f'sqlalchemy_drill-{args.expected_version}-py3-none-any.whl',
            f'sqlalchemy_drill-{args.expected_version}.tar.gz',
        }
        if (len(artifacts) != 2 or {p.name for p in artifacts} != expected_names
                or any(p.is_symlink() or not p.is_file() for p in artifacts)):
            _fail(f'expected exactly two regular artifacts: {sorted(expected_names)}')
        wheel = dist_dir / f'sqlalchemy_drill-{args.expected_version}-py3-none-any.whl'
        sdist = dist_dir / f'sqlalchemy_drill-{args.expected_version}.tar.gz'

        check_wheel(wheel, args.expected_version)
        check_sdist(sdist, args.expected_version)
    except (CheckFailed, OSError, ValueError, tarfile.TarError, zipfile.BadZipFile) as error:
        print(f'ARTIFACT CHECK FAILED: {error}', file=sys.stderr)
        return 1

    print('artifact checks passed')
    return 0


if __name__ == '__main__':
    sys.exit(main())
