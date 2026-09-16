"""Adversarial tests for the release artifact allowlist (no build/server needed)."""
import importlib.util
import io
import tarfile
import zipfile
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "check_dist", Path(__file__).resolve().parents[1] / "tools" / "check_dist.py"
)
check_dist = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_dist)
VERSION = "1.1.11"
WHEEL = f"sqlalchemy_drill-{VERSION}-py3-none-any.whl"
SDIST = f"sqlalchemy_drill-{VERSION}.tar.gz"


def make_dist(path, wheel_version=VERSION, sdist_version=VERSION,
              wheel_name="sqlalchemy_drill", sdist_name="sqlalchemy_drill",
              leak_test=False, omit=None):
    metadata = f"Name: {wheel_name}\nVersion: {wheel_version}\n"
    with zipfile.ZipFile(path / WHEEL, "w") as archive:
        archive.writestr(f"sqlalchemy_drill-{VERSION}.dist-info/METADATA", metadata)
        archive.writestr("sqlalchemy_drill/__init__.py", "")
        if leak_test:
            archive.writestr("test/__init__.py", "")
    with tarfile.open(path / SDIST, "w:gz") as archive:
        for name in (*check_dist.REQUIRED_SDIST_PATHS, "PKG-INFO"):
            if name == omit:
                continue
            data = (f"Name: {sdist_name}\nVersion: {sdist_version}\n"
                    if name == "PKG-INFO" else "").encode()
            info = tarfile.TarInfo(f"sqlalchemy_drill-{VERSION}/{name}")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))


def check(path):
    return check_dist.main([str(path), "--expected-version", VERSION])


def test_valid_pair(tmp_path):
    make_dist(tmp_path)
    assert check(tmp_path) == 0


@pytest.mark.parametrize("extra", ["unchecked-9.9.9.zip", "hidden", "extra.whl", "extra.tar.gz"])
def test_reject_extra_file(tmp_path, extra):
    make_dist(tmp_path)
    (tmp_path / extra).write_bytes(b"unchecked")
    assert check(tmp_path) == 1


@pytest.mark.parametrize("name", [WHEEL, SDIST])
def test_reject_version_prefix_collision(tmp_path, name):
    make_dist(tmp_path)
    (tmp_path / name).rename(tmp_path / name.replace(VERSION, VERSION + "0"))
    assert check(tmp_path) == 1


@pytest.mark.parametrize("name", [WHEEL, SDIST])
def test_reject_symlink(tmp_path, name):
    make_dist(tmp_path)
    original = tmp_path / name
    original.unlink()
    original.symlink_to(__file__)
    assert check(tmp_path) == 1


def test_reject_directory(tmp_path):
    make_dist(tmp_path)
    (tmp_path / "extra").mkdir()
    assert check(tmp_path) == 1


@pytest.mark.parametrize("kwargs", [
    {"wheel_version": "1.1.110"}, {"sdist_version": "1.1.110"},
    {"wheel_name": "other"}, {"sdist_name": "other"}, {"leak_test": True},
])
def test_reject_archive_metadata_and_namespace(tmp_path, kwargs):
    make_dist(tmp_path, **kwargs)
    assert check(tmp_path) == 1


@pytest.mark.parametrize("name", [WHEEL, SDIST])
def test_reject_corrupt_archive(tmp_path, name):
    make_dist(tmp_path)
    (tmp_path / name).write_bytes(b"not an archive")
    assert check(tmp_path) == 1


@pytest.mark.parametrize("missing", ["test/dbapi20.py", "test/__init__.py", "ivy.xml", "resolve.sh"])
def test_reject_missing_source_test_dependency(tmp_path, missing):
    make_dist(tmp_path, omit=missing)
    assert check(tmp_path) == 1


@pytest.mark.parametrize("separator", ["/./", "//"])
@pytest.mark.parametrize("kind", ["wheel", "sdist"])
def test_reject_normalized_extraction_collisions(tmp_path, separator, kind):
    make_dist(tmp_path)
    if kind == "wheel":
        with zipfile.ZipFile(tmp_path / WHEEL, "a") as archive:
            archive.writestr("sqlalchemy_drill" + separator + "__init__.py", "overwrite")
    else:
        path = tmp_path / SDIST
        with tarfile.open(path) as archive:
            members = [(info, archive.extractfile(info).read())
                       for info in archive.getmembers()]
        with tarfile.open(path, "w:gz") as archive:
            for info, data in members:
                archive.addfile(info, io.BytesIO(data))
            alias = tarfile.TarInfo(f"sqlalchemy_drill-{VERSION}/test{separator}dbapi20.py")
            alias.size = 9
            archive.addfile(alias, io.BytesIO(b"overwrite"))
    assert check(tmp_path) == 1
