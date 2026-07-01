import io
import os
import tarfile
import zipfile

import pytest

from dcicutils.tmpfile_utils import temporary_directory
from dcicutils.zip_utils import (
    unpack_zip_file_to_temporary_directory, unpack_tar_file_to_temporary_directory,
)


def _make_zip_with_member(zip_path: str, member_name: str, content: bytes = b"data") -> None:
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr(member_name, content)


def _make_tar_with_member(tar_path: str, member_name: str, content: bytes = b"data") -> None:
    with tarfile.open(tar_path, "w") as tf:
        info = tarfile.TarInfo(name=member_name)
        info.size = len(content)
        tf.addfile(info, io.BytesIO(content))


def test_unpack_zip_file_to_temporary_directory_rejects_path_traversal():
    # Zip Slip: a malicious archive entry named with ../ segments must not be extracted
    # outside of the target directory, even though zipfile.extractall would otherwise allow it.
    with temporary_directory() as work_dir:
        zip_path = os.path.join(work_dir, "evil.zip")
        _make_zip_with_member(zip_path, "../../../tmp/dcicutils_zip_slip_poc.txt", b"pwned")
        with pytest.raises(ValueError):
            with unpack_zip_file_to_temporary_directory(zip_path) as _tmp_dir:
                pass
        # The malicious path must never have landed on disk anywhere.
        assert not os.path.exists("/tmp/dcicutils_zip_slip_poc.txt")


def test_unpack_zip_file_to_temporary_directory_rejects_absolute_path():
    with temporary_directory() as work_dir:
        zip_path = os.path.join(work_dir, "evil_abs.zip")
        _make_zip_with_member(zip_path, "/tmp/dcicutils_zip_slip_abs_poc.txt", b"pwned")
        with pytest.raises(ValueError):
            with unpack_zip_file_to_temporary_directory(zip_path) as _tmp_dir:
                pass
        assert not os.path.exists("/tmp/dcicutils_zip_slip_abs_poc.txt")


def test_unpack_zip_file_to_temporary_directory_extracts_benign_archive():
    # Legitimate archives must still work normally after the guard is added.
    with temporary_directory() as work_dir:
        zip_path = os.path.join(work_dir, "benign.zip")
        _make_zip_with_member(zip_path, "subdir/hello.txt", b"hello world")
        with unpack_zip_file_to_temporary_directory(zip_path) as tmp_dir:
            extracted_path = os.path.join(tmp_dir, "subdir", "hello.txt")
            assert os.path.exists(extracted_path)
            with open(extracted_path, "rb") as f:
                assert f.read() == b"hello world"


def test_unpack_tar_file_to_temporary_directory_rejects_path_traversal():
    # Tar Slip: same attack, via tarfile.extractall.
    with temporary_directory() as work_dir:
        tar_path = os.path.join(work_dir, "evil.tar")
        _make_tar_with_member(tar_path, "../../../tmp/dcicutils_tar_slip_poc.txt", b"pwned")
        with pytest.raises(ValueError):
            with unpack_tar_file_to_temporary_directory(tar_path) as _tmp_dir:
                pass
        assert not os.path.exists("/tmp/dcicutils_tar_slip_poc.txt")


def test_unpack_tar_file_to_temporary_directory_extracts_benign_archive():
    with temporary_directory() as work_dir:
        tar_path = os.path.join(work_dir, "benign.tar")
        _make_tar_with_member(tar_path, "subdir/hello.txt", b"hello world")
        with unpack_tar_file_to_temporary_directory(tar_path) as tmp_dir:
            extracted_path = os.path.join(tmp_dir, "subdir", "hello.txt")
            assert os.path.exists(extracted_path)
            with open(extracted_path, "rb") as f:
                assert f.read() == b"hello world"
