from contextlib import contextmanager
from datetime import datetime
import os
import shutil
import tempfile
from uuid import uuid4 as uuid
from typing import List, Optional, Union
from dcicutils.file_utils import create_random_file


@contextmanager
def temporary_directory() -> str:
    try:
        with tempfile.TemporaryDirectory() as tmp_directory_name:
            yield tmp_directory_name
    finally:
        remove_temporary_directory(tmp_directory_name)


@contextmanager
def temporary_file(name: Optional[str] = None, prefix: Optional[str] = None, suffix: Optional[str] = None,
                   content: Optional[Union[str, bytes, List[str]]] = None) -> str:
    with temporary_directory() as tmp_directory_name:
        tmp_file_name = f"{prefix or ''}{name or tempfile.mktemp(dir='')}{suffix or ''}"
        tmp_file_path = os.path.join(tmp_directory_name, tmp_file_name)
        with open(tmp_file_path, "wb" if isinstance(content, bytes) else "w") as tmp_file:
            if content is not None:
                tmp_file.write("\n".join(content) if isinstance(content, list) else content)
        yield tmp_file_path


def create_temporary_file_name(prefix: Optional[str] = None, suffix: Optional[str] = None) -> str:
    """
    Generates and returns the full path to file within the system temporary directory.
    """
    random_string = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}{str(uuid()).replace('-', '')}"
    tmp_file_name = f"{prefix or ''}{random_string}{suffix or ''}"
    return os.path.join(tempfile.gettempdir(), tmp_file_name)


@contextmanager
def temporary_random_file(prefix: Optional[str] = None, suffix: Optional[str] = None,
                          nbytes: int = 1024, binary: bool = False, line_length: Optional[int] = None) -> str:
    with temporary_file(prefix=prefix, suffix=suffix) as tmp_file_path:
        create_random_file(tmp_file_path, nbytes=nbytes, binary=binary, line_length=line_length)
        yield tmp_file_path


def remove_temporary_directory(tmp_directory_name: str) -> None:
    """
    Removes the given directory, recursively; but ONLY if it is (somewhere) within the system temporary directory.
    """
    if is_temporary_directory(tmp_directory_name):  # Guard against errant deletion.
        shutil.rmtree(tmp_directory_name)


def remove_temporary_file(tmp_file_name: str) -> bool:
    """
    Removes the given file; but ONLY if it is (somewhere) within the system temporary directory.
    """
    try:
        tmpdir = tempfile.gettempdir()
        if (os.path.commonpath([tmpdir, tmp_file_name]) == tmpdir) and os.path.isfile(tmp_file_name):
            os.remove(tmp_file_name)
            return True
        return False
    except Exception:
        return False


def is_temporary_directory(path: str) -> bool:
    try:
        tmpdir = tempfile.gettempdir()
        return os.path.commonpath([path, tmpdir]) == tmpdir and os.path.exists(path) and os.path.isdir(path)
    except Exception:
        return False
