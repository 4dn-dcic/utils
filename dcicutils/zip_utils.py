from contextlib import contextmanager
import gzip
import os
import shutil
import tarfile
import tempfile
from typing import List, Optional, Union
import zipfile


@contextmanager
def unpack_zip_file_to_temporary_directory(file: str) -> str:
    with temporary_directory() as tmp_directory_name:
        with zipfile.ZipFile(file, "r") as zipf:
            zipf.extractall(tmp_directory_name)
        yield tmp_directory_name


@contextmanager
def unpack_tar_file_to_temporary_directory(file: str) -> str:
    with temporary_directory() as tmp_directory_name:
        with tarfile.open(file, "r") as tarf:
            tarf.extractall(tmp_directory_name)
        yield tmp_directory_name


def unpack_files(file: str, suffixes: Optional[List[str]] = None) -> Optional[str]:
    unpack_file_to_tmp_directory = {
        ".tar": unpack_tar_file_to_temporary_directory,
        ".zip": unpack_zip_file_to_temporary_directory
    }.get(file[dot:]) if (dot := file.rfind(".")) > 0 else None
    if unpack_file_to_tmp_directory is not None:
        with unpack_file_to_tmp_directory(file) as tmp_directory_name:
            for directory, _, files in os.walk(tmp_directory_name):  # Ignore "." prefixed files.
                for file in [file for file in files if not file.startswith(".")]:
                    if not suffixes or any(file.endswith(suffix) for suffix in suffixes):
                        yield os.path.join(directory, file)


@contextmanager
def unpack_gz_file_to_temporary_file(file: str, suffix: Optional[str] = None) -> str:
    if (gz := file.endswith(".gz")) or file.endswith(".tgz"):  # The .tgz suffix is simply short for .tar.gz.
        with temporary_file(name=os.path.basename(file[:-3] if gz else file[:-4] + ".tar")) as tmp_file_name:
            with open(tmp_file_name, "wb") as outputf:
                with gzip.open(file, "rb") as inputf:
                    outputf.write(inputf.read())
                    outputf.close()
                    yield tmp_file_name


@contextmanager
def temporary_directory() -> str:
    try:
        with tempfile.TemporaryDirectory() as tmp_directory_name:
            yield tmp_directory_name
    finally:
        remove_temporary_directory(tmp_directory_name)


@contextmanager
def temporary_file(name: Optional[str] = None, suffix: Optional[str] = None,
                   content: Optional[Union[str, bytes, List[str]]] = None) -> str:
    with temporary_directory() as tmp_directory_name:
        tmp_file_name = os.path.join(tmp_directory_name, name or tempfile.mktemp(dir="")) + (suffix or "")
        if content is not None:
            with open(tmp_file_name, "wb" if isinstance(content, bytes) else "w") as tmp_file:
                tmp_file.write("\n".join(content) if isinstance(content, list) else content)
        yield tmp_file_name


def remove_temporary_directory(tmp_directory_name: str) -> None:
    def is_temporary_directory(path: str) -> bool:
        try:
            tmpdir = tempfile.gettempdir()
            return os.path.commonpath([path, tmpdir]) == tmpdir and os.path.exists(path) and os.path.isdir(path)
        except Exception:
            return False
    if is_temporary_directory(tmp_directory_name):  # Guard against errant deletion.
        shutil.rmtree(tmp_directory_name)
