from contextlib import contextmanager
from dcicutils.tmpfile_utils import temporary_directory, temporary_file
import gzip
import os
import shutil
import tarfile
import tempfile
from typing import List, Optional
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


def extract_file_from_zip(zip_file: str, file_to_extract: str,
                          destination_file: str, raise_exception: bool = True) -> bool:
    """
    Extracts from the given zip file, the given file to extract, writing it to the
    given destination file. Returns True if all is well, otherwise False, or if the
    raise_exception argument is True (the default), then raises and exception on error.
    """
    try:
        if not (destination_directory := os.path.dirname(destination_file)):
            destination_directory = os.getcwd()
            destination_file = os.path.join(destination_directory, destination_file)
        with tempfile.TemporaryDirectory() as tmp_directory_name:
            with zipfile.ZipFile(zip_file, "r") as zipf:
                if file_to_extract not in zipf.namelist():
                    return False
                zipf.extract(file_to_extract, path=tmp_directory_name)
                os.makedirs(destination_directory, exist_ok=True)
                shutil.move(os.path.join(tmp_directory_name, file_to_extract), destination_file)
            return True
    except Exception as e:
        if raise_exception:
            raise e
    return False
