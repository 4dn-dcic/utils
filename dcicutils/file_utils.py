import glob
import os
import pathlib
from datetime import datetime
import random
import string
from tempfile import gettempdir as get_temporary_directory
from typing import List, Optional, Union
from uuid import uuid4 as uuid


def search_for_file(file: str,
                    location: Union[str, Optional[List[str]]] = None,
                    recursive: bool = False,
                    single: bool = False) -> Union[List[str], Optional[str]]:
    """
    Searches for the existence of the given file name, first directly in the given directory or list
    of directories, if specified, and if not then just in the current (working) directory; if the
    given recursive flag is True then also searches all sub-directories of these directories;
    returns the full path name to the file if found. If the single flag is True then just the
    first file which is found is returns (as a string), or None if none; if the single flag
    is False, then all matched files are returned in a list, or and empty list if none.
    """
    if file and isinstance(file, (str, pathlib.PosixPath)):
        if os.path.isabs(file):
            if os.path.exists(file):
                return file if single else [file]
            return None if single else []
        files_found = []
        if not location:
            location = ["."]
        elif isinstance(location, (str, pathlib.PosixPath)):
            location = [location]
        elif not isinstance(location, list):
            location = []
        for directory in location:
            if not directory:
                continue
            if isinstance(directory, (str, pathlib.PosixPath)) and os.path.exists(os.path.join(directory, file)):
                file_found = os.path.abspath(os.path.normpath(os.path.join(directory, file)))
                if single:
                    return file_found
                if file_found not in files_found:
                    files_found.append(file_found)
        if recursive:
            for directory in location:
                if not directory:
                    continue
                if not directory.endswith("/**") and not file.startswith("**/"):
                    path = f"{directory}/**/{file}"
                else:
                    path = f"{directory}/{file}"
                files = glob.glob(path, recursive=recursive)
                if files:
                    for file_found in files:
                        file_found = os.path.abspath(file_found)
                        if single:
                            return file_found
                        if file_found not in files_found:
                            files_found.append(file_found)
        if files_found:
            return files_found[0] if single else files_found
        return None if single else []


def normalize_file_path(path: str, home_directory: bool = True) -> str:
    """
    Normalizes the given file path name and returns. Does things like remove multiple
    consecutive slashes and redundant/unnecessary parent paths; if the home_directory
    argument is True (the default) then also handles the special tilde home directory
    component/convention and uses this in the result if applicable.
    """
    if not isinstance(path, str) or not path:
        path = os.getcwd()
    path = os.path.normpath(path)
    home_directory = os.path.expanduser("~") if home_directory is True else None
    if home_directory and path.startswith("~"):
        path = os.path.join(home_directory, path[2 if path.startswith("~/") else 1:])
    path = os.path.abspath(path)
    if home_directory and (os.name == "posix"):
        if path.startswith(home_directory) and path != home_directory:
            path = "~/" + pathlib.Path(path).relative_to(home_directory).as_posix()
    return path


def are_files_equal(filea: str, fileb: str) -> bool:
    """
    Returns True iff the contents of the two given files are exactly the same.
    """
    try:
        with open(filea, "rb") as fa:
            with open(fileb, "rb") as fb:
                chunk_size = 4096
                while True:
                    chunka = fa.read(chunk_size)
                    chunkb = fb.read(chunk_size)
                    if chunka != chunkb:
                        return False
                    if not chunka:
                        break
        return True
    except Exception:
        return False


def create_random_file(file: Optional[str] = None,
                       prefix: Optional[str] = None, suffix: Optional[str] = None,
                       nbytes: int = 1024, binary: bool = False, line_length: Optional[int] = None) -> str:
    if not isinstance(nbytes, int) or nbytes < 0:
        nbytes = 0
    if not isinstance(file, str) or not file:
        if not isinstance(prefix, str):
            prefix = ""
        if not isinstance(suffix, str):
            suffix = ""
        file = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}{str(uuid()).replace('-', '')}"
        file = os.path.join(get_temporary_directory(), file)
    with open(file, "wb" if binary is True else "w") as f:
        if binary is True:
            f.write(os.urandom(nbytes))
        else:
            if (not isinstance(line_length, int)) or (line_length < 1):
                line_length = 80
            line_length += 1
            nlines = nbytes // line_length
            nremainder = nbytes % line_length
            for n in range(nlines):
                f.write("".join(random.choices(string.ascii_letters + string.digits, k=line_length - 1)))
                f.write("\n")
            if nremainder > 1:
                f.write("".join(random.choices(string.ascii_letters + string.digits, k=nremainder - 1)))
            if nremainder > 0:
                f.write("\n")
    return file
