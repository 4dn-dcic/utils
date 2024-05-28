import glob
import hashlib
import io
import os
import pathlib
from datetime import datetime
import random
import string
from tempfile import gettempdir as get_temporary_directory
from typing import List, Optional, Union
from uuid import uuid4 as uuid

HOME_DIRECTORY = str(pathlib.Path().home())


def search_for_file(file: str,
                    location: Union[str, pathlib.PosixPath, Optional[List[Union[str, pathlib.PosixPath]]]] = None,
                    recursive: bool = False,
                    single: bool = False,
                    order: bool = True) -> Union[List[str], Optional[str]]:
    """
    Searches for the existence of the given file name, first directly in the given directory or list
    of directories, if specified, and if not then just in the current (working) directory; if the
    given recursive flag is True then also searches all sub-directories of these directories;
    returns the full path name to the file if found. If the single flag is True then just the
    first file which is found is returns (as a string), or None if none; if the single flag
    is False, then all matched files are returned in a list, or and empty list if none.
    """
    def order_by_fewest_number_of_paths_and_then_alphabetically(paths: List[str]) -> List[str]:
        def order_by(path: str):
            return len(path.split(os.path.sep)), path
        return sorted(paths, key=order_by)

    if not (file and isinstance(file, (str, pathlib.PosixPath))):
        return None if single is True else []
    if os.path.isabs(file):
        if os.path.exists(file):
            return str(file) if single is True else [str(file)]
        return None if single is True else []
    files_found = []
    if not location:
        location = ["."]
    elif isinstance(location, (str, pathlib.PosixPath)):
        location = [location]
    elif not isinstance(location, list):
        location = []
    location_pruned = []
    for directory in location:
        if not isinstance(directory, str):
            if not isinstance(directory, pathlib.PosixPath):
                continue
            directory = str(directory)
        if not (directory := directory.strip()):
            continue
        if os.path.isfile(directory := os.path.abspath(os.path.normpath(directory))):
            # Actually, allow a file rather then a directory; assume its parent directory was intended.
            if not (directory := os.path.dirname(directory)):
                continue
        if directory not in location_pruned:
            location_pruned.append(directory)
    location = location_pruned
    for directory in location:
        if os.path.exists(os.path.join(directory, file)):
            file_found = os.path.abspath(os.path.normpath(os.path.join(directory, file)))
            if single is True:
                return file_found
            if file_found not in files_found:
                files_found.append(file_found)
    if recursive is True:
        for directory in location:
            if not directory.endswith("/**") and not file.startswith("**/"):
                path = f"{directory}/**/{file}"
            else:
                path = f"{directory}/{file}"
            files = glob.glob(path, recursive=True if recursive is True else False)
            if files:
                for file_found in files:
                    file_found = os.path.abspath(file_found)
                    if single is True:
                        return file_found
                    if file_found not in files_found:
                        files_found.append(file_found)
    if single is True:
        return files_found[0] if files_found else None
    elif order is True:
        return order_by_fewest_number_of_paths_and_then_alphabetically(files_found)
    else:
        return files_found


def normalize_path(value: Union[str, pathlib.Path], absolute: bool = False, expand_home: Optional[bool] = None) -> str:
    """
    Normalizes the given path value and returns the result; does things like remove redundant
    consecutive directory separators and redundant parent paths. If the given absolute argument
    is True than converts the path to an absolute path. If the given expand_home argument is False
    and if the path can reasonably be represented with a home directory indicator (i.e. "~"), then
    converts it to such. If the expand_home argument is True and path starts with the home directory
    indicator (i.e. "~") then expands it to the actual (absolute) home path of the caller. If the
    given path value is not actually even a string (or pathlib.Path) then returns an empty string.
    """
    if isinstance(value, pathlib.Path):
        value = str(value)
    elif not isinstance(value, str):
        return ""
    if not (value := value.strip()) or not (value := os.path.normpath(value)):
        return ""
    if expand_home is True:
        value = os.path.expanduser(value)
    elif (expand_home is False) and (os.name == "posix"):
        if value.startswith(home := HOME_DIRECTORY + os.sep):
            value = "~/" + value[len(home):]
        elif value == HOME_DIRECTORY:
            value = "~"
    if absolute is True:
        value = os.path.abspath(value)
    return value


def get_file_size(file: str, raise_exception: bool = True) -> Optional[int]:
    try:
        return os.path.getsize(file) if isinstance(file, str) else None
    except Exception:
        if raise_exception is True:
            raise
        return None


def get_file_modified_datetime(file: str, raise_exception: bool = True) -> Optional[datetime]:
    try:
        return datetime.fromtimestamp(os.path.getmtime(file)) if isinstance(file, str) else None
    except Exception:
        if raise_exception is True:
            raise
        return None


def are_files_equal(filea: str, fileb: str, raise_exception: bool = True) -> bool:
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
        if raise_exception is True:
            raise
        return False


def compute_file_md5(file: str, raise_exception: bool = True) -> str:
    """
    Returns the md5 checksum for the given file.
    """
    if not isinstance(file, str):
        return ""
    try:
        md5 = hashlib.md5()
        with open(file, "rb") as file:
            for chunk in iter(lambda: file.read(4096), b""):
                md5.update(chunk)
        return md5.hexdigest()
    except Exception:
        if raise_exception is True:
            raise
        return ""


def compute_file_etag(file: str, raise_exception: bool = True) -> Optional[str]:
    """
    Returns the AWS S3 "etag" for the given file; this value is md5-like but
    not the same as a normal md5. We use this to compare that a file in S3
    appears to be the exact the same file as a local file.
    """
    try:
        with io.open(file, "rb") as f:
            return _compute_file_etag(f)
    except Exception:
        if raise_exception is True:
            raise
        return None


def _compute_file_etag(f: io.BufferedReader) -> str:
    # See: https://stackoverflow.com/questions/75723647/calculate-md5-from-aws-s3-etag
    MULTIPART_THRESHOLD = 8388608
    MULTIPART_CHUNKSIZE = 8388608
    # BUFFER_SIZE = 1048576
    # Verify some assumptions are correct
    # assert(MULTIPART_CHUNKSIZE >= MULTIPART_THRESHOLD)
    # assert((MULTIPART_THRESHOLD % BUFFER_SIZE) == 0)
    # assert((MULTIPART_CHUNKSIZE % BUFFER_SIZE) == 0)
    hash = hashlib.md5()
    read = 0
    chunks = None
    while True:
        # Read some from stdin, if we're at the end, stop reading
        bits = f.read(1048576)
        if len(bits) == 0:
            break
        read += len(bits)
        hash.update(bits)
        if chunks is None:
            # We're handling a multi-part upload, so switch to calculating
            # hashes of each chunk
            if read >= MULTIPART_THRESHOLD:
                chunks = b''
        if chunks is not None:
            if (read % MULTIPART_CHUNKSIZE) == 0:
                # Dont with a chunk, add it to the list of hashes to hash later
                chunks += hash.digest()
                hash = hashlib.md5()
    if chunks is None:
        # Normal upload, just output the MD5 hash
        etag = hash.hexdigest()
    else:
        # Multipart upload, need to output the hash of the hashes
        if (read % MULTIPART_CHUNKSIZE) != 0:
            # Add the last part if we have a partial chunk
            chunks += hash.digest()
        etag = hashlib.md5(chunks).hexdigest() + "-" + str(len(chunks) // 16)
    return etag


def create_random_file(file: Optional[str] = None, prefix: Optional[str] = None, suffix: Optional[str] = None,
                       nbytes: int = 1024, binary: bool = False, line_length: Optional[int] = None) -> str:
    """
    Write to the given file (name/path) some random content. If the given file is None then writes
    to a temporary file. In either case, returns the file written to. The of bytes written is 1024
    by default be can be specified with the nbytes argument; default to writing ASCII text but if
    the binary argument is True then writes binary data as well; if not binary the content is in
    lines of 80 characters each; use the line_length argumetn in this case to change the line length.
    """
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
