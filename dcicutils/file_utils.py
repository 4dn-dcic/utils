import glob
import os
import pathlib
from typing import List, Optional, Union


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
            if isinstance(directory, (str, pathlib.PosixPath)) and os.path.exists(os.path.join(directory, file)):
                file_found = os.path.abspath(os.path.normpath(os.path.join(directory, file)))
                if single:
                    return file_found
                if file_found not in files_found:
                    files_found.append(file_found)
        if recursive:
            for directory in location:
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
