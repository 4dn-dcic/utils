import glob
import os
import pathlib
from typing import List, Optional, Union


def search_for_file(file: str,
                    location: Union[str, Optional[List[str]]] = None,
                    multiple: bool = True,
                    recursive: bool = False) -> Union[List[str], Optional[str]]:
    """
    Searches for the actual existence of the given file name, first in the given directory of list
    of directories, if specified, and if not then just in the current (working) directory; if the
    given recursive flag is True then also searches all sub-directories of these directories;
    returns the full path name to the file if found, otherwise None is returned.
    """
    if file and isinstance(file, (str, pathlib.PosixPath)):
        if os.path.isabs(file):
            if os.path.exists(file):
                return file if not multiple else [file]
            return None if not multiple else []
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
                if not multiple:
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
                        if not multiple:
                            return file_found
                        if file_found not in files_found:
                            files_found.append(file_found)
        if files_found:
            return files_found[0] if not multiple else files_found
        return None if not multiple else []
