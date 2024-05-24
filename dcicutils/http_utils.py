from contextlib import contextmanager
import requests
from typing import Callable, Optional
from dcicutils.tmpfile_utils import temporary_file


@contextmanager
def download(url: str, suffix: Optional[str] = None, binary: bool = True,
             progress: Optional[Callable] = None) -> Optional[str]:
    """
    Context manager to download the given URL into a temporary file and yields the file
    path to it. An optional file suffix may be specified for this temporary file name.
    Defaults to binary file mode; if not desired then pass False as the binary argument.
    """
    with temporary_file(suffix=suffix) as file:
        download_to(url, file, binary=binary, progress=progress)
        yield file


def download_to(url: str, file: str, binary: bool = True, progress: Optional[Callable] = None) -> None:
    """
    Download the given URL into the given file. Defaults to binary
    file mode; if not desired then pass False as the binary argument.
    """
    if not callable(progress):
        progress = None
    response = requests.get(url, stream=True)
    if progress:
        nbytes = 0
        nbytes_total = None
        if isinstance(content_length := response.headers.get("Content-Length"), str) and content_length.isdigit():
            nbytes_total = int(content_length)
    with open(file, "wb" if binary is True else "w") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
            if progress:
                nbytes += len(chunk)
                progress(nbytes, nbytes_total)
