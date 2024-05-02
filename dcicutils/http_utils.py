from contextlib import contextmanager
import requests
from typing import Callable, Optional
from dcicutils.tmpfile_utils import temporary_file


@contextmanager
def download(url: str, suffix: Optional[str] = None, binary: bool = True,
             progress: Optional[Callable] = None) -> Optional[str]:
    """
    Context manager to ownload the given URL into a temporary file and yields the file
    path to it. An optional file suffix may be specified. Defaults to binary file mode;
    if this is not desired then pass False as the binary argument.
    """
    if not callable(progress):
        progress = None
    with temporary_file(suffix=suffix) as file:
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
        yield file
