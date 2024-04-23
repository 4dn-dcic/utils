from contextlib import contextmanager
import requests
from typing import Optional
from dcicutils.tmpfile_utils import temporary_file


@contextmanager
def download(url: str, suffix: Optional[str] = None, binary: bool = True) -> Optional[str]:
    with temporary_file(suffix=suffix) as file:
        response = requests.get(url, stream=True)
        with open(file, "wb" if binary is True else "w") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        yield file
