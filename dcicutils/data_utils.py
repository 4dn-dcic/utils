"""
Tools for handling data files, formats, etc.
"""

import gzip
import io
import os
import random

from .misc_utils import remove_suffix, check_true


def gunzip_content(content):
    """ Helper that will gunzip content """
    f_in = io.BytesIO()
    f_in.write(content)
    f_in.seek(0)
    with gzip.GzipFile(fileobj=f_in, mode='rb') as f:
        gunzipped_content = f.read()
    return gunzipped_content.decode('utf-8')


def generate_sample_fastq_content(num=10, length=10):
    """ Creates (pseudo)randomly generated fastq content."""

    content = ''
    bases = 'ACTG'

    for i in range(num):
        content += '@SEQUENCE{} length={}\n'.format(i, length)
        content += ''.join(random.choice(bases) for i in range(length)) + '\n'
        content += '+\n'
        content += 'I' * length + '\n'

    return content


FASTQ_SUFFIXES = [".fastq", ".fq"]


def generate_sample_fastq_file(filename, num=10, length=10, compressed=None):
    """
    Creates a new fastq file with the given name, containing (pseudo)randomly generated content.

    Example usage:

        generate_sample_fastq_file('fastq_sample.fastq.gz', num=25, length=50)
           creates a new fastq file with 25 sequences, each of length 50.

        generate_sample_fastq_file('fastq_sample.fastq.gz')
           creates a new fastq file with default characteristics (10 sequences, each of length 10).

    Args:
        filename str: the name of a file to create
        num int: the number of random sequences (default 10)
        length int: the length of the random sequences (default 10)

    Returns:
        the filename

    """
    if compressed is None:
        if filename.endswith('.gz'):
            compressed = True
            filename = remove_suffix(".gz", filename)
        else:
            compressed = False
    _, ext = os.path.splitext(filename)
    if ext not in FASTQ_SUFFIXES:
        filename = filename + ".fastq"
    check_true(isinstance(compressed, bool), "compressed must be one of True, False, or None (for autoselection)")
    content = generate_sample_fastq_content(num=num, length=length)
    if compressed:
        filename += ".gz"
        with gzip.open(filename, 'w') as zipfile:
            zipfile.write(content.encode('ascii'))
    else:
        with io.open(filename, 'w') as outfile:
            outfile.write(content)
    return filename
