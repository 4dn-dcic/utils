import io
import os
import tempfile

from dcicutils.data_utils import gunzip_content, generate_sample_fastq_file, generate_sample_fastq_content
from dcicutils.misc_utils import ignored
from dcicutils.qa_utils import NotReallyRandom
from unittest import mock
from .conftest_settings import TEST_DIR


def test_gunzip_content():

    uncompressed_filename = os.path.join(TEST_DIR, "data_files/some-data.txt")
    compressed_filename = os.path.join(TEST_DIR, "data_files/some-data.txt.gz")

    with io.open(uncompressed_filename, 'r') as fp:
        text_content = fp.read()

    with io.open(compressed_filename, 'rb') as fp:
        binary_content = fp.read()

    assert gunzip_content(content=binary_content) == text_content


# This value is chosen by assuming that random.choice will return a predictable series of numbers
# starting from option 0 and and working upward, so random.choice('ATCG') repeats as ACTGACTGACTG...
# So this is what generate_sample_fastq_content(2, 10) will generate, we hope:

SAMPLE_CONTENT_2x10 = (
    "@SEQUENCE0 length=10\n"
    "ACTGACTGAC\n"
    "+\n"
    "IIIIIIIIII\n"
    "@SEQUENCE1 length=10\n"
    "TGACTGACTG\n"
    "+\n"
    "IIIIIIIIII\n"
)


def test_generate_sample_fastq_content():

    with mock.patch("random.choice", NotReallyRandom().choice):
        content = generate_sample_fastq_content(num=2, length=10)
        assert content == SAMPLE_CONTENT_2x10


def test_generate_sample_fastq_file():

    def check_it(filename, num=10, length=10, compressed=None, expect_suffix=None):
        created_filename = None
        try:
            created_filename = generate_sample_fastq_file(filename, num=num, length=length, compressed=compressed)
            if expect_suffix:
                assert created_filename == filename + expect_suffix
            else:
                assert created_filename == filename
            return os.path.getsize(created_filename)
        finally:
            if os.path.exists(filename):
                os.remove(filename)
            if created_filename and os.path.exists(created_filename):
                os.remove(created_filename)

    size1a = check_it(tempfile.mktemp(suffix=".fastq"))
    size1b = check_it(tempfile.mktemp(suffix=".fastq.gz"))
    assert size1b < size1a

    size2a = check_it(tempfile.mktemp(), expect_suffix=".fastq")
    size2b = check_it(tempfile.mktemp(), compressed=True, expect_suffix=".fastq.gz")
    assert size2b < size2a

    assert size1a == size2a
    # Not sure if the files encode their uncompressed name, so might differ a little.
    # assert size1b == size2b

    size3a = check_it(tempfile.mktemp(suffix=".fastq"), length=10)
    size3b = check_it(tempfile.mktemp(suffix=".fastq"), length=20)
    size3c = check_it(tempfile.mktemp(suffix=".fastq"), length=30)

    diff_3ab = size3b - size3a
    diff_3bc = size3c - size3b
    assert diff_3ab == diff_3bc

    size4a = check_it(tempfile.mktemp(suffix=".fq"), length=10)
    size4b = check_it(tempfile.mktemp(suffix=".fq"), length=20)
    size4c = check_it(tempfile.mktemp(suffix=".fq"), length=30)

    diff_4ab = size4b - size4a
    diff_4bc = size4c - size4b
    assert diff_4ab == diff_4bc

    size5a = check_it(tempfile.mktemp(suffix=".fastq"), num=10)
    size5b = check_it(tempfile.mktemp(suffix=".fastq"), num=20)
    size5c = check_it(tempfile.mktemp(suffix=".fastq"), num=30)

    diff_5ab = size5b - size5a
    diff_5bc = size5c - size5b
    assert diff_5ab == diff_5bc


def test_generate_sample_fastq_gzip_content_with_gunzip():

    with mock.patch("random.choice", NotReallyRandom().choice):

        created_filename = generate_sample_fastq_file(tempfile.mktemp(suffix='.fq.gz'), num=2, length=10)
        with io.open(created_filename, 'rb') as fp:
            binary_content = fp.read()
        unzipped = gunzip_content(content=binary_content)
        assert unzipped == SAMPLE_CONTENT_2x10
