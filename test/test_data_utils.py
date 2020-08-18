import io
import pytest
import os
import tempfile

from dcicutils.data_utils import (
    gunzip_content, generate_sample_fastq_file, generate_sample_fastq_content, normalize_suffixes, FASTQ_SUFFIXES,
)
from dcicutils.qa_utils import NotReallyRandom, MockFileSystem
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


def test_normalize_suffixes():
    """Test normalize_suffixes, which assures a certain suffix is present, possibly also with a compression suffix."""

    assert normalize_suffixes("foo", FASTQ_SUFFIXES) == ("foo.fastq", False)
    assert normalize_suffixes("foo", FASTQ_SUFFIXES, compressed=None) == ("foo.fastq", False)
    assert normalize_suffixes("foo", FASTQ_SUFFIXES, compressed=False) == ("foo.fastq", False)
    assert normalize_suffixes("foo", FASTQ_SUFFIXES, compressed=True) == ("foo.fastq.gz", True)

    assert normalize_suffixes("foo.gz", FASTQ_SUFFIXES) == ("foo.fastq.gz", True)
    assert normalize_suffixes("foo.gz", FASTQ_SUFFIXES, compressed=None) == ("foo.fastq.gz", True)
    assert normalize_suffixes("foo.gz", FASTQ_SUFFIXES, compressed=True) == ("foo.fastq.gz", True)

    with pytest.raises(RuntimeError):  # This case is self-contradictory so raises an error.
        normalize_suffixes("foo.gz", FASTQ_SUFFIXES, compressed=False)

    assert normalize_suffixes("foo.fastq", FASTQ_SUFFIXES) == ("foo.fastq", False)
    assert normalize_suffixes("foo.fastq", FASTQ_SUFFIXES, compressed=None) == ("foo.fastq", False)
    assert normalize_suffixes("foo.fastq", FASTQ_SUFFIXES, compressed=False) == ("foo.fastq", False)
    assert normalize_suffixes("foo.fastq", FASTQ_SUFFIXES, compressed=True) == ("foo.fastq.gz", True)


def test_fix_for_jira_ticket_c4_278():
    """Test that C4-278 is fixed."""

    # These tests came from C4-278
    # These next two tests are probably enough to test the bug.

    assert normalize_suffixes('test1.fastq.gz', FASTQ_SUFFIXES, compressed=True) == ("test1.fastq.gz", True)

    with pytest.raises(RuntimeError):
        normalize_suffixes('test1.fastq.gz', FASTQ_SUFFIXES, compressed=False)

    # What follows here is probably overkill, but it illustrates some useful techniques in mocking, and it serves
    # as a practical test that the recent support for opening byte streams in the MockFileSystem works.
    # -kmp 18-Aug-2020

    mfs = MockFileSystem()

    def mock_didnt_compress(file, mode):
        """We don't expect to call this function, but should complain if it does get called."""
        raise AssertionError("Compression was not attempted: io.open(%r, %r)" % (file, mode))

    def mock_gzip_open_for_write(file, mode):
        """Writes a prefix followed by an ordinary bytes stream of uncompressed data for mocking purposes."""
        assert mode == 'w'
        opener = mfs.open(file, 'wb')
        opener.stream.write(b"MockCompressed:")
        return opener

    with mock.patch("random.choice", NotReallyRandom().choice):
        # By using a separate copy of NotReallyRandom(), we can simulate the inner core of what the call to
        # generate_sample_fastq_file will return in the next code block.
        # Note that our mock will NOT do compression at the internal level, regardless of the filename.
        expected_content_1 = ("MockCompressed:%s" % generate_sample_fastq_content(num=20, length=25)).encode('utf-8')

    with mock.patch("random.choice", NotReallyRandom().choice):
        # In principle, depending on whether the bug is fixed, we don't know which of io.open or gzip.open will be used.
        with mock.patch("io.open", mock_didnt_compress):
            with mock.patch("gzip.open", mock_gzip_open_for_write):

                # The bug report specifies two situations.
                # The first is that generate_sample_fastq_file(input_filename, num=20, length=25, compressed=True)
                # genenerates the file "test1.fastq.gz.fastq.gz" rather than just "test1.fastq.gz".
                input_filename = 'test1.fastq.gz'
                generated_filename = generate_sample_fastq_file(input_filename, num=20, length=25, compressed=True)
                assert generated_filename == input_filename  # The bug was that it generated a different name
                assert mfs.files.get(generated_filename) == expected_content_1

                # The bug report specifies that this gives a wrong result, too,
                with pytest.raises(RuntimeError):
                    generate_sample_fastq_file(input_filename, num=20, length=25, compressed=False)
