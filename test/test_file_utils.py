import os
from dcicutils.file_utils import search_for_file
from dcicutils.tmpfile_utils import temporary_directory


def test_search_for_file():

    with temporary_directory() as dirname:
        filename = "foo.txt"
        with open(filepath := os.path.join(dirname, filename), "w"):
            assert search_for_file(filename, location=dirname) == [filepath]
            assert search_for_file(filename, location=dirname, single=False) == [filepath]
            assert search_for_file(filename, location=dirname, single=True) == filepath
            assert search_for_file(filename, location=dirname, recursive=True) == [filepath]
            assert search_for_file(filename, location=dirname, single=True, recursive=True) == filepath
            assert search_for_file("bad.txt", location=dirname, single=False, recursive=True) == []
            assert search_for_file("bad.txt", location=dirname, single=True, recursive=True) is None

    with temporary_directory() as dirname:
        subdirname = os.path.join(dirname, "bar")
        os.mkdir(subdirname)
        filename = "foo.txt"
        with open(filepath := os.path.join(dirname, filename), "w"):
            with open(subfilepath := os.path.join(subdirname, filename), "w"):
                assert (set(search_for_file(filename, location=dirname, recursive=True)) ==
                        set([filepath, subfilepath]))
                assert search_for_file(filename, location=dirname, recursive=False) == [filepath]

    with temporary_directory() as dirname:
        topdirname = dirname
        subdirname = os.path.join(dirname, "bar")
        os.mkdir(subdirname)
        dirname = os.path.join(dirname, "abc")
        os.mkdir(dirname)
        filename = "foo.txt"
        with open(filepath := os.path.join(dirname, filename), "w"):
            with open(subfilepath := os.path.join(subdirname, filename), "w"):
                assert (set(search_for_file(filename, location=topdirname, recursive=True)) ==
                        set([filepath, subfilepath]))
                assert search_for_file(filename, location=topdirname, recursive=False) == []
