import importlib
import pytest
import os
from dcicutils.file_utils import normalize_path, search_for_file
from dcicutils.tmpfile_utils import temporary_directory

HOME_DIRECTORY = "/Some/HomeDirectory"
CURRENT_DIRECTORY = os.path.abspath(os.path.curdir)


@pytest.fixture(autouse=True)
def monkey_path_home_directory(monkeypatch):
    monkeypatch.setenv("HOME", HOME_DIRECTORY)
    file_utils_module = importlib.import_module("dcicutils.file_utils")
    save_home_directory = file_utils_module.HOME_DIRECTORY
    file_utils_module.HOME_DIRECTORY = HOME_DIRECTORY
    yield
    file_utils_module = save_home_directory


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


def test_normalize_path():
    assert os.environ["HOME"] == HOME_DIRECTORY
    assert normalize_path(None) == ""
    assert normalize_path("") == ""
    assert normalize_path(123) == ""
    assert normalize_path("///") == "/"
    assert normalize_path(".") == "."
    assert normalize_path(".//") == "."
    assert normalize_path("/") == "/"
    assert normalize_path("/.") == "/"
    assert normalize_path("./abc") == "abc"
    assert normalize_path("./abc", absolute=True) == f"{CURRENT_DIRECTORY}/abc"
    assert normalize_path("/abc/def") == "/abc/def"
    assert normalize_path("/abc/def/") == "/abc/def"
    assert normalize_path("/abc///def") == "/abc/def"
    assert normalize_path("///abc///def") == "/abc/def"
    assert normalize_path("~///Ghi//Jkl//") == "~/Ghi/Jkl"
    assert normalize_path("~///Ghi//Jkl/", expand_home=False, absolute=False) == "~/Ghi/Jkl"
    assert normalize_path("~///Ghi//Jkl/", absolute=True) == f"{CURRENT_DIRECTORY}/~/Ghi/Jkl"
    assert normalize_path("~///Ghi//Jkl/", expand_home=True, absolute=True) == f"{HOME_DIRECTORY}/Ghi/Jkl"
    assert normalize_path("~///Ghi//Jkl/", expand_home=True, absolute=False) == f"{HOME_DIRECTORY}/Ghi/Jkl"
    assert normalize_path("~///Ghi//Jkl/", expand_home=False, absolute=True) == f"{CURRENT_DIRECTORY}/~/Ghi/Jkl"
    assert normalize_path("~///Ghi//Jkl/", expand_home=True) == f"{HOME_DIRECTORY}/Ghi/Jkl"
    assert normalize_path(f"{HOME_DIRECTORY}/Ghi//Jkl/", expand_home=False) == "~/Ghi/Jkl"
    assert normalize_path(f"{HOME_DIRECTORY}", expand_home=False) == "~"
    assert normalize_path(f"{HOME_DIRECTORY}/", expand_home=False) == "~"
    assert normalize_path(f"{HOME_DIRECTORY}/.ssh", expand_home=False) == "~/.ssh"
    assert normalize_path(f"~/.ssh", expand_home=True) == f"{HOME_DIRECTORY}/.ssh"
