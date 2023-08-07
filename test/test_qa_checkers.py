import io
import json
import os
import pytest

from dcicutils import contribution_utils as contribution_utils_module
from dcicutils.contribution_utils import BasicContributions
from dcicutils.misc_utils import lines_printed_to, remove_prefix
from dcicutils.qa_checkers import (
    DebuggingArtifactChecker, DocsChecker, ChangeLogChecker, VersionChecker, confirm_no_uses, find_uses,
    ContributionsChecker
)
from dcicutils.qa_utils import MockFileSystem, printed_output
from unittest import mock
from .test_contribution_utils import git_context, SAMPLE_PROJECT_HOME
from .conftest_settings import TEST_DIR


PRINT_PATTERN = "^[^#]*print[(]"
TRACE_PATTERN = "^[^#]*pdb[.]set_trace[(][)]"

DEBUGGING_PATTERNS = {
    "call to print": PRINT_PATTERN,
    "active use of pdb.set_trace": TRACE_PATTERN
}


def test_change_log_checker():

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove():

        with lines_printed_to("badpyproject.toml") as out:
            out('[tool.poetry]')
            out('name = "Sample"')
            # Missing version = ...
            out('other = "whatever"')

        class BadVersionChecker(VersionChecker):
            PYPROJECT = "badpyproject.toml"
            CHANGELOG = None

        with pytest.raises(AssertionError) as exc:
            BadVersionChecker().check_version()
        assert str(exc.value) == "Missing version in badpyproject.toml."

        with lines_printed_to("pyproject.toml") as out:
            out('[tool.poetry]')
            out('name = "Sample"')
            out('version = "1.0.2"')
            out('other = "whatever"')

        class SimpleProjectChecker(VersionChecker):
            PYPROJECT = "pyproject.toml"
            CHANGELOG = None

        SimpleProjectChecker.check_version()

        with lines_printed_to("goodchangelog.rst") as out:
            out("1.0.2")
            out("=====")
            out()
            out("Another small patch to version one.")
            out()
            out("1.0.1")
            out("=====")
            out()
            out("Small patch to version one.")
            out()
            out("1.0.0")
            out("=====")
            out()
            out("First big release.")
            out()

        class GoodChangeLogChecker(ChangeLogChecker):
            PYPROJECT = "pyproject.toml"
            CHANGELOG = "goodchangelog.rst"

        GoodChangeLogChecker().check_version()

        with lines_printed_to("goodchangelog.md") as out:
            out("# 1.0.2")
            out()
            out("Another small patch to version one.")
            out()
            out("# 1.0.1")
            out()
            out("Small patch to version one.")
            out()
            out("# 1.0.0")
            out()
            out("First big release.")
            out()

        class GoodMdChangeLogChecker(ChangeLogChecker):
            PYPROJECT = "pyproject.toml"
            CHANGELOG = "goodchangelog.md"

        GoodMdChangeLogChecker().check_version()

        with lines_printed_to("badchangelog.rst") as out:
            out("2.0.1")
            out("=====")
            out()
            out("Small patch to version two.")
            out()
            out("2.0.0")
            out("=====")
            out()
            out("Second big release.")
            out()

        class BadChangeLogChecker(ChangeLogChecker):
            PYPROJECT = "pyproject.toml"
            CHANGELOG = "badchangelog.rst"

        with pytest.raises(AssertionError) as exc:
            BadChangeLogChecker().check_version()
        assert str(exc.value) == "Missing entry for version 1.0.2 in badchangelog.rst."

        with lines_printed_to("goodreversedchangelog.rst") as out:
            out("1.0.0")
            out("=====")
            out()
            out("First big release.")
            out()
            out("1.0.1")
            out("=====")
            out()
            out("Small patch to version one.")
            out()
            out("1.0.2")
            out("=====")
            out()
            out("Another small patch to version one.")
            out()

        class GoodReversedChangeLogChecker(ChangeLogChecker):
            PYPROJECT = "pyproject.toml"
            CHANGELOG = "goodreversedchangelog.rst"

        GoodReversedChangeLogChecker().check_version()


def test_as_module_name():

    assert DocsChecker.as_module_name('/foo/bar/baz/alpha.py', relative_to_prefix="/foo/bar") == 'baz.alpha'
    assert DocsChecker.as_module_name('/x/y/baz/alpha.py', relative_to_prefix="/foo/bar") == 'x.y.baz.alpha'


def test_is_allowed_submodule():

    assert DocsChecker.is_allowed_submodule_file("/foo/bar/tests/helpers.py") is False
    assert DocsChecker.is_allowed_submodule_file("/foo/bar/test/helpers.py") is False
    assert DocsChecker.is_allowed_submodule_file("/foo/bar/test_files/helpers.py") is False
    assert DocsChecker.is_allowed_submodule_file("/foo/bar/test_foo.py") is False

    assert DocsChecker.is_allowed_submodule_file("__init__.py") is False
    assert DocsChecker.is_allowed_submodule_file("__foo__.py") is False
    assert DocsChecker.is_allowed_submodule_file("_foo.py") is False
    assert DocsChecker.is_allowed_submodule_file(".foo.py") is False
    assert DocsChecker.is_allowed_submodule_file("-foo.py") is False
    assert DocsChecker.is_allowed_submodule_file("4foo.py") is False

    assert DocsChecker.is_allowed_submodule_file("/foo/bar/__init__.py") is False
    assert DocsChecker.is_allowed_submodule_file("/foo/bar/__foo__.py") is False
    assert DocsChecker.is_allowed_submodule_file("/foo/bar/_foo.py") is False
    assert DocsChecker.is_allowed_submodule_file("/foo/bar/.foo.py") is False
    assert DocsChecker.is_allowed_submodule_file("/foo/bar/-foo.py") is False
    assert DocsChecker.is_allowed_submodule_file("/foo/bar/4foo.py") is False

    assert DocsChecker.is_allowed_submodule_file("/foo/bar/init.py") is True
    assert DocsChecker.is_allowed_submodule_file("/foo/bar/baz") is True
    assert DocsChecker.is_allowed_submodule_file("/foo/bar/baz.txt") is True


def test_find_uses():

    glob_pattern = os.path.join(TEST_DIR, 'data_files/sample_source_files/*.py')
    raw = find_uses(where=glob_pattern,
                    patterns=DEBUGGING_PATTERNS)
    output = {remove_prefix(TEST_DIR + os.sep, file): problems for file, problems in raw.items()}
    assert output == {
        'data_files/sample_source_files/file1.py': [
            {'line': '    print("first use")',
             'line_number': 2,
             'summary': 'call to print'},
            {'line': '    print("second use")',
             'line_number': 3,
             'summary': 'call to print'},
            {'line': '    print("third use", z)',
             'line_number': 11,
             'summary': 'call to print'},
            {'line': '    import pdb; pdb.set_trace()',  # noQA
             'line_number': 12,
             'summary': 'active use of pdb.set_trace'}
        ],
        'data_files/sample_source_files/file2.py': [
            {'line': '    print("third use", z)',
             'line_number': 5,
             'summary': 'call to print'},
            {'line': '    pdb.set_trace()  # Second tallied use',  # noQA
             'line_number': 13,
             'summary': 'active use of pdb.set_trace'},
            {'line': '    pdb.set_trace()  # Third tallied use',  # noQA
             'line_number': 17,
             'summary': 'active use of pdb.set_trace'}
        ]
    }


def test_confirm_no_uses():

    with pytest.raises(AssertionError) as exc_info:

        glob_pattern = os.path.join(TEST_DIR, 'data_files/sample_source_files/*.py')
        confirm_no_uses(where=glob_pattern,
                        patterns=DEBUGGING_PATTERNS)

    lines = str(exc_info.value).split('\n')
    assert len(lines) == 3
    assert lines[0] == "7 problems detected:"

    prefix = f" In {TEST_DIR}/data_files/sample_source_files/"
    lines = [remove_prefix(prefix, line) for line in sorted(lines[1:])]

    assert lines[0] == "file1.py, 3 calls to print and 1 active use of pdb.set_trace."
    assert lines[1] == "file2.py, 1 call to print and 2 active uses of pdb.set_trace."


def test_debugging_artifact_checker():

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove():
        with mock.patch("glob.glob") as mock_glob:
            with printed_output() as printed:

                dac = DebuggingArtifactChecker(sources_subdir="foo")

                mock_glob.return_value = []
                dac.check_for_debugging_patterns()

                with lines_printed_to("foo/bar.py") as out:
                    out('x = 1')
                mock_glob.return_value = ["foo/bar.py"]
                dac.check_for_debugging_patterns()

                assert printed.lines == []

                with lines_printed_to("foo/bar.py") as out:
                    out('x = 1')
                    out('print("foo")')
                with pytest.raises(Exception) as exc:
                    dac.check_for_debugging_patterns()
                assert isinstance(exc.value, AssertionError)
                assert str(exc.value) == "1 problem detected:\n In foo/bar.py, 1 call to print."

                assert printed.lines == []  # We might at some point print the actual problems, but we don't know.

                with lines_printed_to("foo/bar.py") as out:
                    out('x = 1')
                    out('import pdb; pdb.set_trace()')  # noQA
                with pytest.raises(Exception) as exc:
                    dac.check_for_debugging_patterns()
                assert isinstance(exc.value, AssertionError)
                assert str(exc.value) == "1 problem detected:\n In foo/bar.py, 1 active use of pdb.set_trace."

                assert printed.lines == []  # We might at some point print the actual problems, but we don't know.


@mock.patch.object(contribution_utils_module, "PROJECT_HOME", SAMPLE_PROJECT_HOME)
def test_contribution_checker():

    print()  # start on a fresh line
    some_repo_name = 'foo'
    mfs = MockFileSystem()
    with mfs.mock_exists_open_remove_abspath_getcwd_chdir():
        # Predict which cache file we'll need, so we can make it ahead of time.
        contributions_cache_file = BasicContributions(repo=some_repo_name).contributors_json_file()
        print(f"contributions_cache_file={contributions_cache_file}")
        os.chdir(os.path.join(SAMPLE_PROJECT_HOME, some_repo_name))
        print(f"working dir={os.getcwd()}")
        with io.open(contributions_cache_file, 'w') as fp:
            cache_data = {
                "forked_at": "2015-01-01T12:34:56-05:00",
                "pre_fork_contributors_by_name": None,
                "contributors_by_name": {
                    "John Smith": {
                        "emails": ["jsmith@somewhere"],
                        "names": ["John Smith"],
                    }
                }
            }
            json.dump(cache_data, fp=fp)
        mocked_commits = {
            some_repo_name: [
                {
                    "hexsha": "aaaa",
                    "committed_datetime": "2016-01-01T01:23:45-05:00",
                    "author": {"name": "John Smith", "email": "jsmith@somewhere"},
                    "message": "something"
                },
                {
                    "hexsha": "bbbb",
                    "committed_datetime": "2017-01-02T12:34:56-05:00",
                    "author": {"name": "Sally", "email": "ssmith@elsewhere"},
                    "message": "something else"
                }
            ]
        }
        with git_context(mocked_commits=mocked_commits):
            with printed_output() as printed:
                with pytest.raises(AssertionError) as exc:
                    ContributionsChecker.validate()
                assert str(exc.value) == "There are contributor cache discrepancies."
                assert printed.lines == [
                    "John Smith (jsmith@somewhere)",
                    "Sally (ssmith@elsewhere)",
                    "===== THERE ARE CONTRIBUTOR CACHE DISCREPANCIES =====",
                    "To Add:",
                    " * contributors.Sally.emails.ssmith@elsewhere",
                    " * contributors.Sally.names.Sally",
                ]
