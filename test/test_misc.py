import os
import pytest

from dcicutils.qa_checkers import DocsChecker, DebuggingArtifactChecker, ChangeLogChecker


_ROOT_DIR = os.path.dirname(os.path.dirname(__file__))


@pytest.mark.static
def test_utils_doc():

    class UtilsDocsChecker(DocsChecker):
        SKIP_SUBMODULES = ['jh_utils', 'env_utils_legacy']

    checker = UtilsDocsChecker(sources_subdir="dcicutils", docs_index_file="dcicutils.rst", recursive=False,
                               show_detail=False)
    checker.check_documentation()


@pytest.mark.static
def test_utils_debugging_artifacts():
    checker = DebuggingArtifactChecker(sources_subdir="dcicutils")
    checker.check_for_debugging_patterns()

    checker = DebuggingArtifactChecker(sources_subdir="test", skip_files="data_files/")
    checker.check_for_debugging_patterns()


@pytest.mark.static
def test_changelog_consistency():

    class MyChangeLogChecker(ChangeLogChecker):
        PYPROJECT = os.path.join(_ROOT_DIR, "pyproject.toml")
        CHANGELOG = os.path.join(_ROOT_DIR, "CHANGELOG.rst")

    MyChangeLogChecker.check_version()
