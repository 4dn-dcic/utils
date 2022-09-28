import os
import pytest

from dcicutils.qa_checkers import DocsChecker, DebuggingArtifactChecker, ChangeLogChecker

from .conftest_settings import REPOSITORY_ROOT_DIR


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

    checker = DebuggingArtifactChecker(sources_subdir="test", skip_files="data_files/", filter_patterns=['pdb'])
    checker.check_for_debugging_patterns()


@pytest.mark.static
def test_changelog_consistency():

    class MyChangeLogChecker(ChangeLogChecker):
        PYPROJECT = os.path.join(REPOSITORY_ROOT_DIR, "pyproject.toml")
        CHANGELOG = os.path.join(REPOSITORY_ROOT_DIR, "CHANGELOG.rst")

    MyChangeLogChecker.check_version()
