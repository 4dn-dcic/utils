import os
import pytest

from dcicutils.license_utils import C4PythonInfrastructureLicenseChecker
from dcicutils.qa_checkers import DocsChecker, DebuggingArtifactChecker, ChangeLogChecker, ContributionsChecker

from .conftest_settings import REPOSITORY_ROOT_DIR


@pytest.mark.static
def test_utils_doc():

    __tracebackhide__ = True

    class UtilsDocsChecker(DocsChecker):
        SKIP_SUBMODULES = ['jh_utils', 'env_utils_legacy']

    checker = UtilsDocsChecker(sources_subdir="dcicutils", docs_index_file="dcicutils.rst", recursive=False,
                               show_detail=False)
    checker.check_documentation()


@pytest.mark.static
def test_utils_debugging_artifacts():

    __tracebackhide__ = True

    checker = DebuggingArtifactChecker(sources_subdir="dcicutils", if_used='warning')
    checker.check_for_debugging_patterns()

    checker = DebuggingArtifactChecker(sources_subdir="test", skip_files="data_files/", filter_patterns=['pdb'])
    checker.check_for_debugging_patterns()


@pytest.mark.static
def test_changelog_consistency():

    __tracebackhide__ = True

    class MyChangeLogChecker(ChangeLogChecker):
        PYPROJECT = os.path.join(REPOSITORY_ROOT_DIR, "pyproject.toml")
        CHANGELOG = os.path.join(REPOSITORY_ROOT_DIR, "CHANGELOG.rst")

    MyChangeLogChecker.check_version()


@pytest.mark.static
def test_license_compatibility():

    C4PythonInfrastructureLicenseChecker.validate()


@pytest.mark.static
def test_contributions():
    ContributionsChecker.validate()
