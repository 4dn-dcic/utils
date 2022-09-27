import os
import pytest

from dcicutils.qa_checkers import DocsChecker, DebuggingArtifactChecker, ChangeLogChecker


@pytest.mark.static
def test_utils_doc():

    class UtilsDocsChecker(DocsChecker):
        SKIP_SUBMODULES = ['jh_utils', 'env_utils_legacy']

    checker = UtilsDocsChecker(sources_subdir="dcicutils", docs_index_file="dcicutils.rst", recursive=False)
    checker.check_documentation()


@pytest.mark.static
def test_utils_debugging_artifacts():
    checker = DebuggingArtifactChecker(sources_subdir="dcicutils")
    checker.check_for_debugging_patterns()


@pytest.mark.static
def test_changelog_consistency():

    class MyChangeLogChecker(ChangeLogChecker):
        PYPROJECT = os.path.join(os.path.dirname(__file__), "../pyproject.toml")
        CHANGELOG = os.path.join(os.path.dirname(__file__), "../CHANGELOG.rst")

    MyChangeLogChecker.check_version()


def test_foo_bar():
    import pdb; pdb.set_trace()
