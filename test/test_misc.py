import pytest

from dcicutils.qa_checkers import DocsChecker, DebuggingArtifactChecker


class UtilsDocsChecker(DocsChecker):
    SKIP_SUBMODULES = ['jh_utils', 'env_utils_legacy']


@pytest.mark.static
def test_utils_doc():
    checker = UtilsDocsChecker(sources_subdir="dcicutils", docs_index_file="dcicutils.rst")
    checker.check_documentation()


@pytest.mark.static
def test_utils_debugging_artifacts():
    checker = DebuggingArtifactChecker(sources_subdir="dcicutils")
    checker.check_for_debugging_patterns()
