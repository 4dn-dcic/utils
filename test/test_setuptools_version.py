# Build/regression guard for the setuptools pin in pyproject.toml.
#
# dcicutils (deployment_utils, project_utils) and pinned pyramid 1.10.8 import
# pkg_resources at module load. setuptools 82 removed the pkg_resources module,
# so pyproject.toml caps setuptools below 82 (setuptools = ">=70.3,<82").
#
# This module deliberately imports only setuptools -- NOT pkg_resources -- so
# that if a future poetry update (or a widened pin) admits setuptools >= 82 the
# assertion below fails with an explanatory message, instead of the opaque
# "No module named 'pkg_resources'" collection error seen elsewhere.
# The companion test/acceptance/test_pkg_resources.py exercises pkg_resources
# itself; this test guards the version boundary that keeps it importable.

import setuptools


PKG_RESOURCES_REMOVED_IN_SETUPTOOLS_MAJOR = 82


def test_setuptools_still_provides_pkg_resources():
    setuptools_major = int(setuptools.__version__.split('.')[0])
    print(f"setuptools version = {setuptools.__version__}")
    assert setuptools_major < PKG_RESOURCES_REMOVED_IN_SETUPTOOLS_MAJOR, (
        f"setuptools {setuptools.__version__} removed pkg_resources;"
        f" the pyproject.toml cap (setuptools < {PKG_RESOURCES_REMOVED_IN_SETUPTOOLS_MAJOR})"
        f" must hold until pkg_resources uses are migrated.")
