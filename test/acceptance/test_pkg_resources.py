# This is an acceptance test for the pkg_resources package.

import glob
import os
import pkg_resources


TEST_ACCEPTANCE_DIR = os.path.dirname(__file__)  # test/acceptance

TEST_DIR = os.path.dirname(TEST_ACCEPTANCE_DIR)  # test

PYPROJECT_DIR = os.path.dirname(TEST_DIR)


def test_resource_filename():
    """
    They keep telling us the function pkg_resource package is going away.
    We've disabled the warning but we need to make sure we don't get in trouble.
    This tests that at least its most common use, pkg_resources.resource_filename, hasn't gone away yet.
    """

    print()  # start on a fresh line

    short_name = '__init__.py'  # there will almost always be one of these

    print("Experiment #1: Current Package (a bit of a special case)")

    print(f"short_name = {short_name}")
    expected = os.path.join(PYPROJECT_DIR, os.path.join('dcicutils', short_name))
    print(f"expected = {expected}")
    actual = pkg_resources.resource_filename('dcicutils', short_name)
    print(f"actual = {actual}")
    assert actual == expected

    print("Experiment #2: Arbitrary Package (the more usual case)")

    print(f"short_name = {short_name}")
    [pytest_dir] = glob.glob(os.path.join(os.environ['VIRTUAL_ENV'], 'lib', 'python*', 'site-packages', 'pytest'))
    expected = os.path.join(pytest_dir, short_name)
    print(f"expected = {expected}")
    actual = pkg_resources.resource_filename('pytest', short_name)
    print(f"actual = {actual}")
    assert actual == expected
