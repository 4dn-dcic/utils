import pytest

from unittest import mock


class MockMathError(Exception):
    pass


class MockMath:

    MATH_ENABLED = False

    @classmethod
    def add(cls, x, y):
        if cls.MATH_ENABLED:
            return x + y
        else:
            raise MockMathError("Math is not enabled.")


@pytest.fixture()  # formerly pytest.yield_fixture, but that is now deprecated. -kmp 3-Oct-2021
def math_enabled():
    with mock.patch.object(MockMath, "MATH_ENABLED", True):
        yield
