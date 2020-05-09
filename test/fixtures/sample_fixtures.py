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


@pytest.yield_fixture()
def math_enabled():
    with mock.patch.object(MockMath, "MATH_ENABLED", True):
        yield
