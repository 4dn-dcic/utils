import io
from dcicutils.misc_utils import PRINT, ignored


def test_uppercase_print():
    # This is just a synonym, so the easiest thing is just to test that fact.
    assert PRINT == print

    # But also a basic test that it does something
    s = io.StringIO()
    PRINT("something", file=s)
    assert s.getvalue() == "something\n"


def test_ignored():

    def foo(x, y):
        ignored(x, y)

    # Check that no error occurs for having used this.
    assert foo(3, 4) is None
