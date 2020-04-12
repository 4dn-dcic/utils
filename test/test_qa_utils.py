import re
from dcicutils.qa_utils import mock_not_called, ignored


def test_mock_not_called():
    name = "foo"
    mocked_foo = mock_not_called(name)
    try:
        mocked_foo(1, 2, three=3)
    except AssertionError as e:
        m = re.match("%s.*called" % re.escape(name), str(e))
        assert m, "Expected assertion text did not appear."
    else:
        raise AssertionError("An AssertionError was not raised.")


def test_ignored():

    def foo(x, y):
        ignored(x, y)

    # Check that no error occurs for having used this.
    assert foo(3, 4) is None
