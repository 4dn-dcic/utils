import contextlib
import pytest

from unittest import mock
from dcicutils import command_utils as command_utils_module
from dcicutils.command_utils import yes_or_no


@mock.patch.object(command_utils_module, "PRINT")
@mock.patch.object(command_utils_module, "input")
def test_yes_or_no(mock_input, mock_print):

    class OutOfInputs(Exception):
        pass

    def mocked_input(*args, **kwargs):
        if not inputs:
            raise OutOfInputs()
        return inputs.pop()

    @contextlib.contextmanager
    def input_series(*items):
        assert not inputs, "There are previously unused inputs."
        for item in reversed(items):
            inputs.append(item)
        yield
        assert not inputs, "Did not use all inputs."

    def expect_printed(*expected):
        def mocked_print(*what):
            printed = " ".join(what)
            assert printed in expected
        return mocked_print

    mock_input.side_effect = mocked_input

    mock_print.side_effect = expect_printed("Please answer 'yes' or 'no'.")

    inputs = []

    with input_series('yes'):
        assert yes_or_no("foo?") is True

    with input_series('no'):
        assert yes_or_no("foo?") is False

    with input_series('foo', 'bar', '', 'y'):  # None of these are OK
        with pytest.raises(OutOfInputs):
            yes_or_no("foo?")

    with input_series('', 'y', 'n', 'maybe', 'yes'):
        assert yes_or_no("foo?") is True

    with input_series('', 'y', 'n', 'maybe', 'no'):
        assert yes_or_no("foo?") is False

    mock_print.side_effect = expect_printed(
        "The default if you just press Enter is 'y'.",
        "Please answer 'y' or 'n'.")

    with input_series('y'):
        assert yes_or_no("foo?", quick=True) is True

    with input_series('n'):
        assert yes_or_no("foo?", default=True) is False

    with input_series(''):
        assert yes_or_no("foo?", quick=True, default=True) is True

    with input_series('foo', 'bar', ''):
        assert yes_or_no("foo?", default=True) is True

    mock_print.side_effect = expect_printed(
        "The default if you just press Enter is 'n'.",
        "Please answer 'y' or 'n'.")

    with input_series('foo', 'bar', ''):
        assert yes_or_no("foo?", default=False) is False
