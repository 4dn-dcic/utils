import contextlib
import pytest

from unittest import mock
from dcicutils import command_utils as command_utils_module
from dcicutils.command_utils import _ask_boolean_question, yes_or_no, y_or_n


@contextlib.contextmanager
def print_expected(*expected):

    def mocked_print(*what):
        printed = " ".join(what)
        assert printed in expected

    with mock.patch.object(command_utils_module, "PRINT") as mock_print:
        mock_print.side_effect = mocked_print
        yield


class OutOfInputs(Exception):
    pass


@contextlib.contextmanager
def input_series(*items):
    with mock.patch.object(command_utils_module, "input") as mock_input:

        def mocked_input(*args, **kwargs):
            if not inputs:
                raise OutOfInputs()
            return inputs.pop()

        mock_input.side_effect = mocked_input

        inputs = []

        for item in reversed(items):
            inputs.append(item)
        yield
        assert not inputs, "Did not use all inputs."


def test_ask_boolean_question():

    with print_expected("Please answer 'yes' or 'no'."):

        with input_series('yes'):
            assert _ask_boolean_question("foo?") is True

        with input_series('no'):
            assert _ask_boolean_question("foo?") is False

        with input_series('foo', 'bar', '', 'y'):  # None of these are OK
            with pytest.raises(OutOfInputs):
                _ask_boolean_question("foo?")

        with input_series('', 'y', 'n', 'maybe', 'yes'):
            assert _ask_boolean_question("foo?") is True

        with input_series('', 'y', 'n', 'maybe', 'no'):
            assert _ask_boolean_question("foo?") is False

    with print_expected("The default if you just press Enter is 'y'.",
                        "Please answer 'y' or 'n'."):

        with input_series('y'):
            assert _ask_boolean_question("foo?", quick=True) is True

        with input_series('n'):
            assert _ask_boolean_question("foo?", default=True) is False

        with input_series(''):
            assert _ask_boolean_question("foo?", quick=True, default=True) is True

        with input_series(''):
            assert _ask_boolean_question("foo?", quick=True, default=False) is False

        with input_series('foo', 'bar', ''):
            assert _ask_boolean_question("foo?", default=True) is True

    with print_expected("The default if you just press Enter is 'n'.",
                        "Please answer 'y' or 'n'."):

        with input_series('foo', 'bar', ''):
            assert _ask_boolean_question("foo?", default=False) is False


def test_yes_or_no():

    with print_expected("Please answer 'yes' or 'no'."):

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


def test_y_or_n():

    with print_expected("The default if you just press Enter is 'y'.",
                        "Please answer 'y' or 'n'."):
        with input_series('y'):
            assert y_or_n("foo?") is True

        with input_series('n'):
            assert y_or_n("foo?") is False

        with input_series('', 'x', ''):
            with pytest.raises(OutOfInputs):
                y_or_n("foo?")

        with input_series(''):
            assert y_or_n("foo?", default=True) is True

        with input_series(''):
            assert y_or_n("foo?", default=False) is False

        with input_series('foo', 'bar', ''):
            assert y_or_n("foo?", default=True) is True

    with print_expected("The default if you just press Enter is 'n'.",
                        "Please answer 'y' or 'n'."):
        with input_series('foo', 'bar', ''):
            assert y_or_n("foo?", default=False) is False
