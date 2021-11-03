import contextlib
import os
import pytest
import tempfile

from unittest import mock
from dcicutils import command_utils as command_utils_module
from dcicutils.command_utils import (
    _ask_boolean_question,  # noQA - access to internal function is so we can test it
    yes_or_no, y_or_n, ShellScript, shell_script,
)
from dcicutils.misc_utils import ignored, file_contents
from dcicutils.qa_utils import printed_output


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
            ignored(args, kwargs)
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


def test_shell_script_class():

    script = ShellScript()
    assert script.script == ""

    assert script.executable == script.EXECUTABLE == "/bin/bash"

    script.do("foo")
    expected_script = "foo"
    assert script.script == expected_script

    script.do("bar")
    expected_script = "foo; bar"
    assert script.script == expected_script

    with mock.patch("subprocess.run") as mock_run:

        script.execute()
        mock_run.assert_called_with(expected_script, executable=script.EXECUTABLE, shell=True)


def test_shell_script_class_with_working_dir():

    script = ShellScript()

    with mock.patch("subprocess.run") as mock_run:

        with script.using_working_dir("/some/dir"):

            expected_script = 'pushd /some/dir > /dev/null; echo "Selected working directory $(pwd)."'
            assert script.script == expected_script
            mock_run.assert_not_called()

        expected_script = expected_script + '; popd > /dev/null; echo "Restored working directory $(pwd)."'
        assert script.script == expected_script
        # The context manager does not finalize, but does restore outer directory context.
        mock_run.assert_not_called()

        script.execute()  # After finalizing explicitly, it gets called
        mock_run.assert_called_with(expected_script, executable=script.EXECUTABLE, shell=True)


@pytest.mark.parametrize('simulate', [False, True])
def test_shell_script_with_done_first(simulate):

    temp_filename = tempfile.mktemp()
    assert not os.path.exists(temp_filename)  # we were promised a filename that doesn't exist. test that.

    try:

        with printed_output() as printed:

            script = ShellScript(simulate=simulate)
            script.do(f"echo baz >> {temp_filename}")
            with script.done_first() as script_setup:
                script_setup.do(f"echo foo >> {temp_filename}")
                script_setup.do(f"echo bar >> {temp_filename}")
            script.execute()

            if simulate:
                assert not os.path.exists(temp_filename)  # test that file did NOT get made
                expected = [
                    f"SIMULATED:",
                    f"================================================================================",
                    f"echo foo >> {temp_filename};\\\n"
                    f" echo bar >> {temp_filename};\\\n"
                    f" echo baz >> {temp_filename}",
                    f"================================================================================"
                ]
                import json
                print(json.dumps(printed.lines, indent=2))
                print(json.dumps(expected, indent=2))
                assert printed.lines == expected
            else:
                assert os.path.exists(temp_filename)  # test that file got made
                assert file_contents(temp_filename) == 'foo\nbar\nbaz\n'

    finally:

        if os.path.exists(temp_filename):
            os.remove(temp_filename)  # cleanup, not that we actually have to
        assert not os.path.exists(temp_filename)  # make sure everything is tidy again


@pytest.mark.parametrize('simulate', [True, False])
def test_shell_script_class_unmocked(simulate):

    temp_filename = tempfile.mktemp()
    assert not os.path.exists(temp_filename)  # we were promised a filename that doesn't exist. test that.

    try:

        with printed_output() as printed:

            script = ShellScript(simulate=simulate)
            script.do(f"touch {temp_filename}")  # script will create the file
            script.execute()

            if simulate:
                assert not os.path.exists(temp_filename)  # test that file did NOT get made
                assert printed.lines == [
                    f'SIMULATED:',
                    f'================================================================================',
                    f'touch {temp_filename}',
                    f'================================================================================',
                ]
            else:
                assert os.path.exists(temp_filename)  # test that file got made

    finally:

        if os.path.exists(temp_filename):
            os.remove(temp_filename)  # cleanup, not that we actually have to
        assert not os.path.exists(temp_filename)  # make sure everything is tidy again


@pytest.mark.parametrize('simulate', [True, False])
def test_shell_script_context_manager(simulate):

    temp_filename = tempfile.mktemp()
    assert not os.path.exists(temp_filename)  # we were promised a filename that doesn't exist. test that.

    try:

        with printed_output() as printed:

            with shell_script(simulate=simulate) as script:
                script.do(f"touch {temp_filename}")  # script will create the file

            if simulate:
                assert not os.path.exists(temp_filename)  # test that file did NOT get made
                assert printed.lines == [
                    f'SIMULATED:',
                    f'================================================================================',
                    f'touch {temp_filename}',
                    f'================================================================================',
                ]
            else:
                assert os.path.exists(temp_filename)  # test that file got made

    finally:

        if os.path.exists(temp_filename):
            os.remove(temp_filename)  # cleanup, not that we actually have to
        assert not os.path.exists(temp_filename)  # make sure everything is tidy again
