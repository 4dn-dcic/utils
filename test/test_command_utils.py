import contextlib
import os
import pytest
import tempfile

from unittest import mock
from dcicutils import command_utils as command_utils_module
from dcicutils.command_utils import (
    _ask_boolean_question,  # noQA - access to internal function is so we can test it
    yes_or_no, y_or_n, ShellScript, shell_script, script_catch_errors, DEBUG_SCRIPT, SCRIPT_ERROR_HERALD,
)
from dcicutils.misc_utils import ignored, file_contents, PRINT
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


def test_script_catch_errors():

    normal_output = "This is normal program output."
    custom_exit_message = "Command failure."
    raw_error_message = "This is an error message."
    value_error_message = f"ValueError: {raw_error_message}"

    # Normal program, output occurs, no error raised, does an exit(0) implicitly via script_catch_errors.
    with printed_output() as printed:
        with pytest.raises(SystemExit) as exit_exc:
            with script_catch_errors():
                PRINT(normal_output)
        sys_exit = exit_exc.value
        assert isinstance(sys_exit, SystemExit)
        assert sys_exit.code == 0
        assert printed.lines == [normal_output]

    # Erring program before output occurs. Does an exit(1) implicitly via script_catch_errors
    # after catching and showing error.
    with printed_output() as printed:
        with pytest.raises(SystemExit) as exit_exc:
            with script_catch_errors():
                raise ValueError(raw_error_message)
        sys_exit = exit_exc.value
        assert isinstance(sys_exit, SystemExit)
        assert sys_exit.code == 1
        assert printed.lines == [SCRIPT_ERROR_HERALD, value_error_message]

    # Erring program after output occurs. Does an exit(1) implicitly via script_catch_errors
    # after catching and showing error.
    with printed_output() as printed:
        with pytest.raises(SystemExit) as exit_exc:
            with script_catch_errors():
                PRINT(normal_output)
                raise ValueError(raw_error_message)
        sys_exit = exit_exc.value
        assert isinstance(sys_exit, SystemExit)
        assert sys_exit.code == 1
        assert printed.lines == [normal_output, SCRIPT_ERROR_HERALD, value_error_message]

    # Erring program after output occurs. Does an exit(1) explicitly before script_catch_errors does.
    with printed_output() as printed:
        with pytest.raises(SystemExit) as exit_exc:
            with script_catch_errors():
                PRINT(normal_output)
                PRINT(custom_exit_message)
                exit(1)  # Bypasses script_catch_errors context manager, so won't show SCRIPT_ERROR_HERALD
        sys_exit = exit_exc.value
        assert isinstance(sys_exit, SystemExit)
        assert sys_exit.code == 1
        assert printed.lines == [normal_output, custom_exit_message]

    print(f"NOTE: The DEBUG_SCRIPT environment bool is globally {DEBUG_SCRIPT!r}.")
    with mock.patch.object(command_utils_module, "DEBUG_SCRIPT", "TRUE"):
        # As if DEBUG_SCRIPT=environ_bool("DEBUG_SCRIPT") had given different value for module variable DEBUG_SCRIPT.
        with printed_output() as printed:
            with pytest.raises(Exception) as non_exit_exc:
                with script_catch_errors():
                    PRINT(normal_output)
                    raise ValueError(raw_error_message)
            non_exit_exception = non_exit_exc.value
            assert isinstance(non_exit_exception, ValueError)
            assert str(non_exit_exception) == raw_error_message
            assert printed.lines == [normal_output]  # Any more output would be from Python itself reporting ValueError

    # Erring program explicitly fails.
    with printed_output() as printed:
        failure_message = "Foo! This failed."
        failure_message_parts = failure_message.split(' ')
        with pytest.raises(SystemExit) as exit_exc:
            with script_catch_errors() as fail:
                fail(*failure_message_parts)
        sys_exit = exit_exc.value
        assert isinstance(sys_exit, SystemExit)
        assert sys_exit.code == 1
        assert printed.lines == [failure_message]

    # Erring program explicitly fails, bypassing regular exception catches
    with printed_output() as printed:
        failure_message = "Foo! This failed."
        failure_message_parts = failure_message.split(' ')
        with pytest.raises(SystemExit) as exit_exc:
            with script_catch_errors() as fail:
                try:
                    fail(failure_message)
                except Exception:
                    assert "Test failure."
        sys_exit = exit_exc.value
        assert isinstance(sys_exit, SystemExit)
        assert sys_exit.code == 1
        assert printed.lines == [failure_message]
