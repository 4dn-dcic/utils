import os
import pytest

from dcicutils import trace_utils as trace_utils_module
from dcicutils.misc_utils import override_environ, environ_bool
from dcicutils.qa_utils import printed_output
from dcicutils.trace_utils import make_trace_decorator, Trace
from unittest import mock


def test_trace():

    with override_environ(TRACE_ENABLED=None):

        # If the decorator is used with explicit enabled=True, it arranges for trace output.

        with printed_output() as printed:

            @Trace(enabled=True)
            def alpha_add_up(x, y):
                return x + y

            assert alpha_add_up(3, 4) == 7
            assert printed.lines == [
                'Entering test.test_trace_utils.alpha_add_up with args=(3, 4) kwargs={}',
                'Function test.test_trace_utils.alpha_add_up returned 7',
            ]

        # If the decorator is used without explicit enabled=True, it doesn't arrange for trace output
        # unless an environment variable is set (which is not the case here).

        with printed_output() as printed:

            @Trace()
            def beta_add_up(x, y):
                return x + y

            assert beta_add_up(3, 4) == 7
            assert printed.lines == []

        # Same case, but with TRACE_ENABLED environment variable set

        with printed_output() as printed:

            with override_environ(TRACE_ENABLED="TRUE"):

                assert os.environ.get('TRACE_ENABLED') == "TRUE"

                @Trace()
                def gamma_add_up(x, y):
                    return x + y

            assert gamma_add_up(3, 4) == 7
            assert printed.lines == [
                'Entering test.test_trace_utils.gamma_add_up with args=(3, 4) kwargs={}',
                "Function test.test_trace_utils.gamma_add_up returned 7",
            ]

            printed.reset()

            # Same function, but show it works with keyword arg calling ...

            assert gamma_add_up(y=3, x=4) == 7
            assert printed.lines == [
                "Entering test.test_trace_utils.gamma_add_up with args=() kwargs={'y': 3, 'x': 4}",
                "Function test.test_trace_utils.gamma_add_up returned 7",
            ]

            printed.reset()

            assert gamma_add_up(3, y=4) == 7
            assert printed.lines == [
                "Entering test.test_trace_utils.gamma_add_up with args=(3,) kwargs={'y': 4}",
                "Function test.test_trace_utils.gamma_add_up returned 7",
            ]


def test_make_trace_decorator():

    print()

    print("Scenario 1")

    with override_environ(TRACE_ENABLED=None, FOO_TRACE_ENABLED=None):

        FOO_TRACE_ENABLED = environ_bool('FOO_TRACE_ENABLED')

        FooTrace = make_trace_decorator(FOO_TRACE_ENABLED)

        # no tracers enabled, so this won't have output

        with printed_output() as printed:

            @FooTrace()
            def alpha_add_up(x, y):
                return x + y

            assert alpha_add_up(3, 4) == 7
            assert printed.lines == []

    print("Scenario 2")

    with override_environ(TRACE_ENABLED=None, BAR_TRACE_ENABLED="TRUE"):

        BAR_TRACE_ENABLED = environ_bool('BAR_TRACE_ENABLED')

        BarTrace = make_trace_decorator(BAR_TRACE_ENABLED)

        # As long as one of the trace enablers is on, this should see trace output

        with printed_output() as printed:

            @BarTrace()
            def beta_add_up(x, y):
                return x + y

            assert beta_add_up(3, 4) == 7
            assert printed.lines == [
                'Entering test.test_trace_utils.beta_add_up with args=(3, 4) kwargs={}',
                'Function test.test_trace_utils.beta_add_up returned 7',
            ]


def test_trace_redact():

    print()

    with override_environ(TRACE_REDACT=None):

        with printed_output() as printed:

            @Trace(enabled=True)
            def fn_1(x):
                return x

            d = {"AWS_ACCESS_KEY_ID": "FOO", "AWS_SECRET_KEY": "BAR"}
            d_obfuscated = {"AWS_ACCESS_KEY_ID": "FOO", "AWS_SECRET_KEY": "<REDACTED>"}

            assert fn_1(d) == d
            assert printed.lines == [
                f"Entering test.test_trace_utils.fn_1 with args={(d_obfuscated,)!r} kwargs={{}}",
                f"Function test.test_trace_utils.fn_1 returned {d_obfuscated!r}",
            ]

            printed.reset()

            @Trace(enabled=True)
            def fn_2(**x):
                return x

            assert fn_2(**d) == d
            assert printed.lines == [
                f"Entering test.test_trace_utils.fn_2 with args=() kwargs={d_obfuscated!r}",
                f"Function test.test_trace_utils.fn_2 returned {d_obfuscated!r}",
            ]

            printed.reset()

            # Simulate what would happen if TRACE_REDACT=FALSE had been in an environment variable at module load time.
            with mock.patch.object(trace_utils_module, "TRACE_REDACT", False):
                assert fn_2(**d) == d
                assert printed.lines == [
                    f"Entering test.test_trace_utils.fn_2 with args=() kwargs={d!r}",
                    f"Function test.test_trace_utils.fn_2 returned {d!r}",
                ]

            printed.reset()

            @Trace(enabled=True)
            def fn_3():
                return d

            assert fn_3() == d
            assert printed.lines == [
                f"Entering test.test_trace_utils.fn_3 with args=() kwargs={{}}",
                f"Function test.test_trace_utils.fn_3 returned {d_obfuscated!r}"
            ]

            printed.reset()

            @Trace(enabled=False)
            def fn_3a():
                return d

            assert fn_3a() == d
            assert printed.lines == []

            printed.reset()

            @Trace(enabled=True)
            def fn_4():
                raise ValueError("Bad input.")

            with pytest.raises(ValueError):
                fn_4()
            assert printed.lines == [
                f"Entering test.test_trace_utils.fn_4 with args=() kwargs={{}}",
                f"Function test.test_trace_utils.fn_4 raised ValueError: Bad input."
            ]
