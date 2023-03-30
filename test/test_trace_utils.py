import os

from dcicutils.misc_utils import override_environ, environ_bool
from dcicutils.qa_utils import printed_output
from dcicutils.trace_utils import make_trace_decorator, Trace


def test_trace():

    with override_environ(TRACE_ENABLED=None):

        # If the decorator is used with explicit enabled=True, it arranges for trace output.

        with printed_output() as printed:

            @Trace(enabled=True)
            def alpha_add_up(x, y):
                return x + y

            assert alpha_add_up(3, 4) == 7
            assert printed.lines == [
                'Entering test.test_trace_utils.alpha_add_up with args=(3, 4) kwargs=dict()',
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
                'Entering test.test_trace_utils.gamma_add_up with args=(3, 4) kwargs=dict()',
                'Function test.test_trace_utils.gamma_add_up returned 7',
            ]

            printed.reset()

            # Same function, but show it works with keyword arg calling ...

            assert gamma_add_up(y=3, x=4) == 7
            assert printed.lines == [
                'Entering test.test_trace_utils.gamma_add_up with args=() kwargs=dict(',
                '  y=3,',
                '  x=4,',
                ')',
                'Function test.test_trace_utils.gamma_add_up returned 7'
            ]

            printed.reset()

            assert gamma_add_up(3, y=4) == 7
            assert printed.lines == [
                'Entering test.test_trace_utils.gamma_add_up with args=(3,) kwargs=dict(',
                '  y=4,',
                ')',
                'Function test.test_trace_utils.gamma_add_up returned 7'
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
                'Entering test.test_trace_utils.beta_add_up with args=(3, 4) kwargs=dict()',
                'Function test.test_trace_utils.beta_add_up returned 7',
            ]
