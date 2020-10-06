import datetime
import io
import os
import pytest
import pytz
import re
import subprocess
import time
import uuid

from dcicutils import qa_utils
from dcicutils.misc_utils import Retry, PRINT, file_contents
from dcicutils.qa_utils import (
    mock_not_called, local_attrs, override_environ, override_dict, show_elapsed_time, timed, ignored,
    ControlledTime, Occasionally, RetryManager, MockFileSystem, NotReallyRandom,
    MockResponse, printed_output, MockBotoS3Client, MockKeysNotImplemented,
    UncustomizedInstance, CustomizableProperty, getattr_customized, raises_regexp,
)
# The following line needs to be separate from other imports. It is PART OF A TEST.
from dcicutils.qa_utils import notice_pytest_fixtures   # Use care if editing this line. It is PART OF A TEST.
from unittest import mock
from .fixtures.sample_fixtures import MockMathError, MockMath, math_enabled


notice_pytest_fixtures(math_enabled)   # Use care if editing this line. It is PART OF A TEST.


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


NORMAL_ATTR0 = 16
NORMAL_ATTR1 = 17
NORMAL_ATTR2 = 'foo'
NORMAL_ATTR3 = 'bar'

OVERRIDDEN_ATTR0 = 61
OVERRIDDEN_ATTR1 = 71
OVERRIDDEN_ATTR2 = 'oof'
OVERRIDDEN_ATTR3 = 'rab'


def test_dynamic_properties():

    def test_thing(test_obj):

        assert test_obj.attr0 == NORMAL_ATTR0
        assert test_obj.attr1 == NORMAL_ATTR1
        assert test_obj.attr2 == NORMAL_ATTR2
        assert test_obj.attr3 == NORMAL_ATTR3

        # attrs = ['attr0', 'attr1', 'attr2', 'attr3']

        # If this were done wrong, we'd bind an inherited attribute
        # and then when we put things back it would become an instance
        # attribute, so we remember what things were originally
        # instance attributes so that we can check later.
        old_attr_dict = test_obj.__dict__.copy()

        # Test of the ordinary case.
        with local_attrs(test_obj, attr0=OVERRIDDEN_ATTR0, attr2=OVERRIDDEN_ATTR2):
            assert test_obj.attr0 == OVERRIDDEN_ATTR0
            assert test_obj.attr1 == NORMAL_ATTR1
            assert test_obj.attr2 == OVERRIDDEN_ATTR2
            assert test_obj.attr3 == NORMAL_ATTR3
        assert test_obj.attr0 == NORMAL_ATTR0
        assert test_obj.attr1 == NORMAL_ATTR1
        assert test_obj.attr2 == NORMAL_ATTR2
        assert test_obj.attr3 == NORMAL_ATTR3

        assert test_obj.__dict__ == old_attr_dict

        # Another test of the ordinary case.
        with local_attrs(test_obj, attr0=OVERRIDDEN_ATTR0, attr1=OVERRIDDEN_ATTR1,
                         attr2=OVERRIDDEN_ATTR2, attr3=OVERRIDDEN_ATTR3):
            assert test_obj.attr0 == OVERRIDDEN_ATTR0
            assert test_obj.attr1 == OVERRIDDEN_ATTR1
            assert test_obj.attr2 == OVERRIDDEN_ATTR2
            assert test_obj.attr3 == OVERRIDDEN_ATTR3
        assert test_obj.attr0 == NORMAL_ATTR0
        assert test_obj.attr1 == NORMAL_ATTR1
        assert test_obj.attr2 == NORMAL_ATTR2
        assert test_obj.attr3 == NORMAL_ATTR3

        # Test case of raising an error and assuring things are still set to normal
        try:
            with local_attrs(test_obj, attr0=OVERRIDDEN_ATTR0, attr2=OVERRIDDEN_ATTR2):
                assert test_obj.attr0 == NORMAL_ATTR0
                assert test_obj.attr1 == NORMAL_ATTR1
                assert test_obj.attr2 == NORMAL_ATTR2
                assert test_obj.attr3 == NORMAL_ATTR3
                raise Exception("This is expected to be caught.")
        except Exception:
            pass
        assert test_obj.attr0 == NORMAL_ATTR0
        assert test_obj.attr1 == NORMAL_ATTR1
        assert test_obj.attr2 == NORMAL_ATTR2
        assert test_obj.attr3 == NORMAL_ATTR3

        # Test case of no attributes set at all
        with local_attrs(object):
            assert test_obj.attr0 == NORMAL_ATTR0
            assert test_obj.attr1 == NORMAL_ATTR1
            assert test_obj.attr2 == NORMAL_ATTR2
            assert test_obj.attr3 == NORMAL_ATTR3
        assert test_obj.attr0 == NORMAL_ATTR0
        assert test_obj.attr1 == NORMAL_ATTR1
        assert test_obj.attr2 == NORMAL_ATTR2
        assert test_obj.attr3 == NORMAL_ATTR3

    class Foo:
        attr0 = NORMAL_ATTR0
        attr1 = NORMAL_ATTR1

        def __init__(self):
            self.attr2 = NORMAL_ATTR2
            self.attr3 = NORMAL_ATTR3

    with pytest.raises(ValueError):
        # Binding attr1 would affect other instances.
        test_thing(Foo())

    class Bar:
        def __init__(self):
            self.attr0 = NORMAL_ATTR0
            self.attr1 = NORMAL_ATTR1
            self.attr2 = NORMAL_ATTR2
            self.attr3 = NORMAL_ATTR3

    test_thing(Bar())

    class Baz:
        attr0 = NORMAL_ATTR0
        attr1 = NORMAL_ATTR1
        attr2 = NORMAL_ATTR2
        attr3 = NORMAL_ATTR3

    test_thing(Baz)

    with pytest.raises(ValueError):
        # Binding attr1 would affect other instances.
        test_thing(Baz())

    for thing in [3, "foo", None]:
        with local_attrs(thing):
            pass  # Just make sure no error occurs when no attributes given


def test_override_dict():

    d = {'foo': 'bar'}
    d_copy = d.copy()

    unique_prop1 = str(uuid.uuid4())
    unique_prop2 = str(uuid.uuid4())
    unique_prop3 = str(uuid.uuid4())

    assert unique_prop1 not in d
    assert unique_prop2 not in d
    assert unique_prop3 not in d

    with override_dict(d, **{unique_prop1: "something", unique_prop2: "anything"}):

        assert unique_prop1 in d  # added
        value1a = d.get(unique_prop1)
        assert value1a == "something"

        assert unique_prop2 in d  # added
        value2a = d.get(unique_prop2)
        assert value2a == "anything"

        assert unique_prop3 not in d

        with override_dict(d, **{unique_prop1: "something_else", unique_prop3: "stuff"}):

            assert unique_prop1 in d  # updated
            value1b = d.get(unique_prop1)
            assert value1b == "something_else"

            assert unique_prop2 in d  # unchanged
            assert d.get(unique_prop2) == value2a

            assert unique_prop3 in d  # added
            assert d.get(unique_prop3) == "stuff"

            with override_dict(d, **{unique_prop1: None}):

                assert unique_prop1 not in d  # removed

                with override_dict(d, **{unique_prop1: None}):

                    assert unique_prop1 not in d  # re-removed

                assert unique_prop1 not in d  # un-re-removed, but still removed

            assert unique_prop1 in d  # restored after double removal
            assert d.get(unique_prop1) == value1b

        assert unique_prop1 in d
        assert d.get(unique_prop1) == value1a

        assert unique_prop2 in d
        assert d.get(unique_prop2) == value2a

        assert unique_prop3 not in d

    assert unique_prop1 not in d
    assert unique_prop2 not in d
    assert unique_prop3 not in d

    assert d == d_copy


def test_override_environ():

    unique_prop1 = str(uuid.uuid4())
    unique_prop2 = str(uuid.uuid4())
    unique_prop3 = str(uuid.uuid4())

    assert unique_prop1 not in os.environ
    assert unique_prop2 not in os.environ
    assert unique_prop3 not in os.environ

    with override_environ(**{unique_prop1: "something", unique_prop2: "anything"}):

        assert unique_prop1 in os.environ  # added
        value1a = os.environ.get(unique_prop1)
        assert value1a == "something"

        assert unique_prop2 in os.environ  # added
        value2a = os.environ.get(unique_prop2)
        assert value2a == "anything"

        assert unique_prop3 not in os.environ

        with override_environ(**{unique_prop1: "something_else", unique_prop3: "stuff"}):

            assert unique_prop1 in os.environ  # updated
            value1b = os.environ.get(unique_prop1)
            assert value1b == "something_else"

            assert unique_prop2 in os.environ  # unchanged
            assert os.environ.get(unique_prop2) == value2a

            assert unique_prop3 in os.environ  # added
            assert os.environ.get(unique_prop3) == "stuff"

            with override_environ(**{unique_prop1: None}):

                assert unique_prop1 not in os.environ  # removed

                with override_environ(**{unique_prop1: None}):

                    assert unique_prop1 not in os.environ  # re-removed

                assert unique_prop1 not in os.environ  # un-re-removed, but still removed

            assert unique_prop1 in os.environ  # restored after double removal
            assert os.environ.get(unique_prop1) == value1b

        assert unique_prop1 in os.environ
        assert os.environ.get(unique_prop1) == value1a

        assert unique_prop2 in os.environ
        assert os.environ.get(unique_prop2) == value2a

        assert unique_prop3 not in os.environ

    assert unique_prop1 not in os.environ
    assert unique_prop2 not in os.environ
    assert unique_prop3 not in os.environ


def test_controlled_time_creation():

    t = ControlledTime()

    assert t.just_now() == t.INITIAL_TIME

    with pytest.raises(ValueError):  # expecting a datetime
        ControlledTime(initial_time=1)  # noqa

    with pytest.raises(ValueError):  # expecting the datetime has no timezone
        ControlledTime(initial_time=datetime.datetime(2019, 1, 1, tzinfo=pytz.UTC))

    with pytest.raises(ValueError):  # expecting an int or float
        ControlledTime(tick_seconds="whatever")  # noqa


def test_controlled_time_just_now():

    t = ControlledTime()

    t0 = t.just_now()
    t1 = t.just_now()
    assert (t1 - t0).total_seconds() == 0


def test_controlled_time_now():

    t = ControlledTime()
    t0 = t.just_now()

    t1 = t.now()
    t2 = t.now()
    t3 = t.now()

    assert (t1 - t0).total_seconds() == 1
    assert (t2 - t0).total_seconds() == 2
    assert (t3 - t0).total_seconds() == 3


def test_controlled_time_utcnow():

    hour = 60 * 60  # 60 seconds * 60 minutes

    eastern_time = pytz.timezone("US/Eastern")
    t0 = datetime.datetime(2020, 1, 1, 0, 0, 0)
    t = ControlledTime(initial_time=t0, local_timezone=eastern_time)

    t1 = t.now()     # initial time + 1 second
    t.set_datetime(t0)
    t2 = t.utcnow()  # initial time UTC + 1 second
    # US/Eastern on 2020-01-01 is not daylight time, so EST (-0500) not EDT (-0400).
    assert (t2 - t1).total_seconds() == 5 * hour


def test_controlled_time_reset_datetime():

    t = ControlledTime()
    t0 = t.just_now()

    for i in range(5):
        t.now()  # tick the clock 5 times

    assert (t.just_now() - t0).total_seconds() == 5

    t.reset_datetime()
    assert (t.just_now() - t0).total_seconds() == 0


def test_controlled_time_set_datetime():

    t = ControlledTime()
    t0 = t.just_now()

    t.set_datetime(t0 + datetime.timedelta(seconds=5))
    assert (t.just_now() - t0).total_seconds() == 5

    with pytest.raises(ValueError):
        t.set_datetime(17)  # Not a datetime

    with pytest.raises(ValueError):
        t.set_datetime(datetime.datetime(2015, 1, 1, 1, 2, 3, tzinfo=pytz.timezone("US/Pacific")))


def test_controlled_time_sleep():

    t = ControlledTime()
    t0 = t.just_now()

    t.sleep(10)

    assert (t.just_now() - t0).total_seconds() == 10


def test_controlled_time_documentation_scenario():

    start_time = datetime.datetime.now()

    def sleepy_function():
        time.sleep(10)

    dt = ControlledTime()
    with mock.patch("datetime.datetime", dt):
        with mock.patch("time.sleep", dt.sleep):
            t0 = datetime.datetime.now()
            sleepy_function()  # sleeps 10 seconds
            t1 = datetime.datetime.now()  # 1 more second increments
            assert (t1 - t0).total_seconds() == 11  # 11 virtual seconds have passed

    end_time = datetime.datetime.now()
    # In reality, whole test takes much less than one second...
    assert (end_time - start_time).total_seconds() < 0.5


def test_notice_pytest_fixtures_part_1():

    with pytest.raises(MockMathError):
        MockMath.add(2, 2)


def test_notice_pytest_fixtures_part_2(math_enabled):

    notice_pytest_fixtures(math_enabled)  # Use care if editing this line. It is PART OF A TEST.

    assert MockMath.add(2, 2) == 4


THIS_TEST_FILE = __file__


def test_notice_pytest_fixtures_part_3():

    # This test will call out to a subprocess to check that this file passes flake8 tests.
    # So please keep the file up-to-date. :)
    # Then if it passes, it will filter out lines containing 'PART OF A TEST' (including this one)
    # and show that their absence causes flake8 warnings.

    line_filter_marker = '[P]ART OF A TEST'  # Using '[P]' instead of 'P' assures this line won't match.

    def get_output(command):
        print('command=')
        print(command)
        try:
            code = 0
            output = subprocess.check_output(["bash", "-c", command])
        except subprocess.CalledProcessError as e:
            code = e.returncode
            output = e.output
        output = output.decode('utf-8')
        print("output=")
        print(output)
        return code, output

    template = "cat '%s' %s | flake8 - --ignore=E303"  # ignore E303 (blank lines) caused by filtering

    # This shows the file passes cleanly. If this fails, someone has let this file get sloppy. Fix that first.
    code, output = get_output(template % (THIS_TEST_FILE, ""))
    assert code == 0
    assert output == ""

    # This shows that if your remove the declaration, it leads to annoying errors from flake8 about fixtures.
    declaration_usage_filter = "| sed '/%s/d' " % line_filter_marker
    code, output = get_output(template % (THIS_TEST_FILE, declaration_usage_filter))
    assert code == 1
    warnings = output.strip().split('\n')
    assert len(warnings) == 2  # a global warning about the import, and a local warning about a bound variable
    for line in warnings:
        assert "unused" in line  # allow for some variability in message wording, but should be about something unused


def test_occasionally():

    def add1(x):
        return x + 1

    # Test that Occasionally(fn) == Occasionally(fn, failure_frequency=2)

    # This is the same as supplying failure_frequency=2, implementing succeed, fail, succeed, fail, ...
    # So it works on try 0, 2, 4, ... and it fails on try 1, 3, 5, ...

    flaky_add1 = Occasionally(add1)

    # SUCCESS (first attempt)
    assert flaky_add1(1) == 2

    # FAILURE (second attempt)
    with pytest.raises(Exception):
        assert flaky_add1(1) == 2

    # SUCCESS (third attempt)
    assert flaky_add1(2) == 3

    # FAILURE (fourth attempt)
    with pytest.raises(Exception):
        assert flaky_add1(2) == 3

    # Test that Occasionally(fn, success_frequency=2) does fail, succeess, fail, succeed, ...

    # Our function sometimes_add1 will WORK every other time, since the default frequency is 2.
    sometimes_add1 = Occasionally(add1, success_frequency=2)

    # FAILURE (first attempt)
    try:
        assert sometimes_add1(1) == 2
    except Exception as e:
        msg = str(e)
        assert msg == Occasionally.DEFAULT_ERROR_MESSAGE

    # SUCCESS (second attempt)
    assert sometimes_add1(1) == 2

    # FAILURE (third attempt)
    with pytest.raises(Exception):
        assert sometimes_add1(2) == 3

    # SUCCESS (fourth attempt)
    assert sometimes_add1(2) == 3

    # Test that Occasionally(fn, success_frequency=3) does fail, fail, succeed, fail, fail, succeed, ...

    # Our function occasionally_add1 will WORK every third time, since the default frequency is 3.
    occasionally_add1 = Occasionally(add1, success_frequency=3)

    # FAILURE (first time)
    with pytest.raises(Exception):
        assert occasionally_add1(2) == 3

    # FAILURE (second time)
    with pytest.raises(Exception):
        assert occasionally_add1(2) == 3

    # SUCCESS (third time)
    assert occasionally_add1(2) == 3

    # Test that Occasionally(fn, failure_frequency=3) does succeed, succeed, fail, succeed, succeed, fail, ...

    # Our function mostly_add1 will FAIL every third time, since the default frequency is 3.
    mostly_add1 = Occasionally(add1, failure_frequency=3)

    # This will work for a while...
    assert mostly_add1(1) == 2
    assert mostly_add1(2) == 3
    # But third time is going to fail...
    with pytest.raises(Exception):
        assert mostly_add1(3) == 4

    # This will work for a while...
    assert mostly_add1(1) == 2
    assert mostly_add1(2) == 3

    # Interrupt the sequence before it fails, and reset the sequence.
    mostly_add1.reset()

    # Now that the object has been reset, it'll work a bit longer

    assert mostly_add1(3) == 4
    assert mostly_add1(4) == 5

    # But third attempt is going to fail...
    with pytest.raises(Exception):
        assert mostly_add1(5) == 6


def test_occasionally_errors():

    try:
        # The first call to this function will err.
        fail_for_a_while = Occasionally(lambda x: x + 1, success_frequency=10,
                                        error_class=SyntaxError,
                                        error_message="something")
        fail_for_a_while(1)
    except Exception as e:
        assert type(e) == SyntaxError
        assert str(e) == "something"


def test_retry_manager():

    def adder(n):
        def addn(x):
            return x + n
        return addn

    sometimes_add2 = Occasionally(adder(2), success_frequency=2)

    try:
        assert sometimes_add2(1) == 3
    except Exception as e:
        msg = str(e)
        assert msg == Occasionally.DEFAULT_ERROR_MESSAGE
    assert sometimes_add2(1) == 3
    with pytest.raises(Exception):
        assert sometimes_add2(2) == 4
    assert sometimes_add2(2) == 4

    sometimes_add2.reset()

    @Retry.retry_allowed(retries_allowed=1)
    def reliably_add2(x):
        return sometimes_add2(x)

    assert reliably_add2(1) == 3
    assert reliably_add2(2) == 4
    assert reliably_add2(3) == 5

    rarely_add3 = Occasionally(adder(3), success_frequency=5)

    with pytest.raises(Exception):
        assert rarely_add3(1) == 4
    with pytest.raises(Exception):
        assert rarely_add3(1) == 4
    with pytest.raises(Exception):
        assert rarely_add3(1) == 4
    with pytest.raises(Exception):
        assert rarely_add3(1) == 4
    assert rarely_add3(1) == 4  # 5th time's a charm

    rarely_add3.reset()

    # NOTE WELL: For testing, we chose 1.25 to use factors of 2 so floating point can exactly compare

    @Retry.retry_allowed(retries_allowed=4, wait_seconds=2, wait_multiplier=1.25)
    def reliably_add3(x):
        return rarely_add3(x)

    # We have to access a random place out of a tuple structure for mock data on time.sleep's arg.
    # Documentation says we should be able to access the call with .call_args[n] but that doesn't work
    # and it's also documented to work by tuple, so .mock_calls[n][1][m] substitutes for
    # .mock_calls[n].call_args[m], but using .mock_calls[n][ARGS][m] as the compromise. -kmp 20-May-2020

    ARGS = 1  # noqa - yeah, this is all uppercase, but we only need this constant locally

    with mock.patch("time.sleep") as mock_sleep:

        assert reliably_add3(1) == 4

        assert mock_sleep.call_count == 4

        assert mock_sleep.mock_calls[0][ARGS][0] == 2
        assert mock_sleep.mock_calls[1][ARGS][0] == 2.5      # 2 * 1.25
        assert mock_sleep.mock_calls[2][ARGS][0] == 3.125    # 2 * 1.25 ** 2
        assert mock_sleep.mock_calls[3][ARGS][0] == 3.90625  # 2 * 1.25 ** 3

        assert reliably_add3(2) == 5
        assert mock_sleep.call_count == 8

        assert mock_sleep.mock_calls[4][ARGS][0] == 2
        assert mock_sleep.mock_calls[5][ARGS][0] == 2.5      # 2 * 1.25
        assert mock_sleep.mock_calls[6][ARGS][0] == 3.125    # 2 * 1.25 ** 2
        assert mock_sleep.mock_calls[7][ARGS][0] == 3.90625  # 2 * 1.25 ** 3

        # Note that this does not change the wait multiplier, but does exercise the code that processes it,
        # showing that it is doing the same thing as before.
        with RetryManager.retry_options('reliably_add3', retries_allowed=3, wait_seconds=5, wait_multiplier=1.25):

            mock_sleep.reset_mock()
            assert mock_sleep.call_count == 0

            for i in range(10):
                # In this context, we won't retry enough to succeed...
                rarely_add3.reset()
                with pytest.raises(Exception):
                    reliably_add3(1)

            # All the sleep calls will be the same 5, 6.25, 7.8125 progression
            assert mock_sleep.call_count == 30
            for i in range(0, 30, 3):  # start, stop, step
                assert mock_sleep.mock_calls[i][ARGS][0] == 5
                assert mock_sleep.mock_calls[i + 1][ARGS][0] == 6.25
                assert mock_sleep.mock_calls[i + 2][ARGS][0] == 7.8125

            mock_sleep.reset_mock()
            assert mock_sleep.call_count == 0

            with RetryManager.retry_options('reliably_add3', wait_seconds=7):

                for i in range(10):
                    # In this context, we won't retry enough to succeed...
                    rarely_add3.reset()
                    with pytest.raises(Exception):
                        reliably_add3(1)

                # Now the sleep calls will be the same 7,  8.75, 10.9375 progression
                assert mock_sleep.call_count == 30
                for i in range(0, 30, 3):  # start, stop, step
                    assert mock_sleep.mock_calls[i][ARGS][0] == 7
                    assert mock_sleep.mock_calls[i + 1][ARGS][0] == 8.75
                    assert mock_sleep.mock_calls[i + 2][ARGS][0] == 10.9375

            mock_sleep.reset_mock()
            assert mock_sleep.call_count == 0

            with RetryManager.retry_options('reliably_add3', wait_seconds=7, wait_increment=1):

                for i in range(10):
                    # In this context, we won't retry enough to succeed...
                    rarely_add3.reset()
                    with pytest.raises(Exception):
                        reliably_add3(1)

                # Now the sleep calls will be the same 7,  8.75, 10.9375 progression
                assert mock_sleep.call_count == 30
                for i in range(0, 30, 3):  # start, stop, step
                    assert mock_sleep.mock_calls[i][ARGS][0] == 7
                    assert mock_sleep.mock_calls[i + 1][ARGS][0] == 8
                    assert mock_sleep.mock_calls[i + 2][ARGS][0] == 9

        with pytest.raises(ValueError):

            # The name-key must not be a number.
            with RetryManager.retry_options(name_key=17, retries_allowed=3, wait_seconds=5):
                pass

        with pytest.raises(ValueError):

            # The name-key must not be a number.
            with RetryManager.retry_options(17, retries_allowed=3, wait_seconds=5):
                pass

        with pytest.raises(ValueError):

            # The name-key must be registered
            with RetryManager.retry_options(name_key="not-a-registered-name", retries_allowed=3, wait_seconds=5):
                pass


def test_mock_file_system():

    fs = MockFileSystem()

    with mock.patch.object(io, "open") as mock_open:
        with mock.patch.object(os.path, "exists") as mock_exists:
            with mock.patch.object(os, "remove") as mock_remove:

                mock_open.side_effect = fs.open
                mock_exists.side_effect = fs.exists
                mock_remove.side_effect = fs.remove

                filename = "no.such.file"
                assert os.path.exists(filename) is False

                with pytest.raises(AssertionError):
                    with io.open(filename, 'q'):
                        pass

                with io.open(filename, 'w') as fp:
                    fp.write("foo")
                    fp.write("bar")

                assert os.path.exists(filename) is True

                with io.open(filename, 'r') as fp:
                    assert fp.read() == 'foobar'

                with io.open(filename, 'r') as fp:
                    assert fp.read() == 'foobar'

                assert os.path.exists(filename) is True

                os.remove(filename)

                assert os.path.exists(filename) is False

                with pytest.raises(FileNotFoundError):
                    os.remove(filename)

                with pytest.raises(FileNotFoundError):
                    io.open(filename, 'r')

                with io.open(filename, 'wb') as fp:
                    fp.write(b'foo')
                    fp.write(b'bar')
                    fp.write(bytes((10, 65, 66, 67, 10)))  # Unicode Newline, A, B, C, Newline
                    fp.write(b'a b c')
                    fp.write(b'\n')

                assert os.path.exists(filename)

                with io.open(filename, 'rb') as fp:
                    assert fp.read() == b'foobar\nABC\na b c\n'

                with io.open(filename, 'r') as fp:
                    assert [line.rstrip('\n') for line in fp] == ['foobar', 'ABC', 'a b c']

                os.remove(filename)

                assert not os.path.exists(filename)


class _MockPrinter:

    def __init__(self):
        self.printed = []

    def mock_print(self, *args):
        self.printed.append(" ".join(args))

    def reset(self):
        self.printed = []


def test_show_elapsed_time():

    mock_printer = _MockPrinter()

    with mock.patch.object(qa_utils, "PRINT", mock_printer.mock_print):
        show_elapsed_time(1.0, 5.625)
        show_elapsed_time(6, 7)

        assert mock_printer.printed == ["Elapsed: 4.625", "Elapsed: 1"]


class _MockTime:

    def __init__(self, start=0.0, tick=1.0):
        self.elapsed = start
        self.tick = tick

    def time(self):
        self.elapsed = now = self.elapsed + self.tick
        return now


def test_timed():

    mocked_printer = _MockPrinter()
    mocked_time = _MockTime()

    with mock.patch.object(qa_utils, "PRINT", mocked_printer.mock_print):
        with mock.patch.object(time, 'time', mocked_time.time):

            with timed():
                pass
            assert mocked_printer.printed == ["Elapsed: 1.0"]

            mocked_printer.reset()

            with timed():
                time.time()
                time.time()
            assert mocked_printer.printed == ["Elapsed: 3.0"]

            mocked_printer.reset()

            stuff = []

            with timed(reporter=lambda x, y: stuff.append(y - x)):
                time.time()
                time.time()
            assert mocked_printer.printed == []
            assert stuff == [3.0]

            mocked_printer.reset()

            stuff = []

            def my_debugger(x):
                assert isinstance(x, RuntimeError)
                assert str(x) == "Foo"

            success = False
            try:
                with timed(reporter=lambda x, y: stuff.append(y - x), debug=my_debugger):
                    time.time()
                    raise RuntimeError("Foo")
            except RuntimeError:
                success = True

            assert mocked_printer.printed == []
            assert stuff == [2.0]
            assert success, "RuntimeError was not caught."


def test_not_really_random():

    r = NotReallyRandom()
    assert [r.randint(3, 5) for _ in range(10)] == [3, 4, 5, 3, 4, 5, 3, 4, 5, 3]


def test_mock_response():

    # Cannot specify both json and content
    with pytest.raises(Exception):
        MockResponse(200, content="foo", json={"foo": "bar"})

    ok_empty_response = MockResponse(status_code=200)

    assert ok_empty_response.content == ""

    with pytest.raises(Exception):
        ok_empty_response.json()

    assert str(ok_empty_response) == '<MockResponse 200>'

    ok_empty_response.raise_for_status()  # This should raise no error

    ok_response = MockResponse(status_code=200, json={'foo': 'bar'})

    assert ok_response.status_code == 200
    assert ok_response.json() == {'foo': 'bar'}

    assert str(ok_response) == '<MockResponse 200 {"foo": "bar"}>'

    ok_response.raise_for_status()  # This should raise no error

    ok_non_json_response = MockResponse(status_code=200, content="foo")

    assert ok_non_json_response.status_code == 200
    assert ok_non_json_response.content == "foo"
    with pytest.raises(Exception):
        ok_non_json_response.json()

    error_response = MockResponse(status_code=400, json={'message': 'bad stuff'})

    assert error_response.status_code == 400
    assert error_response.json() == {'message': "bad stuff"}

    assert str(error_response) == '<MockResponse 400 {"message": "bad stuff"}>'

    with pytest.raises(Exception):
        error_response.raise_for_status()


def test_uppercase_print_with_printed_output():

    with printed_output() as printed:

        assert printed.lines == []

        PRINT("foo")
        assert printed.lines == ["foo"]
        assert printed.last == "foo"

        PRINT("bar")
        assert printed.lines == ["foo", "bar"]
        assert printed.last == "bar"


def test_uppercase_print_with_time():

    # Test uses WITHOUT timestamps
    with printed_output() as printed:

        assert printed.lines == []
        assert printed.last is None

        PRINT("This", "is", "a", "test.")

        assert printed.lines == ["This is a test."]
        assert printed.last == "This is a test."

        PRINT("This, too.")

        assert printed.lines == ["This is a test.", "This, too."]
        assert printed.last == "This, too."

    timestamp_pattern = re.compile(r'^[0-9][0-9]:[0-9][0-9]:[0-9][0-9] (.*)$')

    # Test uses WITH timestamps
    with printed_output() as printed:

        PRINT("This", "is", "a", "test.", timestamped=True)
        PRINT("This, too.", timestamped=True)

        trimmed = []
        for line in printed.lines:
            matched = timestamp_pattern.match(line)
            assert matched, "Timestamp missing or in bad form: %s" % line
            trimmed.append(matched.group(1))

        assert trimmed == ["This is a test.", "This, too."]

    with printed_output() as printed:

        PRINT("This", "is", "a", "test.", timestamped=True)
        PRINT("This, too.")

        line0, line1 = printed.lines

        assert timestamp_pattern.match(line0)
        assert not timestamp_pattern.match(line1)


def test_mock_boto_s3_client_upload_file_and_download_file_positional():

    mock_s3_client = MockBotoS3Client()
    local_mfs = MockFileSystem()

    # Check positionally

    with mock.patch("io.open", local_mfs.open):

        with io.open("file1.txt", 'w') as fp:
            fp.write('Hello!\n')

        assert local_mfs.files == {"file1.txt": b"Hello!\n"}
        assert mock_s3_client.s3_files.files == {}

        mock_s3_client.upload_file("file1.txt", "MyBucket", "MyFile")

        assert local_mfs.files == {"file1.txt": b"Hello!\n"}
        assert mock_s3_client.s3_files.files == {'MyBucket/MyFile': b"Hello!\n"}

        mock_s3_client.download_file("MyBucket", "MyFile", "file2.txt")

        assert local_mfs.files == {
            "file1.txt": b"Hello!\n",
            "file2.txt": b"Hello!\n",
        }
        assert mock_s3_client.s3_files.files == {'MyBucket/MyFile': b"Hello!\n"}

        assert file_contents("file1.txt") == file_contents("file2.txt")


def test_mock_boto_s3_client_upload_file_and_download_file_keyworded():

    mock_s3_client = MockBotoS3Client()
    local_mfs = MockFileSystem()

    with mock.patch("io.open", local_mfs.open):

        with io.open("file1.txt", 'w') as fp:
            fp.write('Hello!\n')

        assert local_mfs.files == {"file1.txt": b"Hello!\n"}
        assert mock_s3_client.s3_files.files == {}

        mock_s3_client.upload_file(Filename="file1.txt", Bucket="MyBucket", Key="MyFile")

        assert local_mfs.files == {"file1.txt": b"Hello!\n"}
        assert mock_s3_client.s3_files.files == {'MyBucket/MyFile': b"Hello!\n"}

        mock_s3_client.download_file(Bucket="MyBucket", Key="MyFile", Filename="file2.txt")

        assert local_mfs.files == {
            "file1.txt": b"Hello!\n",
            "file2.txt": b"Hello!\n",
        }
        assert mock_s3_client.s3_files.files == {'MyBucket/MyFile': b"Hello!\n"}

        assert file_contents("file1.txt") == file_contents("file2.txt")


def test_mock_boto_s3_client_upload_fileobj_and_download_fileobj_positional():

    mock_s3_client = MockBotoS3Client()
    local_mfs = MockFileSystem()

    # Check positionally

    with mock.patch("io.open", local_mfs.open):

        with io.open("file1.txt", 'w') as fp:
            fp.write('Hello!\n')

        assert local_mfs.files == {"file1.txt": b"Hello!\n"}
        assert mock_s3_client.s3_files.files == {}

        with io.open("file1.txt", 'rb') as fp:
            mock_s3_client.upload_fileobj(fp, "MyBucket", "MyFile")

        assert local_mfs.files == {"file1.txt": b"Hello!\n"}
        assert mock_s3_client.s3_files.files == {'MyBucket/MyFile': b"Hello!\n"}

        with io.open("file2.txt", 'wb') as fp:
            mock_s3_client.download_fileobj("MyBucket", "MyFile", fp)

        assert local_mfs.files == {
            "file1.txt": b"Hello!\n",
            "file2.txt": b"Hello!\n",
        }
        assert mock_s3_client.s3_files.files == {'MyBucket/MyFile': b"Hello!\n"}

        assert file_contents("file1.txt") == file_contents("file2.txt")


def test_mock_boto_s3_client_upload_fileobj_and_download_fileobj_keyworded():

    mock_s3_client = MockBotoS3Client()
    local_mfs = MockFileSystem()

    with mock.patch("io.open", local_mfs.open):

        with io.open("file1.txt", 'w') as fp:
            fp.write('Hello!\n')

        assert local_mfs.files == {"file1.txt": b"Hello!\n"}
        assert mock_s3_client.s3_files.files == {}

        with io.open("file1.txt", 'rb') as fp:
            mock_s3_client.upload_fileobj(Fileobj=fp, Bucket="MyBucket", Key="MyFile")

        assert local_mfs.files == {"file1.txt": b"Hello!\n"}
        assert mock_s3_client.s3_files.files == {'MyBucket/MyFile': b"Hello!\n"}

        with io.open("file2.txt", 'wb') as fp:
            mock_s3_client.download_fileobj(Bucket="MyBucket", Key="MyFile", Fileobj=fp)

        assert local_mfs.files == {
            "file1.txt": b"Hello!\n",
            "file2.txt": b"Hello!\n",
        }
        assert mock_s3_client.s3_files.files == {'MyBucket/MyFile': b"Hello!\n"}

        assert file_contents("file1.txt") == file_contents("file2.txt")


def test_mock_boto_s3_client_limitations():

    mock_s3_client = MockBotoS3Client()

    local_fs = MockFileSystem()

    with mock.patch("io.open", local_fs.open):

        with pytest.raises(MockKeysNotImplemented):
            mock_s3_client.upload_file(Filename="foo", Bucket="bucketname", Key="keyname",
                                       Config='not-implemented')

        with pytest.raises(MockKeysNotImplemented):
            mock_s3_client.upload_fileobj(Fileobj=io.BytesIO(), Bucket="bucketname", Key="keyname",
                                          Config='not-implemented')

        with pytest.raises(MockKeysNotImplemented):
            mock_s3_client.download_file(Filename="foo", Bucket="bucketname", Key="keyname",
                                         Config='not-implemented')

        with pytest.raises(MockKeysNotImplemented):
            mock_s3_client.download_fileobj(Fileobj=io.BytesIO(), Bucket="bucketname", Key="keyname",
                                            Config='not-implemented')


def test_mock_keys_not_implemented():

    err = MockKeysNotImplemented(keys=['foo', 'bar'], operation="some-operation")
    assert str(err) == 'Mocked some-operation does not implement keywords: foo, bar'


class SampleClass:

    def __init__(self, favorite_fruit):
        self.favorite_fruit = favorite_fruit

    FAVORITE_COLOR = CustomizableProperty('FAVORITE_COLOR', description="the string name of a color")


class SampleClass2(SampleClass):

    FAVORITE_SONG = CustomizableProperty('FAVORITE_SONG', description="the string name of a song")


class SampleClass3(SampleClass2):

    FAVORITE_COLOR = 'blue'
    FAVORITE_SONG = 'Jingle Bells'


def test_find_field_declared_class():

    thing = SampleClass2(favorite_fruit='orange')

    [kind, value] = UncustomizedInstance._find_field_declared_class(thing, 'favorite_fruit')
    assert kind == 'instance'
    assert value == 'orange'

    [kind, value] = UncustomizedInstance._find_field_declared_class(thing, 'FAVORITE_SONG')
    assert kind == SampleClass2
    ignored(value)

    [kind, value] = UncustomizedInstance._find_field_declared_class(thing, 'FAVORITE_COLOR')
    assert kind == SampleClass
    ignored(value)

    with pytest.raises(RuntimeError):
        # If we were to search for something that simply wasn't a property, customizable or not,
        # the search would fail and a RuntimeError would be raised.
        UncustomizedInstance._find_field_declared_class(thing, 'FAVORITE_SHOW')


def test_uncustomized_instance():

    thing = SampleClass2(favorite_fruit=CustomizableProperty('favorite_fruit',
                                                             description="the string name of a fruit"))

    assert str(UncustomizedInstance(thing, field='FAVORITE_SONG')) == (
        "Attempt to access field FAVORITE_SONG from class test.test_qa_utils.SampleClass2."
        " It was expected to be given a custom value in a subclass: the string name of a song."
    )

    assert str(UncustomizedInstance(thing, field='FAVORITE_COLOR')) == (
        "Attempt to access field FAVORITE_COLOR from class test.test_qa_utils.SampleClass."
        " It was expected to be given a custom value in a subclass: the string name of a color."
    )

    assert str(UncustomizedInstance(thing, field='favorite_fruit')) == (
        "Attempt to access field favorite_fruit from instance."
        " It was expected to be given a custom value in a subclass: the string name of a fruit."
    )


def test_customized_instance():

    uncustomized_thing = SampleClass2(favorite_fruit=CustomizableProperty('favorite_fruit',
                                                                          description="the string name of a fruit"))

    # It doesn't work to store these things directly in the slot
    assert isinstance(uncustomized_thing.favorite_fruit, CustomizableProperty)
    assert isinstance(getattr(uncustomized_thing, "favorite_fruit"), CustomizableProperty)
    # But this will spot it...
    with raises_regexp(UncustomizedInstance, "Attempt to access field favorite_fruit from instance."
                                             " It was expected to be given a custom value in a subclass:"
                                             " the string name of a fruit."):
        print(getattr_customized(uncustomized_thing, "favorite_fruit"))

    with raises_regexp(UncustomizedInstance, "Attempt to access field FAVORITE_COLOR"
                                             " from class test.test_qa_utils.SampleClass."
                                             " It was expected to be given a custom value in a subclass:"
                                             " the string name of a color."):
        print(uncustomized_thing.FAVORITE_COLOR)
    with raises_regexp(UncustomizedInstance, "Attempt to access field FAVORITE_COLOR"
                                             " from class test.test_qa_utils.SampleClass."
                                             " It was expected to be given a custom value in a subclass:"
                                             " the string name of a color."):
        print(getattr(uncustomized_thing, "FAVORITE_COLOR"))
    with raises_regexp(UncustomizedInstance, "Attempt to access field FAVORITE_COLOR"
                                             " from class test.test_qa_utils.SampleClass."
                                             " It was expected to be given a custom value in a subclass:"
                                             " the string name of a color."):
        print(getattr_customized(uncustomized_thing, "FAVORITE_COLOR"))

    with raises_regexp(UncustomizedInstance, "Attempt to access field FAVORITE_SONG"
                                             " from class test.test_qa_utils.SampleClass2."
                                             " It was expected to be given a custom value in a subclass:"
                                             " the string name of a song."):
        print(uncustomized_thing.FAVORITE_SONG)
    with raises_regexp(UncustomizedInstance, "Attempt to access field FAVORITE_SONG"
                                             " from class test.test_qa_utils.SampleClass2."
                                             " It was expected to be given a custom value in a subclass:"
                                             " the string name of a song."):
        print(getattr(uncustomized_thing, "FAVORITE_SONG"))
    with raises_regexp(UncustomizedInstance, "Attempt to access field FAVORITE_SONG"
                                             " from class test.test_qa_utils.SampleClass2."
                                             " It was expected to be given a custom value in a subclass:"
                                             " the string name of a song."):
        print(getattr_customized(uncustomized_thing, "FAVORITE_SONG"))

    customized_thing = SampleClass3(favorite_fruit='orange')

    assert customized_thing.favorite_fruit == 'orange'
    assert customized_thing.FAVORITE_COLOR == 'blue'
    assert customized_thing.FAVORITE_SONG == 'Jingle Bells'


def test_raises_regexp():

    class MyRuntimeError(RuntimeError):
        pass

    with raises_regexp(RuntimeError, "This.*test!"):
        raise RuntimeError("This is a test!")

    with raises_regexp(RuntimeError, "This.*test!"):
        raise MyRuntimeError("This is a test!")

    with pytest.raises(AssertionError):
        # This will fail because the inner error has a period, not an exclamation mark, terminating it.
        # That will cause it to raise an AssertionError instead.
        with raises_regexp(RuntimeError, "This.*test!"):
            raise MyRuntimeError("This is a test.")

    with pytest.raises(Exception):
        # This will fail because the inner error is a KeyError, not a RuntimeError.
        # I WISH this would raise AssertionError, but pytest lets the KeyError through.
        # I am not sure that's the same as what unittest does in this case but it will
        # suffice for now. -kmp 6-Oct-2020
        with raises_regexp(RuntimeError, "This.*test!"):
            raise KeyError('This is a test!')
