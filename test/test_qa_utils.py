import logging

import boto3
import datetime
import io
import json
import os
import platform
import pytest
import pytz
import re
import subprocess
import sys
import time
import uuid

from dcicutils import qa_utils
from dcicutils.exceptions import ExpectedErrorNotSeen, WrongErrorSeen, UnexpectedErrorAfterFix
from dcicutils.ff_mocks import mocked_s3utils
from dcicutils.lang_utils import there_are
from dcicutils.misc_utils import Retry, PRINT, file_contents, REF_TZ, local_attrs, ignored
from dcicutils.qa_utils import (
    mock_not_called, override_environ, override_dict, show_elapsed_time, timed, is_subdict,
    ControlledTime, Occasionally, RetryManager, MockFileSystem, NotReallyRandom, MockUUIDModule, MockedCommandArgs,
    MockResponse, printed_output, MockBotoS3Client, MockKeysNotImplemented, MockBoto3, known_bug_expected,
    raises_regexp, VersionChecker, check_duplicated_items_by_key, guess_local_timezone_for_testing,
    logged_messages, input_mocked, ChangeLogChecker, MockLog, MockId, Eventually, Timer,
    MockObjectBasicAttributeBlock, MockObjectAttributeBlock, MockObjectDeleteMarker, MockTemporaryRestoration,
    MockBoto3IamUserAccessKeyPair, MockBoto3IamUserAccessKeyPairCollection,
    MockBoto3IamUser, MockBoto3IamUserCollection, MockBoto3IamRoleCollection, MockBoto3IamRole,
)
# The following line needs to be separate from other imports. It is PART OF A TEST.
from dcicutils.qa_utils import notice_pytest_fixtures   # Use care if editing this line. It is PART OF A TEST.
from typing import List, Dict, Literal
from unittest import mock
from .fixtures.sample_fixtures import MockMathError, MockMath, math_enabled


notice_pytest_fixtures(math_enabled)   # Use care if editing this line. It is PART OF A TEST.


logger = logging.getLogger(__name__)


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


class MockLocalTimezone:  # Technically should return pytz.tzinfo but doesn't

    def __init__(self, summer_tz, winter_tz):
        self._summer_tz = summer_tz
        self._winter_tz = winter_tz

    def tzname(self, dt: datetime.datetime):
        # The exact time that daylight time runs varies from year to year. For testing, we'll say that
        # daylight time is April 1 to Oct 31.  In practice, we recommend times close to Dec 31 for winter
        # and Jun 30 for summer, so the precise transition date doesn't matter. -kmp 9-Mar-2021
        if 3 < dt.month < 11:
            return self._summer_tz
        else:
            return self._winter_tz


def test_guess_local_timezone_for_testing_contextually():

    if platform.system() == 'Darwin':
        assert guess_local_timezone_for_testing() == REF_TZ
    else:
        assert guess_local_timezone_for_testing() == pytz.UTC


def test_guess_local_timezone_for_testing():

    with mock.patch.object(qa_utils.dateutil_tz, "tzlocal") as mock_tzlocal:  # noQA

        mock_tzlocal.side_effect = lambda: MockLocalTimezone(summer_tz='GMT', winter_tz='GMT')
        assert guess_local_timezone_for_testing() == pytz.UTC

        mock_tzlocal.side_effect = lambda: MockLocalTimezone(summer_tz='EDT', winter_tz='EST')
        guess = guess_local_timezone_for_testing()
        assert guess == pytz.timezone("US/Eastern")

        mock_tzlocal.side_effect = lambda: MockLocalTimezone(summer_tz='MST', winter_tz='MST')
        guess = guess_local_timezone_for_testing()
        assert guess == pytz.timezone("MST")

        mock_tzlocal.side_effect = lambda: MockLocalTimezone(summer_tz='CEST', winter_tz='CET')
        guess = guess_local_timezone_for_testing()
        assert guess == pytz.timezone("CET")

        with pytest.raises(Exception):
            # Unknown times that disagree will fail.
            mock_tzlocal.side_effect = lambda: MockLocalTimezone(summer_tz='GMT', winter_tz='BST')
            guess_local_timezone_for_testing()


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

    t = ControlledTime(tick_seconds=1)

    t0 = t.just_now()
    t1 = t.just_now()
    assert (t1 - t0).total_seconds() == 0

    t0 = t.time()
    t1 = t.time()  # one second should have passed
    t2 = t.time()  # one more second should have passed

    assert t1 - t0 == 1
    assert t2 - t1 == 1


def test_just_utcnow():

    t = ControlledTime()
    t0 = t.utcnow()
    assert t.just_utcnow() == t0

    assert ControlledTime.ProxyDatetimeClass(t).utcnow() == t0 + datetime.timedelta(seconds=1)


def test_controlled_time_time():

    t = ControlledTime()

    t0 = t.time()
    t1 = t.time()

    assert t1 - t0 == 1


def test_controlled_time_now():

    t = ControlledTime()
    t0 = t.just_now()

    t1 = t.now()
    t2 = t.now()
    t3 = t.now()

    assert (t1 - t0).total_seconds() == 1
    assert (t2 - t0).total_seconds() == 2
    assert (t3 - t0).total_seconds() == 3


def test_controlled_time_utcnow_with_tz():

    hour = 60 * 60  # 60 seconds * 60 minutes

    # This doesn't test that we resolve the timezone correclty, just that if we use a given timezone, it works.
    # We've picked a timezone where daylight time is not likely to be in play.
    eastern_time = pytz.timezone("US/Eastern")
    t0 = datetime.datetime(2020, 1, 1, 0, 0, 0)
    t = ControlledTime(initial_time=t0, local_timezone=eastern_time)

    t1 = t.now()     # initial time + 1 second
    t.set_datetime(t0)
    t2 = t.utcnow()  # initial time UTC + 1 second
    # US/Eastern on 2020-01-01 is not daylight time, so EST (-0500) not EDT (-0400).
    assert (t2 - t1).total_seconds() == 5 * hour
    assert (t2 - t1).total_seconds() == abs(eastern_time.utcoffset(t0).total_seconds())


def test_controlled_time_utcnow():

    # This doesn't test that we resolve the timezone correclty, just that if we use a given timezone, it works.
    # We've picked a timezone where daylight time is not likely to be in play.
    local_time = guess_local_timezone_for_testing()
    t0 = datetime.datetime(2020, 1, 1, 0, 0, 0)
    t = ControlledTime(initial_time=t0)

    t1 = t.now()     # initial time + 1 second
    t.set_datetime(t0)
    t2 = t.utcnow()  # initial time UTC + 1 second
    # This might be 5 hours in US/Eastern at HMS, or it might be 0 hours in UTC on AWS or GitHub Actions.
    assert (t2 - t1).total_seconds() == abs(local_time.utcoffset(t0).total_seconds())


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


def test_controlled_time_datetime():

    module_type = type(sys.modules['builtins'])

    dt_args = (2016, 1, 1, 12, 34, 56)

    t = ControlledTime(datetime.datetime(*dt_args), tick_seconds=1)
    t0 = t.just_now()

    # At this point, datetime is still the imported datetime module.
    assert isinstance(datetime, module_type)

    # This is like using mock.patch on our own datetime
    with override_dict(globals(), datetime=t):

        # At this point, we've installed a ControlledTime instance as our datetime module.
        assert not isinstance(datetime, module_type)
        assert isinstance(datetime, ControlledTime)
        assert isinstance(datetime.datetime, ControlledTime.ProxyDatetimeClass)
        # Make sure we can make a datetime.datetime object even with the mock
        assert datetime.datetime(*dt_args) == t0
        assert datetime.datetime.now() == t0 + datetime.timedelta(seconds=1)


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
        assert type(e) is SyntaxError
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
    # Documentation says we should be able to access the call with .call_args[n] but that doesn't work,
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


def test_mock_file_system_simple():

    mfs = MockFileSystem(files={"pre-existing-file.txt": "stuff from yesterday"})

    with mock.patch("io.open", mfs.open):
        with mock.patch("os.path.exists", mfs.exists):
            with mock.patch("os.remove", mfs.remove):

                filename = "no.such.file"
                assert os.path.exists(filename) is False

                filename2 = "pre-existing-file.txt"
                assert os.path.exists(filename2)

                assert len(mfs.files) == 1

                with io.open(filename, 'w') as fp:
                    fp.write("foo")
                    fp.writelines(["bar\n", "baz\n"])

                assert os.path.exists(filename) is True

                with io.open(filename, 'r') as fp:
                    assert fp.read() == 'foobar\nbaz\n'

                assert len(mfs.files) == 2

                with io.open(filename2, 'r') as fp:
                    assert fp.read() == "stuff from yesterday"

                assert sorted(mfs.files.keys()) == ['no.such.file', 'pre-existing-file.txt']

                assert mfs.files == {
                    'no.such.file': b'foobar\nbaz\n',
                    'pre-existing-file.txt': b'stuff from yesterday'
                }


def test_mock_exists_open_remove_abspath_getcwd_chdir():

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove_abspath_getcwd_chdir():

        assert os.path.abspath(".") == "/home/mock"
        assert os.path.abspath("./foo") == "/home/mock/foo"
        assert os.path.abspath("foo") == "/home/mock/foo"
        assert os.path.abspath("foo/bar") == "/home/mock/foo/bar"
        assert os.path.abspath("/foo") == "/foo"

        assert os.getcwd() == "/home/mock"
        os.chdir('bin')
        assert os.getcwd() == "/home/mock/bin"
        assert os.path.abspath(os.curdir) == "/home/mock/bin"
        os.chdir('/bin')
        assert os.getcwd() == "/bin"
        assert os.path.abspath(os.curdir) == "/bin"


def test_mock_expanduser():

    mfs = MockFileSystem()

    with mock.patch("os.path.expanduser", mfs.expanduser):

        assert os.path.expanduser("~") == "/home/mock"
        assert os.path.expanduser("~root") == "/root"
        assert os.path.expanduser("~foo") == "~foo"

        assert os.path.expanduser("~/x") == "/home/mock/x"
        assert os.path.expanduser("~root/x") == "/root/x"
        assert os.path.expanduser("~foo/x") == "~foo/x"


def test_mock_file_system_auto():

    temp_filename = "IF_YOU_SEE_THIS_FILE_DELETE_IT.txt"

    temp_file_text = ("This file is used only temporarily by dcicutils.qa_utils.test_mock_file_system_auto.\n"
                      "It is safe and encouraged to to delete it.\n"
                      "This token is unique: %s.\n"
                      % uuid.uuid4())

    try:

        # We're writing this before turning on the mock, to see if the mock can see it.
        with open(temp_filename, 'w') as outfile:
            outfile.write(temp_file_text)

        with MockFileSystem(auto_mirror_files_for_read=True).mock_exists_open_remove() as mfs:

            assert len(mfs.files) == 0

            assert os.path.exists(temp_filename)

            assert len(mfs.files) == 1

            with open(temp_filename) as infile:
                content = infile.read()

            assert content == temp_file_text

            os.remove(temp_filename)

            assert len(mfs.files) == 0

            # Removing the file in the mock does not cause us to auto-mirror anew.
            assert not os.path.exists(temp_filename)

            # This is just confirmation
            assert len(mfs.files) == 0

        # But now we are outside the mock again, so the file should be visible.
        assert os.path.exists(temp_filename)

    finally:

        os.remove(temp_filename)


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

        printed.reset()

        mfs = MockFileSystem()
        with mfs.mock_exists_open_remove():

            with io.open("some-file.txt", 'w') as fp:
                PRINT("stuff to file", file=fp)
                PRINT("stuff to console")
                PRINT("more stuff to file", file=fp)
                PRINT("more stuff to console")

            assert printed.lines == ["stuff to console", "more stuff to console"]
            assert printed.last == "more stuff to console"
            assert printed.lines == printed.file_lines[None]
            assert printed.last == printed.file_last[None]
            assert printed.file_lines[None] == ["stuff to console", "more stuff to console"]
            assert printed.file_last[None] == "more stuff to console"
            assert printed.file_lines[fp] == ["stuff to file", "more stuff to file"]
            assert printed.file_last[fp] == "more stuff to file"

            PRINT("Done.")
            assert printed.last == "Done."
            assert printed.file_last[None] == "Done."
            assert printed.lines == ["stuff to console", "more stuff to console", "Done."]
            assert printed.file_lines[None] == ["stuff to console", "more stuff to console", "Done."]

            PRINT("Done, too.", file=fp)
            assert printed.file_last[fp] == "Done, too."
            assert printed.file_lines[fp] == ["stuff to file", "more stuff to file", "Done, too."]

            printed.reset()

            with io.open("another-file.txt", 'w') as fp2:

                assert printed.last is None
                assert printed.file_last[None] is None
                assert printed.file_last[sys.stdout] is None

                assert printed.lines == []
                assert printed.file_lines[None] == []
                assert printed.file_lines[sys.stdout] == []

                assert printed.file_last[fp2] is None
                assert printed.file_lines[fp2] == []

                PRINT("foo", file=fp2)

                assert printed.last is None
                assert printed.lines == []
                assert printed.file_last[fp2] == "foo"
                assert printed.file_lines[fp2] == ["foo"]


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


def test_mock_boto3_client():

    mock_boto3 = MockBoto3()

    assert isinstance(mock_boto3.client('s3'), MockBotoS3Client)
    assert isinstance(mock_boto3.client('s3', region_name='us-east-1'), MockBotoS3Client)

    with pytest.raises(ValueError):
        mock_boto3.client('s3', region_name='us-east-2')

    with pytest.raises(NotImplementedError):
        mock_boto3.client('some_other_kind')


def test_mock_boto3_client_use():

    print()

    mock_boto3 = MockBoto3()
    mfs = MockFileSystem(files={"myfile": "some content", 'other_file': "other content"})

    with mfs.mock_exists_open_remove():
        with mock.patch.object(boto3, "client", mock_boto3.client):

            s3 = mock_boto3.client('s3')  # noQA - PyCharm wrongly sees a syntax error
            assert isinstance(s3, MockBotoS3Client)
            s3.upload_file(Filename="myfile", Bucket="foo", Key="bar")
            s3.upload_file(Filename="other_file", Bucket="foo", Key="baz")
            s3.download_file(Filename="myfile2", Bucket="foo", Key="bar")
            myfile_content = file_contents("myfile")
            myfile_content2 = file_contents("myfile2")
            assert myfile_content == myfile_content2 == "some content"

        s3 = mock_boto3.client('s3')

        # No matter what clients you get, they all share the same MockFileSystem, which we can get from s3_files
        s3fs = s3.s3_files
        # We saved an s3 file to bucket "foo" and key "bar", so it will be in the s3fs as "foo/bar"
        assert sorted(s3fs.files.keys()) == ['foo/bar', 'foo/baz']
        # The content is stored in binary format
        assert s3fs.files['foo/bar'] == b'some content'
        assert s3fs.files['foo/baz'] == b'other content'

        assert isinstance(s3, MockBotoS3Client)

        assert s3._object_storage_class('foo/bar') == s3.DEFAULT_STORAGE_CLASS == 'STANDARD'
        s3._set_object_storage_class_for_testing('foo/bar', 'DEEP_ARCHIVE')
        assert s3._object_storage_class('foo/bar') == 'DEEP_ARCHIVE'
        assert s3._object_storage_class('foo/baz') == 'STANDARD'

        # Because of shared reality in our mock_boto3, we'll see those same results with a new client.
        s3_client2 = mock_boto3.client('s3')
        assert s3_client2._object_storage_class('foo/bar') == 'DEEP_ARCHIVE'
        assert s3_client2._object_storage_class('foo/baz') == 'STANDARD'

        # Creating a new boto3 and asking for a client will see a different reality that does not know about
        # the files we've created in the above boto3 mock.

        new_s3_from_new_boto3 = MockBoto3().client('s3')
        with pytest.raises(Exception):  # This s3 file does not exist
            new_s3_from_new_boto3._object_storage_class('foo/bar')
        new_s3_from_new_boto3.upload_file(Filename="myfile", Bucket="foo", Key="bar")  # now it does
        assert new_s3_from_new_boto3._object_storage_class('foo/bar') == 'STANDARD'


def test_mock_uuid_module_documentation_example():
    mock_uuid_module = MockUUIDModule()
    assert str(mock_uuid_module.uuid4()) == '00000000-0000-0000-0000-000000000001'
    with mock.patch.object(uuid, "uuid4", mock_uuid_module.uuid4):
        assert str(uuid.uuid4()) == '00000000-0000-0000-0000-000000000002'


def test_mock_uuid_module():

    for _ in range(2):
        fake_uuid = MockUUIDModule()
        assert str(fake_uuid.uuid4()) == '00000000-0000-0000-0000-000000000001'
        assert str(fake_uuid.uuid4()) == '00000000-0000-0000-0000-000000000002'
        assert str(fake_uuid.uuid4()) == '00000000-0000-0000-0000-000000000003'

    fake_uuid = MockUUIDModule()
    assert isinstance(fake_uuid.uuid4(), uuid.UUID)

    fake_uuid = MockUUIDModule(prefix='', pad=3, uuid_class=str)
    assert str(fake_uuid.uuid4()) == '001'


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


def test_object_basic_attribute_block():

    start_time = datetime.datetime.now()
    sample_filename = "foo"
    mock_boto3 = MockBoto3()
    s3 = mock_boto3.client('s3')
    b = MockObjectBasicAttributeBlock(filename=sample_filename, s3=s3)

    assert b.last_modified < datetime.datetime.now()
    assert b.last_modified > start_time
    assert b.s3 == s3
    assert b.filename == sample_filename
    assert isinstance(b.version_id, str)

    with local_attrs(MockObjectBasicAttributeBlock, MONTONIC_VERSIONS=False):
        version_id_monotonic_false = MockObjectBasicAttributeBlock._generate_version_id()
    with local_attrs(MockObjectBasicAttributeBlock, MONTONIC_VERSIONS=True):
        version_id_monotonic_true = MockObjectBasicAttributeBlock._generate_version_id()

    assert isinstance(version_id_monotonic_false, str)
    assert '.' in version_id_monotonic_false      # a random string is really a guid with '.' instead of '-'
    assert isinstance(version_id_monotonic_true, str)
    assert '.' not in version_id_monotonic_true  # a timestamp of sorts, with digits 0-9 and a-z, but no dots

    with pytest.raises(NotImplementedError):
        x = b.storage_class
        ignored(x)

    with pytest.raises(NotImplementedError):
        b.initialize_storage_class('STANDARD')

    with pytest.raises(NotImplementedError):
        x = b.tagset
        ignored(x)

    with pytest.raises(NotImplementedError):
        b.set_tagset([])


def test_object_delete_marker():

    sample_filename = "foo"
    mock_boto3 = MockBoto3()
    s3 = mock_boto3.client('s3')
    b = MockObjectDeleteMarker(filename=sample_filename, s3=s3)

    assert isinstance(b.last_modified, datetime.datetime)
    assert b.s3 == s3
    assert b.filename == sample_filename
    assert isinstance(b.version_id, str)

    with pytest.raises(Exception):
        x = b.storage_class
        ignored(x)

    with pytest.raises(Exception):
        b.initialize_storage_class('STANDARD')

    with pytest.raises(Exception):
        x = b.tagset
        ignored(x)

    with pytest.raises(Exception):
        b.set_tagset([])


def test_object_attribute_block():

    start_time = datetime.datetime.now()
    sample_filename = "foo"
    sample_content = "some text"
    sample_tagset: List[Dict[Literal['Key', 'Value'], str]] = [{'Key': 'foo', 'Value': 'bar'}]
    sample_delay_seconds = 60
    sample_duration_days = 7
    mock_boto3 = MockBoto3()
    s3 = mock_boto3.client('s3')
    b = MockObjectAttributeBlock(filename=sample_filename, s3=s3)

    assert isinstance(b.last_modified, datetime.datetime)
    assert b.s3 == s3
    assert b.filename == sample_filename
    assert isinstance(b.version_id, str)
    assert b.storage_class == 'STANDARD'
    b.initialize_storage_class('GLACIER')
    assert b.storage_class == 'GLACIER'
    assert b.tagset == []
    b.set_tagset(sample_tagset)
    assert b.tagset == sample_tagset
    assert b.content is None
    b.set_content(sample_content)
    assert b.content == sample_content
    with pytest.raises(Exception):
        b.set_content(sample_content)

    assert b.restoration is None
    b.restore_temporarily(delay_seconds=sample_delay_seconds, duration_days=sample_duration_days,
                          storage_class='STANDARD')
    assert isinstance(b.restoration, MockTemporaryRestoration)
    assert b.restoration.available_after > start_time
    assert b.restoration.available_after < datetime.datetime.now() + datetime.timedelta(seconds=sample_delay_seconds)
    assert b.storage_class == 'STANDARD'
    b.hurry_restoration()
    assert b.storage_class == 'STANDARD'
    assert b.restoration.available_after < datetime.datetime.now()
    assert b.restoration.available_until > datetime.datetime.now()
    assert b.restoration.available_until > datetime.datetime.now()
    assert b.restoration.available_until < datetime.datetime.now() + datetime.timedelta(days=sample_duration_days,
                                                                                        seconds=sample_delay_seconds)
    assert b.storage_class == 'STANDARD'
    b.hurry_restoration_expiry()
    # We have to examine ._restoration because an expired restoration will disappear as soon as we check it
    assert b._restoration.available_until < datetime.datetime.now()
    # Here we see it goes away...
    assert b.restoration is None
    assert b.storage_class == 'GLACIER'


def test_mock_keys_not_implemented():

    err = MockKeysNotImplemented(keys=['foo', 'bar'], operation="some-operation")
    assert str(err) == 'Mocked some-operation does not implement keywords: foo, bar'


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
        # I am not sure that's the same as what unittest does in this case, but it will
        # suffice for now. -kmp 6-Oct-2020
        with raises_regexp(RuntimeError, "This.*test!"):
            raise KeyError('This is a test!')


def test_version_checker_no_changelog():

    class MyVersionChecker(VersionChecker):
        PYPROJECT = os.path.join(os.path.dirname(__file__), "../pyproject.toml")
        CHANGELOG = None

    MyVersionChecker.check_version()

    class MyChangeLogChecker(ChangeLogChecker):
        PYPROJECT = os.path.join(os.path.dirname(__file__), "../pyproject.toml")
        CHANGELOG = None

    MyChangeLogChecker.check_version()


def test_version_checker_with_missing_changelog():

    mfs = MockFileSystem(files={'pyproject.toml': '[tool.poetry]\nname = "foo"\nversion = "1.2.3"'})

    with mock.patch("os.path.exists", mfs.exists):

        class MyVersionChecker(VersionChecker):

            PYPROJECT = os.path.join(os.path.dirname(__file__), "../pyproject.toml")
            CHANGELOG = os.path.join(os.path.dirname(__file__), "../CHANGELOG.rst")

        with pytest.raises(AssertionError):
            MyVersionChecker.check_version()  # The version history will be missing because of mocking.

        class MyChangeLogChecker(ChangeLogChecker):

            PYPROJECT = os.path.join(os.path.dirname(__file__), "../pyproject.toml")
            CHANGELOG = os.path.join(os.path.dirname(__file__), "../CHANGELOG.rst")

        with pytest.raises(AssertionError):
            MyChangeLogChecker.check_version()  # The version history will be missing because of mocking.


def test_version_checker_with_proper_changelog():

    pyproject_filename = os.path.join(os.path.dirname(__file__), "../pyproject.toml")
    changelog_filename = os.path.join(os.path.dirname(__file__), "../CHANGELOG.rst")

    mfs = MockFileSystem(files={
        pyproject_filename: '[tool.poetry]\nname = "foo"\nversion = "1.2.3"',
        changelog_filename:
            '1.2.0\n'
            'Some new feature.\n'
            '1.2.1\n'
            'A bug fix.\n'
            '1.2.2\n'
            'A second bug fix.\n'
            '1.2.3\n'
            'A third bug fix.\n'
    })

    with mock.patch("io.open", mfs.open):
        with mock.patch("os.path.exists", mfs.exists):

            class MyVersionChecker(VersionChecker):

                PYPROJECT = pyproject_filename
                CHANGELOG = changelog_filename

            # The CHANGELOG is present and with the right data.
            MyVersionChecker.check_version()

            class MyChangeLogChecker(ChangeLogChecker):

                PYPROJECT = pyproject_filename
                CHANGELOG = changelog_filename

            # The CHANGELOG is present and with the right data.
            MyChangeLogChecker.check_version()


def test_version_checker_with_insufficient_changelog():

    pyproject_filename = os.path.join(os.path.dirname(__file__), "../pyproject.toml")
    changelog_filename = os.path.join(os.path.dirname(__file__), "../CHANGELOG.rst")

    mfs = MockFileSystem(files={
        pyproject_filename: '[tool.poetry]\nname = "foo"\nversion = "1.2.3"',
        changelog_filename:
            '1.2.0\n'
            'Some new feature.\n'
            '1.2.1\n'
            'A bug fix.\n'
    })

    with mock.patch("io.open", mfs.open):
        with mock.patch("os.path.exists", mfs.exists):

            class MyVersionChecker(VersionChecker):

                PYPROJECT = pyproject_filename
                CHANGELOG = changelog_filename

            with pytest.warns(VersionChecker.WARNING_CATEGORY):
                # The CHANGELOG won't have the right data, so we should see a warning.
                MyVersionChecker.check_version()

            class MyChangeLogChecker(ChangeLogChecker):

                PYPROJECT = pyproject_filename
                CHANGELOG = changelog_filename

            with pytest.raises(AssertionError):
                # The CHANGELOG won't have the right data, so we should see a warning.
                MyChangeLogChecker.check_version()


def test_check_duplicated_items_by_key():

    with raises_regexp(AssertionError,
                       "Duplicated uuid 123 in {'uuid': '123', 'foo': 'a'} and {'uuid': '123', 'foo': 'c'}"):
        check_duplicated_items_by_key(
            'uuid',
            [
                {'uuid': '123', 'foo': 'a'},
                {'uuid': '456', 'foo': 'b'},
                {'uuid': '123', 'foo': 'c'},
            ]
        )


def test_known_bug_expected_and_found():
    with known_bug_expected(jira_ticket="TST-00001"):
        raise ValueError("Foo")


def test_known_bug_expected_but_wrong_class_1():
    with pytest.raises(WrongErrorSeen):
        with known_bug_expected(jira_ticket="TST-00001", error_class=RuntimeError):
            raise ValueError("Foo")


def test_known_bug_expected_but_wrong_class_2():
    with known_bug_expected(jira_ticket="TST-00002", error_class=WrongErrorSeen):
        with known_bug_expected(jira_ticket="TST-00001", error_class=RuntimeError):
            raise ValueError("Foo")


def test_known_bug_expected_but_no_error_1():
    with pytest.raises(ExpectedErrorNotSeen):
        with known_bug_expected(jira_ticket="TST-00001", error_class=RuntimeError):
            pass


def test_known_bug_expected_but_no_error_2():
    with known_bug_expected(jira_ticket="TST-00002", error_class=ExpectedErrorNotSeen):
        with known_bug_expected(jira_ticket="TST-00001", error_class=RuntimeError):
            pass


def test_known_bug_expected_fixed():
    with known_bug_expected(jira_ticket="TST-00001", error_class=RuntimeError, fixed=True):
        pass


def test_known_bug_expected_regression_1():
    with pytest.raises(UnexpectedErrorAfterFix):
        with known_bug_expected(jira_ticket="TST-00001", error_class=RuntimeError, fixed=True):
            raise RuntimeError("foo")


def test_known_bug_expected_regression_2():
    with known_bug_expected(jira_ticket="TST-00002", error_class=UnexpectedErrorAfterFix):
        with known_bug_expected(jira_ticket="TST-00001", error_class=RuntimeError, fixed=True):
            raise RuntimeError("foo")


def test_mocked_command_args():
    with pytest.raises(AssertionError):
        MockedCommandArgs(foo='x')

    class MockedFooBarArgs(MockedCommandArgs):
        VALID_ARGS = ['foo', 'bar', 'foobar']

    args = MockedFooBarArgs(foo='x', bar='y', foobar='xy')
    assert args.foo == 'x'      # noQA - PyCharm can't see we declared this arg
    assert args.bar == 'y'      # noQA - PyCharm can't see we declared this arg
    assert args.foobar == 'xy'  # noQA - PyCharm can't see we declared this arg


MY_MODULE = sys.modules['test.test_qa_utils']


def test_input_mocked():

    def some_function_with_input():
        return input("input something:")

    with input_mocked("x", "y", module=MY_MODULE):
        assert some_function_with_input() == "x"
        assert some_function_with_input() == "y"
        with pytest.raises(AssertionError) as exc_info:
            some_function_with_input()
        assert str(exc_info.value) == "There are not enough mock inputs."

    with pytest.raises(AssertionError) as exc_info:
        with input_mocked("x", "y", module=sys.modules['test.test_qa_utils']):
            assert some_function_with_input() == "x"
    assert str(exc_info.value) == "There is 1 unused mock input."


def test_logged_messages():

    with logged_messages("INFO: foo", module=MY_MODULE, logvar='logger'):
        logger.info("foo")

    with pytest.raises(AssertionError):
        with logged_messages("INFO: foo", module=MY_MODULE, logvar='logger'):
            logger.info("foo")
            logger.info("bar")

    with logged_messages(info=["foo"], module=MY_MODULE, logvar='logger'):
        logger.info("foo")

    with logged_messages(info=["foo"], warning=["bar"], module=MY_MODULE, logvar='logger'):
        logger.info("foo")
        logger.warning("bar")

    with pytest.raises(AssertionError):
        with logged_messages(info=["foo"], module=MY_MODULE, logvar='logger'):
            logger.info("foo")
            logger.info("bar")

    with pytest.raises(AssertionError):
        with logged_messages(info=["foo"], module=MY_MODULE, logvar='logger'):
            logger.info("foo")
            logger.warning("bar")

    with logged_messages(warning=["bar"], module=MY_MODULE, logvar='logger', allow_warn=True):
        logger.warn("bar")  # noQA - yes, code should use .warning() not .warn(), but we're testing a check for that

    with pytest.raises(AssertionError):
        with logged_messages(warning=["bar"], module=MY_MODULE, logvar='logger', allow_warn=False):
            logger.warn("bar")  # noQA - yes, code should use .warning() not .warn(), but we're testing a check for that

    with pytest.raises(AssertionError):
        with logged_messages(warning=["bar"], module=MY_MODULE, logvar='logger'):
            # allow_warn defaults to False
            logger.warn("bar")  # noQA - yes, code should use .warning() not .warn(), but we're testing a check for that


def test_mock_log():

    m = MockLog(allow_warn=False)

    with pytest.raises(AssertionError) as exc:
        m.warn("should fail")  # noQA - yes, code should use .warning() not .warn(), but we're testing a check for that
    assert "warn called. Should be 'warning'" in str(exc.value)

    m = MockLog(allow_warn=True)

    m.debug("a log.debug message")
    m.info("a log.info message")
    m.warn("a call to log.warn")
    m.warning("a call to log.warning")
    m.error("a call to log.error")
    m.critical("a call to log.critical")

    assert m.messages == {
        "debug": ["a log.debug message"],
        "info": ["a log.info message"],
        "warning": ["a call to log.warn", "a call to log.warning"],
        "error": ["a call to log.error"],
        "critical": ["a call to log.critical"]
    }

    assert m.all_log_messages == [
        "DEBUG: a log.debug message",
        "INFO: a log.info message",
        "WARNING: a call to log.warn",
        "WARNING: a call to log.warning",
        "ERROR: a call to log.error",
        "CRITICAL: a call to log.critical",
    ]


def test_sqs_client_bad_region():

    with pytest.raises(ValueError) as exc:
        MockBoto3().client('sqs', region_name='some-region')
    assert str(exc.value) == "Unexpected region: some-region"


def test_s3_client_bad_region():

    with pytest.raises(ValueError) as exc:
        MockBoto3().client('s3', region_name='some-region')
    assert str(exc.value) == "Unexpected region: some-region"


def test_mock_id():

    mock_id = MockId()

    w = 3
    x = object()
    y = 'foo'
    z = None

    w_id = mock_id(w)
    x_id = mock_id(x)
    y_id = mock_id(y)
    z_id = mock_id(z)

    assert w_id + 1 == x_id
    assert x_id + 1 == y_id
    assert y_id + 1 == z_id

    assert mock_id(w) == w_id
    assert mock_id(x) == x_id
    assert mock_id(y) == y_id
    assert mock_id(z) == z_id

    mock_id = MockId(counter_base=25)
    assert mock_id('something') == 25
    assert mock_id('something-else') == 26
    assert mock_id('something') == 25


def test_eventually():

    dt = ControlledTime()
    with mock.patch("datetime.datetime", dt):
        with mock.patch("time.sleep", dt.sleep):

            def foo():
                return 17

            flakey_success_frequency = 3

            flakey_foo = Occasionally(foo, success_frequency=flakey_success_frequency)

            def my_assertions():
                assert flakey_foo() == 17

            with pytest.raises(AssertionError):
                Eventually.call_assertion(my_assertions, threshold_seconds=flakey_success_frequency - 1,
                                          error_class=Exception)

            # Beyond here we're testing the error_message="something" argument,
            # which allows us to only wait for a specific eventual message.

            def test_error_message_argument(*, expected_message=None):
                Eventually.call_assertion(my_assertions, threshold_seconds=flakey_success_frequency,
                                          error_class=Exception,
                                          error_message=expected_message)

            actual_message = "Oops. Occasionally this fails."

            test_error_message_argument()

            test_error_message_argument(expected_message=actual_message)

            with pytest.raises(Exception) as e:
                # If we're Eventually expecting an unrelated message,
                # the actual error we get will just pass through and won't be retried.
                test_error_message_argument(expected_message="SOME OTHER MESSAGE")
            assert str(e.value) == actual_message

            # Here we want to test how much time passes if all tests are tried.
            # The default is 10 tries at 1-second intervals, so should be at least 10 seconds.

            one_second = dt.timedelta(seconds=1)
            five_seconds = dt.timedelta(seconds=5)
            ten_seconds = dt.timedelta(seconds=10)
            before = dt.now()

            def always_failing():
                raise AssertionError("Failed.")

            with pytest.raises(Exception):
                Eventually.call_assertion(always_failing)

            after = dt.now()
            delta = after - before
            assert delta >= ten_seconds

            @Eventually.consistent()
            def also_failing():
                raise AssertionError("Also failed.")

            before = dt.now()
            with pytest.raises(AssertionError):
                also_failing()
            after = dt.now()
            delta = after - before  # 10 secs for the computation plus 1 sec to check current time, so about 11 secs
            assert delta > ten_seconds

            @Eventually.consistent(wait_seconds=0.1)
            def quickly_failing():
                raise AssertionError("Also failed.")

            before2 = dt.now()
            with pytest.raises(AssertionError):
                quickly_failing()
            after2 = dt.now()

            delta2 = after2 - before2  # 1 sec for the computation plus 1 sec to check current time, so about 2 sec
            assert delta2 > one_second
            assert delta2 < five_seconds

            before3 = dt.now()
            with pytest.raises(AssertionError):
                quickly_failing(tries=100)
            after3 = dt.now()

            delta3 = after3 - before3  # 100 tries * 0.1 sec plus 1 sec to check current time, so about 11 sec
            assert delta3 > ten_seconds

            class MyStore:
                VALUE = 0

            @Eventually.consistent()
            def foo():
                MyStore.VALUE += 1
                return MyStore.VALUE

            assert foo(tries=3) == 1  # The value will increment exactly once because it succeeds first try.


def test_timer():

    print()  # start on a fresh line

    dt = ControlledTime(tick_seconds=0.1)
    with mock.patch("datetime.datetime", dt):

        with Timer() as t1:
            dt.sleep(20)

        s1 = t1.duration_seconds()
        print(f"Seconds elapsed: {s1}")
        # floating point compares are tricky, but approximately s1 = 20.1
        # clock checks cost us 1 tick (0.1 sec)
        assert 20.05 < s1 < 20.15

        t2 = Timer()
        t2.start_timer()
        dt.sleep(20)
        t2.stop_timer()

        s2 = t2.duration_seconds()
        print(f"Seconds elapsed: {s2}")
        assert 20.05 < s2 < 20.15  # see explanation above

        t2.start_timer()  # start timer with no intent to stop it
        dt.sleep(100)     # wait a good while

        check_time = dt.now()

        t2.start_timer()  # reuse same timer, but it will be reset by this
        dt.sleep(10)
        interim_seconds = t2.duration_seconds()
        print(f"Seconds elapsed: {interim_seconds}  # interim, timer not stopped")
        assert 10.05 < interim_seconds < 10.15  # approximately equal to 10.1 (clock check costs 1 tick = 0.1)
        dt.sleep(5)
        interim_seconds = t2.duration_seconds()
        print(f"Seconds elapsed: {interim_seconds}  # interim, timer not stopped")
        assert 15.15 < interim_seconds < 15.25  # approximately equal to 15.2 (another clock check costs 1 more tick)
        dt.sleep(5)
        t2.stop_timer()

        s2a = t2.duration_seconds()
        print(f"Seconds elapsed: {s2a}")
        assert 20.25 < s2a < 20.35  # see explanation above

        assert t2.start > check_time


def show_s3_debugging_data(mfs, s3, bucket_name):
    print("file system:")
    for file, data in mfs.files.items():
        s3_filename = f'{bucket_name}/{file}'
        all_versions = s3._object_all_versions(s3_filename)  # noQA - internal method needed for testing
        print(f" {file}[{s3._object_attribute_block(s3_filename).version_id}]:"  # noQA - ditto
              f" {data!r}  # length={len(data)}."
              f" {there_are([x.version_id for x in all_versions[:-1]], kind='back version')}")


def show_s3_list_object_version_data(s3, bucket_name) -> dict:
    version_info = s3.list_object_versions(Bucket=bucket_name)
    keys = [version['Key'] for version in version_info.get('Versions', [])]
    for key in keys:
        head = s3.head_object(Bucket=bucket_name, Key=key)
        print(f"head_object(Bucket={bucket_name!r}, Key={key!r}) =")
        print(json.dumps(head, indent=2, default=str))
    print(f"list_object_versions(Bucket={bucket_name!r}) =")
    print(json.dumps(version_info, indent=2, default=str))
    return version_info


def test_s3_copy_object_overwrite():
    # Output from this test is usefully viewed by doing:  pytest -s -vv -k test_glacier_utils_object_versions
    # In fact, this doesn't test any glacier_utils functionality, but just that a mock of this kind,
    # calling list_object_versions, would work. It is in some ways a better test of qa_utils.
    mfs = MockFileSystem()
    with mocked_s3utils(environments=['fourfront-mastertest']) as mock_boto3:
        with mfs.mock_exists_open_remove():
            s3: MockBotoS3Client = mock_boto3.client('s3')
            bucket_name = 'foo'
            key_name = 'file.txt'
            s3_filename = f"{bucket_name}/{key_name}"
            s3.create_object_for_testing("first contents", Bucket=bucket_name, Key=key_name)
            show_s3_debugging_data(mfs=mfs, s3=s3, bucket_name=bucket_name)
            attribute_block = s3._object_attribute_block(s3_filename)
            existing_version_id = attribute_block.version_id
            s3.copy_object(CopySource={'Bucket': bucket_name, 'Key': key_name, 'VersionId': existing_version_id},
                           Bucket=bucket_name, Key=key_name, CopySourceVersionId=existing_version_id,
                           StorageClass='GLACIER')
            show_s3_debugging_data(mfs=mfs, s3=s3, bucket_name=bucket_name)
            version_info = show_s3_list_object_version_data(s3=s3, bucket_name=bucket_name)
            versions = version_info['Versions']
            assert len(versions) == 1


def test_s3_copy_object_restoring():
    # Output from this test is usefully viewed by doing:  pytest -s -vv -k test_glacier_utils_object_versions
    # In fact, this doesn't test any glacier_utils functionality, but just that a mock of this kind,
    # calling list_object_versions, would work. It is in some ways a better test of qa_utils.
    mfs = MockFileSystem()
    with mocked_s3utils(environments=['fourfront-mastertest']) as mock_boto3:
        with mfs.mock_exists_open_remove():
            s3: MockBotoS3Client = mock_boto3.client('s3')
            bucket_name = 'foo'
            key_name = 'file.txt'
            s3_filename = f"{bucket_name}/{key_name}"
            print("Step 0")
            s3.create_object_for_testing("first contents", Bucket=bucket_name, Key=key_name)
            print("Step 1")
            show_s3_debugging_data(mfs=mfs, s3=s3, bucket_name=bucket_name)
            attribute_block = s3._object_attribute_block(s3_filename)
            existing_version_id = attribute_block.version_id
            s3.copy_object(CopySource={'Bucket': bucket_name, 'Key': key_name, 'VersionId': existing_version_id},
                           Bucket=bucket_name, Key=key_name, CopySourceVersionId=existing_version_id,
                           StorageClass='GLACIER')
            print("Step 2")
            show_s3_debugging_data(mfs=mfs, s3=s3, bucket_name=bucket_name)
            version_info = show_s3_list_object_version_data(s3=s3, bucket_name=bucket_name)
            versions = version_info['Versions']
            assert len(versions) == 1
            with pytest.raises(Exception):
                s3.copy_object(CopySource={'Bucket': bucket_name, 'Key': key_name, 'VersionId': existing_version_id},
                               Bucket=bucket_name, Key=key_name, CopySourceVersionId=existing_version_id,
                               StorageClass='STANDARD')
            print("Step 3")
            show_s3_debugging_data(mfs=mfs, s3=s3, bucket_name=bucket_name)
            version_info = show_s3_list_object_version_data(s3=s3, bucket_name=bucket_name)
            versions = version_info['Versions']
            assert len(versions) == 1
            s3.restore_object(Bucket=bucket_name, Key=key_name, RestoreRequest={'Days': 7})
            # s3._copy_object(CopySource={'Bucket': bucket_name, 'Key': key_name, 'VersionId': existing_version_id},
            #                Bucket=bucket_name, Key=key_name, CopySourceVersionId=existing_version_id,
            #                StorageClass='STANDARD', allow_glacial=True)
            print("Step 4")
            show_s3_debugging_data(mfs=mfs, s3=s3, bucket_name=bucket_name)
            version_info = show_s3_list_object_version_data(s3=s3, bucket_name=bucket_name)
            versions = version_info['Versions']
            assert len(versions) == 1
            with pytest.raises(Exception):
                s3.copy_object(CopySource={'Bucket': bucket_name, 'Key': key_name, 'VersionId': existing_version_id},
                               Bucket=bucket_name, Key=key_name, CopySourceVersionId=existing_version_id,
                               StorageClass='STANDARD')
            print("Step 5")
            show_s3_debugging_data(mfs=mfs, s3=s3, bucket_name=bucket_name)
            version_info = show_s3_list_object_version_data(s3=s3, bucket_name=bucket_name)
            versions = version_info['Versions']
            assert len(versions) == 1
            # time.sleep(MockBotoS3Client.RESTORATION_DELAY_SECONDS)
            s3.hurry_restoration_for_testing(s3_filename)
            s3.copy_object(CopySource={'Bucket': bucket_name, 'Key': key_name, 'VersionId': existing_version_id},
                           Bucket=bucket_name, Key=key_name + "_new",  # NOTE: Not using CopySourceVersionId here.
                           StorageClass='STANDARD')
            print("Step 6")
            show_s3_debugging_data(mfs=mfs, s3=s3, bucket_name=bucket_name)
            version_info = show_s3_list_object_version_data(s3=s3, bucket_name=bucket_name)
            versions = version_info['Versions']
            assert len(versions) == 2
            s3.hurry_restoration_expiry_for_testing(s3_filename)
            show_s3_debugging_data(mfs=mfs, s3=s3, bucket_name=bucket_name)
            version_info = show_s3_list_object_version_data(s3=s3, bucket_name=bucket_name)
            versions = version_info['Versions']
            assert len(versions) == 2


def test_s3_copy_object_new():
    # Output from this test is usefully viewed by doing:  pytest -s -vv -k test_glacier_utils_object_versions
    # In fact, this doesn't test any glacier_utils functionality, but just that a mock of this kind,
    # calling list_object_versions, would work. It is in some ways a better test of qa_utils.
    mfs = MockFileSystem()
    with mocked_s3utils(environments=['fourfront-mastertest']) as mock_boto3:
        with mfs.mock_exists_open_remove():
            s3: MockBotoS3Client = mock_boto3.client('s3')
            bucket_name = 'foo'
            key_name = 'file.txt'
            s3.create_object_for_testing("first contents", Bucket=bucket_name, Key=key_name)
            show_s3_debugging_data(mfs=mfs, s3=s3, bucket_name=bucket_name)
            s3.copy_object(CopySource={'Bucket': bucket_name, 'Key': key_name},
                           Bucket=bucket_name, Key=key_name, CopySourceVersionId=None,
                           StorageClass='GLACIER')
            show_s3_debugging_data(mfs=mfs, s3=s3, bucket_name=bucket_name)
            version_info = show_s3_list_object_version_data(s3=s3, bucket_name=bucket_name)
            versions = version_info['Versions']
            assert len(versions) == 2


def test_s3_list_object_versions():
    # Output from this test is usefully viewed by doing:  pytest -s -vv -k test_glacier_utils_object_versions
    # In fact, this doesn't test any glacier_utils functionality, but just that a mock of this kind,
    # calling list_object_versions, would work. It is in some ways a better test of qa_utils.
    mfs = MockFileSystem()
    with mocked_s3utils(environments=['fourfront-mastertest']) as mock_boto3:
        with mfs.mock_exists_open_remove():
            s3: MockBotoS3Client = mock_boto3.client('s3')
            bucket_name = 'foo'
            key_name = 'file.txt'
            key2_name = 'file2.txt'

            s3.create_object_for_testing("first contents", Bucket=bucket_name, Key=key_name)
            # with io.open(key_name, 'w') as fp:
            #     fp.write("first contents")
            # s3.upload_file(key_name, Bucket=bucket_name, Key=key_name)

            # s3.create_object_for_testing("second contents", Bucket=bucket_name, Key=key_name)
            with io.open(key_name, 'w') as fp:
                fp.write("second contents")
            s3.upload_file(key_name, Bucket=bucket_name, Key=key_name)

            # s3.create_object_for_testing("other stuff", Bucket=bucket_name, Key=key2_name)
            with io.open(key2_name, 'w') as fp:
                fp.write("other stuff")
            s3.upload_file(key2_name, Bucket=bucket_name, Key=key2_name)

            show_s3_debugging_data(mfs=mfs, s3=s3, bucket_name=bucket_name)
            version_info = show_s3_list_object_version_data(s3=s3, bucket_name=bucket_name)
            versions = version_info['Versions']
            assert len(versions) == 3

            version1, version2, version3 = versions
            # Back version of file.txt
            assert version1['Key'] == key_name
            assert version1['IsLatest'] is False
            # Current version of file.txt
            assert version2['Key'] == key_name
            assert version2['IsLatest'] is True
            # Current version of file2.txt
            assert version3['Key'] == key2_name
            assert version3['IsLatest'] is True
            assert all(version['StorageClass'] == 'STANDARD' for version in versions)


def test_mock_boto3_iam_user_access_key_pair():

    pair = MockBoto3IamUserAccessKeyPair()

    creation_date = pair._create_date  # noQA - access to protected member for testing

    assert isinstance(pair.id, str)
    assert isinstance(pair.secret, str)
    assert isinstance(creation_date, datetime.datetime)
    assert pair['something_else'] is None

    assert pair['AccessKeyId'] == pair._id
    assert pair['CreateDate'] == creation_date


def test_mock_boto3_iam_user_access_key_pair_collection():

    collection = MockBoto3IamUserAccessKeyPairCollection()

    pair1 = MockBoto3IamUserAccessKeyPair()
    pair2 = MockBoto3IamUserAccessKeyPair()

    assert collection['AccessKeyMetadata'] == []
    assert collection.add(pair1) is None
    assert collection['AccessKeyMetadata'] == [pair1]
    assert collection.add(pair2) is None
    assert collection['AccessKeyMetadata'] == [pair1, pair2]


def test_mock_boto3_iam_user():

    user_name = 'JDoe'

    user = MockBoto3IamUser(name=user_name)

    assert user.name == user_name

    assert user.mocked_access_keys["AccessKeyMetadata"] == []

    pair = user.create_access_key_pair()
    assert isinstance(pair, MockBoto3IamUserAccessKeyPair)

    assert user.mocked_access_keys["AccessKeyMetadata"] == [pair]


def test_mock_boto3_iam_user_collection():

    collection = MockBoto3IamUserCollection()

    assert collection.all() == []


def test_mock_boto3_iam_role():

    arn = "arn:some:role"
    role = MockBoto3IamRole(arn)
    assert role["Arn"] == arn
    assert role["Foo"] is None


def test_mock_boto3_iam_role_collection():

    collection = MockBoto3IamRoleCollection()
    assert collection["Roles"] == []
    assert collection["Foo"] is None


def test_is_subdict():

    print()  # start on fresh line

    for same in [{}, {"foo": 3}, {"foo": [1, "x", {"bar": 17}]}]:
        with printed_output() as printed:
            assert is_subdict(same, same)
            assert printed.lines == []

    for verbose in [False, True]:
        with printed_output() as printed:
            assert is_subdict({"foo": 3}, {"foo": 3, "bar": 4}, verbose=verbose)
            if verbose:
                assert printed.lines == [
                    "Non-fatal keyword mismatch at '':",
                    " json1 keys: {'foo'}",
                    " json2 keys: {'bar', 'foo'}",
                ]
            else:
                assert printed.lines == []

    for verbose in [True, False]:
        with printed_output() as printed:
            assert not is_subdict({"foo": 3, "bar": {"x": 3, "y": 4}},
                                  {"foo": 3, "bar": {"x": 3, "y": 5, "baz": 0}}, verbose=verbose)
            if verbose:
                assert printed.lines == [
                    "Non-fatal keyword mismatch at '.bar':",
                    " json1 keys: {'x', 'y'}",
                    " json2 keys: {'baz', 'x', 'y'}",
                    "Failed at '.bar.y' due to value mismatch: 4 != 5",
                    # "Recursive failure at '.bar' in object comparison",
                    # "Recursive failure at '' in object comparison",
                ]
            else:
                assert printed.lines == []

    for verbose in [True, False]:
        with printed_output() as printed:
            assert not is_subdict({"foo": 3, "bar": [1, 2, 3]},
                                  {"foo": 3, "bar": [1, 2, 3, 4]}, verbose=verbose)
            if verbose:
                assert printed.lines == [
                    "Failed at '.bar' in list comparison due to length mismatch: 3 vs 4",
                    # "Recursive failure at '' in object comparison"
                ]
            else:
                assert printed.lines == []

    for verbose in [True, False]:
        with printed_output() as printed:
            assert not is_subdict({"foo": 3, "baz": [1, 2, 3]},
                                  {"foo": 3, "bar": [1, 2, 3, 4]}, verbose=verbose)
            if verbose:
                assert printed.lines == [
                    "Failed at '' in object comparison due to key set mismatch:",
                    " json1 keys: {'baz', 'foo'}",
                    " json2 keys: {'bar', 'foo'}",
                ]
            else:
                assert printed.lines == []

    for verbose in [True, False]:
        with printed_output() as printed:
            assert not is_subdict({"foo": [1, 2, 3]},
                                  {"foo": 3}, verbose=verbose)
            if verbose:
                assert printed.lines == [
                    "Type mismatch (list vs int) at '.foo':",
                    " json1: [1, 2, 3]",
                    " json2: 3",
                ]
            else:
                assert printed.lines == []
