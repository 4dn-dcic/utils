import datetime
import os
import pytest
import pytz
import re
import subprocess
import time
import uuid

from dcicutils.qa_utils import mock_not_called, local_attrs, override_environ, ControlledTime
from unittest import mock

# The following line needs to be separate from other imports. It is PART OF A TEST.
from dcicutils.qa_utils import notice_pytest_fixtures   # Use care if editing this line. It is PART OF A TEST.
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
