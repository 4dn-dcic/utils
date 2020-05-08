"""
qa_utils: Tools for use in quality assurance testing.
"""

import datetime
import contextlib
import os
import pytz
from .misc_utils import PRINT, ignored


def mock_not_called(name):
    """
    This can be used in mocking to mock a function that should not be called.
    Called with the name of a function, it returns a function that if called
    will raise an AssertionError complaining that such a name was called.
    """
    def mocked_function(*args, **kwargs):
        # It's OK to print here because we're expected to be called in a testing context, and
        # we're just about to fail a test. The person invoking the tests may want this data.
        PRINT("args=", args)
        PRINT("kwargs=", kwargs)
        raise AssertionError("%s was called where not expected." % name)
    return mocked_function


@contextlib.contextmanager
def local_attrs(obj, **kwargs):
    """
    This binds the named attributes of the given object.
    This is only allowed for an object that directly owns the indicated attributes.

    """
    keys = kwargs.keys()
    for key in keys:
        if key not in obj.__dict__:
            # This works only for objects that directly have the indicated property.
            # That happens for
            #  (a) an instance where its instance variables are in keys.
            #  (b) an uninstantiated class where its class variables (but not inherited class variables) are in keys.
            # So the error happens for these cases:
            #  (c) an instance where any of the keys come from its class instead of the instance itself
            #  (d) an uninstantiated class being used for keys that are inherited rather than direct class variables
            raise ValueError("%s inherits property %s. Treating it as dynamic could affect other objects."
                             % (obj, key))
    saved = {
        key: getattr(obj, key)
        for key in keys
    }
    try:
        for key in keys:
            setattr(obj, key, kwargs[key])
        yield
    finally:
        for key in keys:
            setattr(obj, key, saved[key])


@contextlib.contextmanager
def override_environ(**overrides):
    to_delete = []
    to_restore = {}
    env = os.environ
    try:
        for k, v in overrides.items():
            if k in env:
                to_restore[k] = env[k]
            else:
                to_delete.append(k)
            if v is None:
                env.pop(k, None)  # Delete key k, tolerating it being already gone
            else:
                env[k] = v
        yield
    finally:
        for k in to_delete:
            env.pop(k, None)  # Delete key k, tolerating it being already gone
        for k, v in to_restore.items():
            os.environ[k] = v


class ControlledTime:  # This will move to dcicutils -kmp 7-May-2020
    """
    This class can be used in mocking datetime.datetime for things that do certain time-related actions.
    Everytime datetime.now() is called, time increments by a known amount (default 1 second). So:

        start_time = datetime.datetime.now()

        def sleepy_function():
            time.sleep(10)

        dt = ControlledTime()
        with unittest.mock.patch("datetime.datetime", dt):
            with unittest.mock.patch("time.sleep", dt.sleep):
                t0 = datetime.datetime.now()
                sleepy_function()  # sleeps 10 seconds
                t1 = datetime.datetime.now()  # 1 more second increments
                assert (t1 - t0).total_seconds() == 11  # 11 virtual seconds have passed

        end_time = datetime.datetime.now()
        # In reality, whole test takes much less than one second...
        assert (end_time - start_time).total_seconds() < 0.5
    """

    # A randomly chosen but reproducible date 2010-07-01 12:00:00
    INITIAL_TIME = datetime.datetime(2010, 1, 1, 12, 0, 0)
    HMS_TIMEZONE = pytz.timezone("US/Eastern")
    _DATETIME = datetime.datetime

    def __init__(self, initial_time: datetime.datetime = INITIAL_TIME, tick_seconds: float = 1,
                 local_timezone: pytz.timezone = HMS_TIMEZONE):
        if not isinstance(initial_time, datetime.datetime):
            raise ValueError("Expected initial_time to be a datetime: %r" % initial_time)
        if initial_time.tzinfo is not None:
            raise ValueError("Expected initial_time to be a naive datetime (no timezone): %r" % initial_time)
        if not isinstance(tick_seconds, (int, float)):
            raise ValueError("Expected tick_seconds to be an int or a float: %r" % tick_seconds)

        self._initial_time = initial_time
        self._just_now = initial_time
        self._tick_timedelta = datetime.timedelta(seconds=tick_seconds)
        self._local_timezone = local_timezone

    def set_datetime(self, dt):
        """
        Sets the virtual clock time to the given value, which must be a regular datetime object.
        """
        if not isinstance(dt, datetime.datetime):
            raise ValueError("Expected a datetime: %r" % dt)
        if dt.tzinfo:
            raise ValueError("Expected a naive datetime (no timezone): %r" % dt)
        self._just_now = dt

    def reset_datetime(self):
        """
        Resets the virtual clock time to the original datetime that it had on creation.
        """
        self.set_datetime(self._initial_time)

    def just_now(self) -> datetime.datetime:
        """
        This is like .now() but it doesn't increment the virtual clock,
        it just tells you the previously known virtual clock time.
        """
        return self._just_now

    def now(self) -> datetime.datetime:
        """
        This advances time by one tick and returns the new time.

        To re-read the same time without advancing the clock, use .just_now() instead of .now().

        The tick duration is controlled by the tick_seconds initialization argument to ControlledTime.

        Note that neither .now() nor .utcnow() yields a time with a timezone,
        though the normal conversion operations are available on the times it does yield.
        It is assumed that .now() returns time in the Harvard Medical School timezone, US/Eastern
        unless a different local_timezone was specified when creating the ControlledTime.
        """
        self._just_now += self._tick_timedelta
        return self._just_now

    def utcnow(self) -> datetime.datetime:
        """
        This tells you what the virtual clock time would be in UTC.
        This works by adjusting for the difference in hours between the local timezone and UTC.

        Note that neither .now() nor .utcnow() yields a time with a timezone,
        though the normal conversion operations are available on the times it does yield.
        It is assumed that .now() returns time in the Harvard Medical School timezone, US/Eastern
        unless a different local_timezone was specified when creating the ControlledTime.
        """
        now = self.now()
        return self._local_timezone.localize(now).astimezone(pytz.UTC).replace(tzinfo=None)

    def sleep(self, secs: float):
        """
        This simulates sleep by advancing the virtual clock time by the indicated number of seconds.
        """

        self._just_now += datetime.timedelta(seconds=secs)


def notice_pytest_fixtures(*fixtures):
    """
    This declares its arguments to be pytest fixtures in use by surrounding code.

    This useful for assuring tools like flake8 and PyCharm that the arguments it is given are not being
    ignored but instead may just have usage patterns that don't make uses apparent.

    For example, in a file test_file.py, we might see uses of my_fixture and my_autoused_fixture
    that wrongly appear both globally unused AND also locally unused in the test_something function.

      from module_a import foo, bar
      from module_b import mock_application  # <-- sample fixture, to be used explicitly
      from module_c import mock_database     # <-- sample fixture to be used implicitly

      def test_something(mock_application):  # <-- sample of fixture used explicitly
          assert foo() = bar()

    In both cases, the apparently unused variables may cause flake8, PyCharm, and other
    'lint' programs to complain that some repair action is needed, such as removing the unused
    variables, even though such an action might break code. Using this declaration will
    guard against that, while providing useful documentation for the code:

      from module_a import foo, bar
      from module_b import mock_application
      from module_c import mock_database
      from dcicutils.qa_utils import notice_pytest_fixtures

      notice_pytest_fixtures(application, database_session)

      def test_something(mock_application):
          notice_pytest_fixtures(mock_application)  # <-- protects bound variable from seeming unused
          assert foo() = bar()
    """
    ignored(fixtures)  # we don't use the given fixtures, but now the tools will think we do
