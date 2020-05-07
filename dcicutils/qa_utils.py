"""
qa_utils: Tools for use in quality assurance testing.
"""

import datetime
import contextlib
import os
import pytz
from .misc_utils import PRINT


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

    # A randomly chosen but reproducible date 2010-07-01 12:00:00
    INITIAL_TIME = datetime.datetime(2010, 1, 1, 12, 0, 0)
    HMS_TIMEZONE = pytz.timezone("US/Eastern")
    _DATETIME = datetime.datetime

    def __init__(self, initial_time: datetime.datetime = INITIAL_TIME, tick_seconds: float = 1,
                 local_timezone: pytz.timezone = HMS_TIMEZONE):
        if not isinstance(initial_time, datetime.datetime):
            raise ValueError("Expected initial_time to be a datetime: %r" % initial_time)
        if not isinstance(tick_seconds, (int, float)):
            raise ValueError("Expected tick_seconds to be an int or a float: %r" % tick_seconds)

        self._initial_time = initial_time
        self._just_now = initial_time
        self._tick_timedelta = datetime.timedelta(seconds=tick_seconds)
        self._local_timezone = local_timezone

    def set_datetime(self, dt):
        if not isinstance(dt, datetime.datetime):
            raise ValueError("Expected a datetime: %r" % dt)
        if dt.tzinfo:
            raise ValueError("Expected a naive datetime (no timezone): %r" % dt)
        self._just_now = dt

    def reset_datetime(self):
        self.set_datetime(self._initial_time)

    def just_now(self) -> datetime.datetime:
        return self._just_now

    def now(self) -> datetime.datetime:
        self._just_now += self._tick_timedelta
        return self._just_now

    def utcnow(self) -> datetime.datetime:
        now = self.now()
        return self._local_timezone.localize(now).astimezone(pytz.UTC).replace(tzinfo=None)

    def sleep(self, secs: float):
        self._just_now += datetime.timedelta(seconds=secs)
