"""
qa_utils: Tools for use in quality assurance testing.
"""

import configparser
import contextlib
import copy
import datetime
import dateutil.tz as dateutil_tz
import functools
import hashlib
import io
import json
import logging
import os
import pytest
import pytz
import re
import sys
import threading
import time
import uuid

from botocore.credentials import Credentials as Boto3Credentials
from botocore.exceptions import ClientError
from collections import defaultdict
from json import dumps as json_dumps, loads as json_loads
from typing import Any, Optional, List, DefaultDict, Union, Type, Dict
from typing_extensions import Literal
from unittest import mock
from . import misc_utils as misc_utils_module, command_utils as command_utils_module
from .common import S3StorageClass
from .env_utils import short_env_name
from .exceptions import ExpectedErrorNotSeen, WrongErrorSeen, UnexpectedErrorAfterFix, WrongErrorSeenAfterFix
from .glacier_utils import GlacierUtils
from .lang_utils import there_are
from .misc_utils import (
    PRINT, INPUT, ignored, Retry, remove_prefix, REF_TZ, builtin_print,
    environ_bool, exported, override_environ, override_dict, local_attrs, full_class_name,
    find_associations, get_error_message, remove_suffix, format_in_radix, future_datetime,
    _mockable_input,  # noQA - need this to keep mocking consistent
)
from .qa_checkers import QA_EXCEPTION_PATTERN, find_uses, confirm_no_uses, VersionChecker, ChangeLogChecker


# Using these names via qa_utils is deprecated. Their proper, supported home is now in qa_checkers.
# Please rewrite imports to get them from qa_checkers, not qa_utils. -kmp 21-Sep-2022
exported(QA_EXCEPTION_PATTERN, find_uses, confirm_no_uses, VersionChecker, ChangeLogChecker)


def show_elapsed_time(start, end):
    """ Helper method for below that is the default - just prints the elapsed time. """
    PRINT('Elapsed: %s' % (end - start))


@contextlib.contextmanager
def timed(reporter=None, debug=None):
    """ A simple context manager that will time how long it spends in context. Useful for debugging.

        :param reporter: lambda x, y where x and y are the start and finish times respectively, default PRINT
        :param debug: lambda x where x is an exception, default NO ACTION
    """
    if reporter is None:
        reporter = show_elapsed_time
    start = time.time()
    try:
        yield
    except Exception as e:
        if debug is not None:
            debug(e)
        raise
    finally:
        end = time.time()
        reporter(start, end)


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


exported(override_environ, override_dict, local_attrs)


LOCAL_TIMEZONE_MAPPINGS = {
    ('EST', 'EDT'): "US/Eastern",
    ('CST', 'CDT'): "US/Central",
    ('MST', 'MDT'): "US/Mountain",
    ('PST', 'PDT'): "US/Pacific",
    ('CET', 'CEST'): "CET",
}


def guess_local_timezone_for_testing() -> pytz.tzinfo:
    # Figuring out the actual local timezone from Python is much discussed on Stackoverflow and elsehwere
    # and there are no perfect solutions. It's a complicated topic. But mostly we need to be able to distinguish
    # local testing at HMS and remote testing on AWS.
    a_winter_time = datetime.datetime(2000, 12, 31, 12, 0, 0)
    a_summer_time = datetime.datetime(2000, 6, 30, 12, 0, 0)
    local_timezone = dateutil_tz.tzlocal()  # Alas, not a full timezone object with all the methods a pytz.timezone has
    winter_tz_name = local_timezone.tzname(a_winter_time)
    summer_tz_name = local_timezone.tzname(a_summer_time)
    mapping_key = (winter_tz_name, summer_tz_name)
    mapping = LOCAL_TIMEZONE_MAPPINGS.get(mapping_key)
    if mapping:
        return pytz.timezone(mapping)
    elif winter_tz_name == summer_tz_name:
        # We have some timezone that doesn't vary
        winter_tz = pytz.timezone(winter_tz_name)
        if winter_tz.utcoffset(a_winter_time).total_seconds() == 0:
            return pytz.UTC
        else:
            # Something like MST that we don't have in our ad hoc table, where it's the same all year round.
            # This may not select its prettiest name, but all we really want is to get a pytz.timezone at all
            # because that will have a .localize() method, which some of our code cares about.
            return winter_tz
    else:
        raise NotImplementedError("This mock is not designed well enough for timezone %s/%s."
                                  % (winter_tz_name, summer_tz_name))


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
    HMS_TIMEZONE = REF_TZ
    LOCAL_TIMEZONE = guess_local_timezone_for_testing()
    DATETIME_TYPE = datetime.datetime
    timedelta = datetime.timedelta

    def __init__(self, initial_time: datetime.datetime = INITIAL_TIME, tick_seconds: float = 1,
                 local_timezone: pytz.timezone = LOCAL_TIMEZONE):
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

        # This is here so that a ControlledTime can be used as a mock for the datetime module itself in some cases.
        # e.g.,
        #        with mock.patch.object(foo_module, "datetime", dt):
        #            ...
        # so that within the foo module, datetime.datetime will access ProxyDatetimeClass,
        # but datetime.timedelta will still work because a ControlledTime has a .timedelta.
        # The ProxyDatetimeClass will offer a few methods that are coordinated with the ControlledTime,
        # most importantly .now() and .utcnow().
        self.datetime = self.ProxyDatetimeClass(self)

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

    def just_utcnow(self):
        return self.just_now().astimezone(pytz.UTC).replace(tzinfo=None)

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

    EPOCH_START_TIME = datetime.datetime(1970, 1, 1, 0, 0, 0)

    def time(self) -> float:
        """
        Returns like what time.time would return.
        """
        return (self.utcnow() - self.EPOCH_START_TIME).total_seconds()

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
        return (self._local_timezone.localize(now)  # noQA - PyCharm complains wrongly about args to .localize()
                .astimezone(pytz.UTC).replace(tzinfo=None))

    def sleep(self, secs: float):
        """
        This simulates sleep by advancing the virtual clock time by the indicated number of seconds.
        """

        self._just_now += datetime.timedelta(seconds=secs)

    class ProxyDatetimeClass:

        def __init__(self, controlled_time):
            self._controlled_time = controlled_time

        def now(self):
            return self._controlled_time.now()

        def utcnow(self):
            return self._controlled_time.utcnow()

        def __call__(self, *args, **kwargs):
            return self._controlled_time.DATETIME_TYPE(*args, **kwargs)


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


class Occasionally:
    """
    This class is useful for testing flakey things.

    Occasionally(function) returns a function that works the first time but fails every other time.
    Occasionally(function, success_frequency=N) returns a function that fails N-1 times, then works.
    Occasionally(function, failure_frequency=N) returns a function that works N-1 times, then fails.

    The counting phase can be reset by calling .reset() on the function.
    """
    # More examples of this can be seen in the tests for misc_utils.RetryManager

    DEFAULT_ERROR_CLASS = Exception

    DEFAULT_ERROR_MESSAGE = "Oops. Occasionally this fails."

    def __init__(self, function, failure_frequency=None, success_frequency=None,
                 error_class=None, error_message=None):
        if not failure_frequency and not success_frequency:
            # Arbitrary. Setting success_frequency would amount to the same, but on other 'phase'.
            # This will succeed once, then fail.
            failure_frequency = 2
        self.frequency = failure_frequency or success_frequency
        self.frequently_fails = True if success_frequency else False
        self.count = 0
        self.function = function
        self.error_class = error_class or self.DEFAULT_ERROR_CLASS
        self.error_message = error_message or self.DEFAULT_ERROR_MESSAGE

    def reset(self):
        self.count = 0

    def __call__(self, *args, **kwargs):
        self.count = (self.count + 1) % self.frequency
        if (self.count == 0 and not self.frequently_fails) or (self.count != 0 and self.frequently_fails):
            raise self.error_class(self.error_message)
        else:
            return self.function(*args, **kwargs)

    @property
    def __name__(self):
        return "{}.occassionally".format(self.function.__name__)


class RetryManager(Retry):
    """
    This class provides a method that is not thread-safe but is usable in unit testing to locally bind retry options
    for a function declared elsewhere using the Retry.allow_retries(...) decorator.

    NOTE: this does NOT use module names of functions to avoid collisions (though you can override that
    with a name_key). For example:

        with RetryManager.retry_options("foo", wait_seconds=4):
            ...code where foo will wait 4 seconds before retrying...

    """

    @classmethod
    @contextlib.contextmanager
    def retry_options(cls, name_key, retries_allowed=None, wait_seconds=None,
                      wait_increment=None, wait_multiplier=None):
        if not isinstance(name_key, str):
            raise ValueError("The required 'name_key' argument to the RetryManager.retry_options context manager"
                             " must be a string: %r" % name_key)
        function_profile = cls._RETRY_OPTIONS_CATALOG.get(name_key)
        if not function_profile:
            raise ValueError("The 'name_key' argument to RetryManager.retry_options"
                             " did not name a registered function: %r" % name_key)
        assert isinstance(function_profile, cls.RetryOptions)
        options = {}
        if retries_allowed is not None:
            options['retries_allowed'] = retries_allowed
        if wait_seconds is not None:
            options['wait_seconds'] = wait_seconds
        if wait_increment is not None:
            options['wait_increment'] = wait_increment
        if wait_multiplier is not None:
            options['wait_multiplier'] = wait_multiplier
        if wait_increment is not None or wait_multiplier is not None:
            options['wait_adjustor'] = function_profile.make_wait_adjustor(wait_increment=wait_increment,
                                                                           wait_multiplier=wait_multiplier)
        with local_attrs(function_profile, **options):
            yield


FILE_SYSTEM_VERBOSE = environ_bool("FILE_SYSTEM_VERBOSE", default=False)


class MockFileWriter:

    def __init__(self, file_system, file, binary=False, encoding='utf-8'):
        self.file_system = file_system
        self.file = file
        self.encoding = encoding
        self.stream = io.BytesIO() if binary else io.StringIO()

    def __enter__(self):
        return self.stream

    def __exit__(self, exc_type, exc_val, exc_tb):
        content = self.stream.getvalue()
        file_system = self.file_system
        if file_system.exists(self.file):
            if FILE_SYSTEM_VERBOSE:  # pragma: no cover - Debugging option. Doesn't need testing.
                PRINT(f"Preparing to overwrite {self.file}.")
            file_system.prepare_for_overwrite(self.file)
        if FILE_SYSTEM_VERBOSE:  # pragma: no cover - Debugging option. Doesn't need testing.
            PRINT(f"Writing {content!r} to {self.file}.")
        file_system.files[self.file] = content if isinstance(content, bytes) else content.encode(self.encoding)


class MockFileSystem:
    """Extremely low-tech mock file system."""

    MOCK_USER_HOME = "/home/mock"
    MOCK_ROOT_HOME = "/root"

    def __init__(self, files=None, default_encoding='utf-8', auto_mirror_files_for_read=False, do_not_auto_mirror=()):
        self.default_encoding = default_encoding
        # Setting this dynamically will make things inconsistent
        self._auto_mirror_files_for_read = auto_mirror_files_for_read
        self._do_not_auto_mirror = set(do_not_auto_mirror or [])
        self.files = {filename: content.encode(default_encoding) for filename, content in (files or {}).items()}
        self.working_dir = self.MOCK_USER_HOME
        for filename in self.files:
            self._do_not_mirror(filename)

    IO_OPEN = staticmethod(io.open)
    OS_PATH_EXISTS = staticmethod(os.path.exists)
    OS_REMOVE = staticmethod(os.remove)

    def _do_not_mirror(self, file):
        if self._auto_mirror_files_for_read:
            self._do_not_auto_mirror.add(file)

    def _maybe_auto_mirror_file(self, file):
        if self._auto_mirror_files_for_read:
            if file not in self._do_not_auto_mirror:
                if (self.OS_PATH_EXISTS(file)
                        # file might be in files if someone has been manipulating the file structure directly
                        and file not in self.files):
                    with open(file, 'rb') as fp:
                        self.files[file] = fp.read()
                self._do_not_mirror(file)

    def prepare_for_overwrite(self, file):
        # By default this does nothing, but it could do something
        pass

    def exists(self, file):
        self._maybe_auto_mirror_file(file)
        return self.files.get(file) is not None  # don't want an empty file to pass for missing

    def remove(self, file):
        self._maybe_auto_mirror_file(file)
        if self.files.pop(file, None) is None:
            raise FileNotFoundError("No such file or directory: %s" % file)

    def expanduser(self, file):
        if file.startswith("~/"):
            return os.path.join(self.MOCK_USER_HOME, file[2:])
        elif file.startswith("~root/"):
            return os.path.join(self.MOCK_ROOT_HOME, file[6:])
        elif file == "~":
            return self.MOCK_USER_HOME
        elif file == "~root":
            return self.MOCK_ROOT_HOME
        else:
            return file

    def chdir(self, dirname):
        assert isinstance(dirname, str), f"The argument to chdir must be a string: {dirname}"
        self.working_dir = os.path.join(self.working_dir, dirname)

    def getcwd(self):
        return self.working_dir

    def abspath(self, file):
        if file.startswith("/"):
            return file
        elif file == ".":
            return self.working_dir
        elif file.startswith("./"):
            return os.path.join(self.working_dir, file[2:])
        else:
            return os.path.join(self.working_dir, file)

    def open(self, file, mode='r', encoding=None):
        if FILE_SYSTEM_VERBOSE:  # pragma: no cover - Debugging option. Doesn't need testing.
            PRINT("Opening %r in mode %r." % (file, mode))
        if mode in ('w', 'wt', 'w+', 'w+t', 'wt+'):
            return self._open_for_write(file_system=self, file=file, binary=False, encoding=encoding)
        elif mode in ('wb', 'w+b', 'wb+'):
            return self._open_for_write(file_system=self, file=file, binary=True, encoding=encoding)
        elif mode in ('r', 'rt', 'r+', 'r+t', 'rt+'):
            return self._open_for_read(file, binary=False, encoding=encoding)
        elif mode in ('rb', 'r+b', 'rb+'):
            return self._open_for_read(file, binary=True, encoding=encoding)
        else:
            raise AssertionError("Mocked io.open doesn't handle mode=%r." % mode)

    def _open_for_read(self, file, binary=False, encoding=None):
        self._maybe_auto_mirror_file(file)
        content = self.files.get(file)
        if content is None:
            raise FileNotFoundError("No such file or directory: %s" % file)
        if FILE_SYSTEM_VERBOSE:  # pragma: no cover - Debugging option. Doesn't need testing.
            PRINT("Read %r from %s." % (content, file))
        return io.BytesIO(content) if binary else io.StringIO(content.decode(encoding or self.default_encoding))

    def _open_for_write(self, file_system, file, binary=False, encoding=None):
        self._do_not_mirror(file)
        return MockFileWriter(file_system=file_system, file=file, binary=binary,
                              encoding=encoding or self.default_encoding)

    @contextlib.contextmanager
    def mock_exists_open_remove(self):
        with mock.patch("os.path.exists", self.exists):
            with mock.patch("io.open", self.open):
                with mock.patch("os.remove", self.remove):
                    yield self

    @contextlib.contextmanager
    def mock_exists_open_remove_abspath_getcwd_chdir(self):
        with mock.patch("os.path.exists", self.exists):
            with mock.patch("io.open", self.open):
                with mock.patch("os.remove", self.remove):
                    with mock.patch("os.path.abspath", self.abspath):
                        with mock.patch("os.getcwd", self.getcwd):
                            with mock.patch("os.chdir", self.chdir):
                                yield self


class MockAWSFileSystem(MockFileSystem):

    def __init__(self, boto3=None, s3=None, versioning_buckets: Union[Literal[True], list, set, tuple] = True,
                 **kwargs):
        self.boto3 = boto3 or MockBoto3()
        self.s3: MockBotoS3Client = s3 or self.boto3.client('s3')
        if versioning_buckets is not True:
            versioning_buckets = set(versioning_buckets or {})
        self.versioning_buckets: Union[Literal[True], set] = versioning_buckets
        super().__init__(**kwargs)

    def add_bucket_versioning(self, versioning_buckets: Union[Literal[True], list, set, tuple]):
        """
        Expands bucket versioning as follows:
          * If the set is already True (all buckets are versioned), nothing can expand that, so do nothing.
          * If the set is given as simply True (all buckets should be versioned), that bcomes the new state.
          * Otherwise, if given a list, set, or tuple of bucket names, and there's already a set, then merge those.
        """
        if versioning_buckets is True or self.versioning_buckets is True:
            self.versioning_buckets = True
            return
        else:
            self.versioning_buckets |= (versioning_buckets or {})
            return

    def bucket_uses_versioning(self, bucket):
        """
        Returns True if the given bucket is needs versioning support.
        This can happen either of two ways:
          * If self.versioning_buckets is True, the result is True.
          * Otherwise, self.versioning_buckets is expected to be a set, and we check if bucket is in that set.
        """
        return self.versioning_buckets is True or bucket in self.versioning_buckets

    def prepare_for_overwrite(self, filename):
        """
        This method overrides a no-op in the parent class and is a hook to allow noticing we'll want to
        archive an old file version when we're about to write new data.
        """
        bucket, key = filename.split('/', 1)
        ignored(key)
        if self.bucket_uses_versioning(bucket):
            self.s3.archive_current_version(filename=filename)


class MockUUIDModule:
    """
    This mock is intended to replace the uuid module itself, not the UUID class (which it ordinarily tries to use).
    In effect, this only changes how UUID strings are generated, not how UUID objects returned are represented.
    However, mocking this is a little complicated because you have to replace individual methods. e.g.,

        import uuid
        def some_test():
            mock_uuid_module = MockUUIDModule()
            assert mock_uuid_module.uuid4() == '00000000-0000-0000-0000-000000000001'
            with mock.patch.object(uuid, "uuid4", mock_uuid_module.uuid4):
                assert uuid.uuid4() == '00000000-0000-0000-0000-000000000002'
    """

    PREFIX = '00000000-0000-0000-0000-'
    PAD = 12
    UUID_CLASS = uuid.UUID

    def __init__(self, prefix=None, pad=None, uuid_class=None):
        self._counter = 1
        self._prefix = self.PREFIX if prefix is None else prefix
        self._pad = self.PAD if pad is None else pad
        self._uuid_class = uuid_class or self.UUID_CLASS

    def _bump(self):
        n = self._counter
        self._counter += 1
        return n

    def uuid4(self):
        return self._uuid_class(self._prefix + str(self._bump()).rjust(self._pad, '0'))


class NotReallyRandom:
    """
    This can be used as a substitute for random to return numbers in a more predictable order.
    """

    def __init__(self):
        self.counter = 0

    def _random_int(self, n):
        """Returns an integer between 0 and n, upper-exclusive, not one of the published 'random' operations."""
        result = self.counter % n
        self.counter += 1
        return result

    def randint(self, a, b):
        """Returns a number between a and b, inclusive at both ends, though not especially randomly."""
        assert isinstance(a, int) and isinstance(b, int) and a < b, "Arguments must be two strictly ascending ints."
        rangesize = int(abs(b-a))+1
        return a + self._random_int(rangesize)

    def choice(self, things):
        return things[self._random_int(len(things))]


class MockResponse:
    """
    This class is useful for mocking requests.Response (the class that comes back from requests.get and friends).

    This mock is useful because requests.Response is a pain to initialize and the common cases are simple to set
    up here by just passing arguments.

    Note, too, that requests.Response differs from pyramid.Response in that in requests.Response the way to get
    the JSON out of a response is to use the .json() method, whereas in pyramid.Response it would just be
    a .json property access. So since this is for mocking requests.Response, we implement the function.
    """

    def __init__(self, status_code=200, json=None, content=None, url=None):
        self.status_code = status_code
        self.url = url or "http://unknown/"
        if json is not None and content is not None:
            raise Exception("MockResponse cannot have both content and json.")
        elif content is not None:
            self.content = content
        elif json is None:
            self.content = ""
        else:
            self.content = json_dumps(json)

    def __str__(self):
        if self.content:
            return "<MockResponse %s %s>" % (self.status_code, self.content)
        else:
            return "<MockResponse %s>" % (self.status_code,)

    @property
    def text(self):
        return self.content

    def json(self):
        return json_loads(self.content)

    def raise_for_status(self):
        if self.status_code >= 300:
            raise Exception("%s raised for status." % self)


class _PrintCapturer:
    """
    This class is used internally to the 'printed_output' context manager to maintain state information
    on what has been printed so far by PRINT within the indicated context.
    """

    def __init__(self):
        self.lines: List[str] = []
        self.last: Optional[str] = None
        self.file_lines: DefaultDict[Optional[str], List[str]] = self._file_lines_dict()
        self.file_last: DefaultDict[Optional[str], Optional[str]] = self._file_last_dict()

    @classmethod
    def _file_lines_dict(cls) -> DefaultDict[Optional[str], List[str]]:
        return defaultdict(lambda: [])

    @classmethod
    def _file_last_dict(cls) -> DefaultDict[Optional[str], Optional[str]]:
        return defaultdict(lambda: None)

    def mock_print_handler(self, *args, **kwargs):
        return self.mock_action_handler(print, *args, **kwargs)

    def mock_input_handler(self, *args, **kwargs):
        return self.mock_action_handler(_mockable_input, *args, **kwargs)

    def mock_action_handler(self, wrapped_action, *args, **kwargs):
        """
        For the simple case of stdout, .last has the last line and .lines contains all lines.
        For all cases, even stdout, .file_lines[fp] and .lines[fp] contain it.
        Notes:
            * If None is the fp value, sys.stdout is used instead. so that None and the current
              value of sys.stdout are synonyms.
            * This mock ignores 'end=' and will treat all calls to PRINT as if they were separate lines.
        """
        text = " ".join(map(str, args))
        texts = remove_suffix('\n', text).split('\n')
        last_text = texts[-1]
        result = wrapped_action(text, **kwargs)  # noQA - This call to print is low-level implementation
        # This only captures non-file output.
        file = kwargs.get('file')
        if file is None:
            file = sys.stdout
        if file is sys.stdout:
            # Easy access to stdout
            self.lines.extend(texts)
            self.last = last_text
            # Every output to stdout is implicitly like output to no file (None)
            self.file_lines[None].extend(texts)
            self.file_last[None] = last_text
        # All accesses of any file/fp, including stdout, get associated with that destination
        self.file_lines[file].extend(texts)
        self.file_last[file] = last_text
        return result  # print did not havea a result, but input does, so be careful to return it.

    def reset(self):
        self.lines = []
        self.last = None
        self.file_lines = self._file_lines_dict()
        self.file_last = self._file_last_dict()


@contextlib.contextmanager
def printed_output():
    """
    This context manager is used to capture output from dcicutils.PRINT for testing.

    The 'printed' object obtained in the 'as' clause of this context manager has two attributes of note:

    * .last contains the last (i.e., most recent) line of output
    * .lines contains all the lines of output in a list

    These values are updated dynamically as output occurs.
    (Only output that is not to a file will be captured.)

    Example:

        def show_succcessor(n):
            PRINT("The successor of %s is %s." % (n, n+1))

        def test_show_successor():
            with printed_output() as printed:
                assert printed.last is None
                assert printed.lines == []
                show_successor(3)
                assert printed.last = 'The successor of 3 is 4.'
                assert printed.lines == ['The successor of 3 is 4.']
                show_successor(4)
                assert printed.last == 'The successor of 4 is 5.'
                assert printed.lines == ['The successor of 3 is 4.', 'The successor of 4 is 5.']
    """

    printed = _PrintCapturer()
    # Use the same PrintCapturer object to capture output from both print and input.
    with local_attrs(PRINT, wrapped_action=printed.mock_print_handler):
        with local_attrs(INPUT, wrapped_action=printed.mock_input_handler):
            yield printed


class MockKeysNotImplemented(NotImplementedError):

    def __init__(self, operation, keys):
        self.operation = operation
        self.keys = keys
        super().__init__("Mocked %s does not implement keywords: %s" % (operation, ", ".join(keys)))


class MockBoto3:

    _CLIENTS = {}

    @classmethod
    def register_client(cls, *, kind):
        """
        A decorator for defining classes of mocked clients. Intended use:

            @MockBoto3.register_client(kind='cloudformation')
            class MockBotoCloudFormationClient:
                ...etc.
        """
        def _wrap(cls_to_wrap):
            if cls._CLIENTS.get(kind):
                raise ValueError(f"A MockBoto3 client for {kind} is already defined.")
            cls._CLIENTS[kind] = cls_to_wrap
            return cls_to_wrap
        return _wrap

    @classmethod
    def _default_mappings(cls):
        return cls._CLIENTS

    def __init__(self, **override_mappings):
        self._mappings = dict(self._default_mappings(), **override_mappings)
        self.shared_reality = {}

    def resource(self, key, **kwargs):
        return self._mappings[key](boto3=self, **kwargs)

    def client(self, kind, **kwargs):
        mapped_class = self._mappings.get(kind)
        logging.info(f"Using {mapped_class} as {kind} boto3.client.")
        if not mapped_class:
            raise NotImplementedError("Unsupported boto3 mock kind:", kind)
        return mapped_class(boto3=self, **kwargs)

    @property
    def session(self):

        class _SessionModule:

            def __init__(self, boto3):
                self.boto3 = boto3

            def Session(self, **kwargs):  # noQA - This name was chosen by AWS, so please don't warn about mixed case
                return MockBoto3Session(boto3=self.boto3, **kwargs)

        return _SessionModule(boto3=self)


@MockBoto3.register_client(kind='session')
class MockBoto3Session:

    _SHARED_DATA_MARKER = "_SESSION_SHARED_DATA_MARKER"

    def __init__(self, *, region_name=None, boto3=None, **kwargs):
        self.boto3 = boto3 or MockBoto3()

        # These kwargs key names are the same as those for the boto3.Session() constructor.
        self._aws_access_key_id = kwargs.get("aws_access_key_id")
        self._aws_secret_access_key = kwargs.get("aws_secret_access_key")
        self._aws_region = region_name

        # This is specific for testing.
        self._aws_credentials_dir = None

    # FYI: Some things to note about how boto3 (and probably any AWS client) reads AWS credentials/region.
    #  - It looks (of course) at envrionment variables before files.
    #  - It wants access key ID and secret access key BOTH to come from the same source,
    #    e.g. does not get access key ID from environment variable and secret access key from file.
    #  - It reads region from EITHER the credentials file OR the config file, the former FIRST;
    #    though (of course) it does NOT read access key ID or secret access key from the config file.
    #  - The aws_access_key_id, aws_secret_access_key, and region properties in the credentials/config
    #    files may be EITHER upper AND/OR lower case; but the environment variables MUST be all upper case.
    #  - If file environment variables (i.e. AWS_SHARED_CREDENTIALS_FILE, AWS_CONFIG_FILE) are NOT set,
    #    i.e. SET to None, it WILL look at the default credentials/config files (e.g. ~/.aws/credentials);
    #    which is why we set to /dev/null in unset_environ_credentials_for_testing().
    #
    # NOTE: The get_credentials method, region_name property, and related methods were
    # added to support usage by 4dn-cloud-infra/setup-remaining-secrets unit tests (June 2022).

    def client(self, service_name, **kwargs):
        return self.boto3.client(service_name, **kwargs)

    AWS_CREDENTIALS_ENVIRON_NAME_OVERRIDES = {
        "AWS_ACCESS_KEY_ID": None,
        "AWS_CONFIG_FILE": "/dev/null",
        "AWS_DEFAULT_REGION": None,
        "AWS_REGION": None,
        "AWS_SECRET_ACCESS_KEY": None,
        "AWS_SESSION_TOKEN": None,
        "AWS_SHARED_CREDENTIALS_FILE": "/dev/null"
    }

    @staticmethod
    @contextlib.contextmanager
    def unset_environ_credentials_for_testing() -> None:
        """
        Unsets all AWS credentials related environment variables for the duration of this context manager.
        """
        with override_environ(**MockBoto3Session.AWS_CREDENTIALS_ENVIRON_NAME_OVERRIDES):
            yield

    def put_credentials_for_testing(self,
                                    aws_access_key_id: str = None,
                                    aws_secret_access_key: str = None,
                                    region_name: str = None,
                                    aws_credentials_dir: str = None) -> None:
        """
        Sets AWS credentials for testing.

        :param aws_access_key_id: AWS access key ID.
        :param aws_secret_access_key: AWS secret access key.
        :param region_name: AWS region name.
        :param aws_credentials_dir: Full path to AWS credentials directory.

        NOTE: Use unset_environ_credentials_for_testing() to clear these environment variables beforehand.
        NOTE: AWS session token not currently handled.
        """

        # These argument names are the same as those for the boto3.Session() constructor.
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_region = region_name

        # This is specific for testing.
        self._aws_credentials_dir = aws_credentials_dir

    @staticmethod
    def _read_aws_credentials_from_file(aws_credentials_file: str) -> (str, str, str):
        """
        Returns from the given AWS credentials file the values of the following properties;
        and returns a tuple with these values, in this listed order:
        aws_access_key_id, aws_secret_access_key, region

        :param aws_credentials_file: Full path to AWS credentials (or config) file.
        :return: Tuple containing aws_access_key_id, aws_secret_access_key, region values; None if not present.
        """
        try:
            if not aws_credentials_file or not os.path.isfile(aws_credentials_file):
                return None, None, None
            config = configparser.ConfigParser()
            config.read(aws_credentials_file)
            if not config or not config.sections() or len(config.sections()) <= 0:
                return None, None, None
            if "default" in config.sections():
                config_section_name = "default"
            else:
                config_section_name = config.sections()[0]
            config_keys_values = {key.lower(): value for key, value in config[config_section_name].items()}
            aws_access_key_id = config_keys_values.get("aws_access_key_id")
            aws_secret_access_key = config_keys_values.get("aws_secret_access_key")
            aws_region = config_keys_values.get("region")
            return aws_access_key_id, aws_secret_access_key, aws_region
        except Exception:
            return None, None, None,

    MISSING_ACCESS_KEY = 'missing access key'
    MISSING_SECRET_KEY = 'missing secret key'

    def get_credentials(self) -> Optional[Boto3Credentials]:
        """
        Returns the AWS credentials (Boto3Credentials) from the aws_access_key_id and aws_secret_access_key values,
        or in the credentials file within the aws_credentials_dir, set in set_credentials_for_testing(); or if not
        set there, then gets them via the standard AWS environment variable names, i.e. AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY, AWS_SHARED_CREDENTIALS_FILE.

        More specifically, returns AWS access key ID and secret access key as a Boto3Credentials,
        from the FIRST of these where BOTH are defined; if BOTH are NOT defined returns None.
        1. From the aws_access_key_id and aws_secret_access_key values set explicitly in set_credentials_for_testing().
        2. From the aws_access_key_id and aws_secret_access_key properties in the credentials
           file within the aws_credentials_dir set explicitly in set_credentials_for_testing().
        3. From the values in the AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY environment variables.
        4. From the aws_access_key_id and aws_secret_access_key properties in the
           credentials file specified by the AWS_SHARED_CREDENTIALS_FILE environment variable.
        5. From the aws_access_key_id and aws_secret_access_key properties in the ~/.aws/credentials file.

        NOTE: Use unset_environ_credentials_for_testing() to clear related environment variables beforehand.
        NOTE: AWS session token not currently handled.

        :return: AWS credentials determined as described above, in a Boto3Credentials object, or None.
        """
        aws_access_key_id = self._aws_access_key_id
        aws_secret_access_key = self._aws_secret_access_key
        if aws_access_key_id and aws_secret_access_key:
            return Boto3Credentials(access_key=aws_access_key_id, secret_key=aws_secret_access_key)
        aws_credentials_dir = self._aws_credentials_dir
        if aws_credentials_dir and os.path.isdir(aws_credentials_dir):
            aws_credentials_file = os.path.join(aws_credentials_dir, "credentials")
            aws_access_key_id, aws_secret_access_key, _ = self._read_aws_credentials_from_file(aws_credentials_file)
            if aws_access_key_id and aws_secret_access_key:
                return Boto3Credentials(access_key=aws_access_key_id, secret_key=aws_secret_access_key)
        aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        if aws_access_key_id and aws_secret_access_key:
            return Boto3Credentials(access_key=aws_access_key_id, secret_key=aws_secret_access_key)
        aws_credentials_file = os.environ.get("AWS_SHARED_CREDENTIALS_FILE", "~/.aws/credentials")
        aws_access_key_id, aws_secret_access_key, _ = self._read_aws_credentials_from_file(aws_credentials_file)
        if aws_access_key_id and aws_secret_access_key:
            return Boto3Credentials(access_key=aws_access_key_id, secret_key=aws_secret_access_key)
        return Boto3Credentials(access_key=self.MISSING_ACCESS_KEY, secret_key=self.MISSING_SECRET_KEY)

    @property
    def region_name(self) -> Optional[str]:
        """
        Returns the AWS region from the _aws_region value, or in the credentials or config file
        within the _aws_credentials_dir, set in the constructor or set_credentials_for_testing();
        or if not set there, then gets it via the standard AWS environment variable names,
        i.e. AWS_REGION, AWS_DEFAULT_REGION, AWS_SHARED_CREDENTIALS_FILE, or AWS_CONFIG_FILE.

        More specifically, returns AWS region from the first of these where defined; if defined returns None.
        1. From the _aws_region value set explicitly in the constructor or set_credentials_for_testing().
        2. From the region property in the credentials file within the
           _aws_credentials_dir set explicitly in the constructor or set_credentials_for_testing().
        3. From the value in the AWS_REGION environment variable (via os.environ).
        4. From the value in the AWS_DEFAULT_REGION environment variable (via os.environ).
        5. From the region property in the credentials file specified
           by the AWS_SHARED_CREDENTIALS_FILE environment variable (via os.environ).
        6. From the region property in the config file specified
           by the AWS_CONFIG_FILE environment variable (via os.environ).
        7. From the region property in the ~/.aws/credentials file.
        8. From the region property in the ~/.aws/config file.

        NOTE: Use unset_environ_credentials_for_testing() to clear related environment variables beforehand.

        :return: AWS region name determined as described above, or None.
        """
        aws_region = self._aws_region
        if aws_region:
            return aws_region
        if self._aws_credentials_dir:
            aws_credentials_file = os.path.join(self._aws_credentials_dir, "credentials")
            _, _, aws_region = self._read_aws_credentials_from_file(aws_credentials_file)
            if aws_region:
                return aws_region
            aws_config_file = os.path.join(self._aws_credentials_dir, "config")
            _, _, aws_region = self._read_aws_credentials_from_file(aws_config_file)
            if aws_region:
                return aws_region
        aws_region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION"))
        if aws_region:
            return aws_region
        aws_credentials_file = os.environ.get("AWS_SHARED_CREDENTIALS_FILE", "~/.aws/credentials")
        _, _, aws_region = self._read_aws_credentials_from_file(aws_credentials_file)
        if aws_region:
            return aws_region
        aws_config_file = os.environ.get("AWS_CONFIG_FILE", "~/.aws/config")
        _, _, aws_region = self._read_aws_credentials_from_file(aws_config_file)
        return aws_region


class MockBoto3IamUserAccessKeyPair:
    def __init__(self) -> None:
        self._id = str(uuid.uuid4())
        self._secret = str(uuid.uuid4())
        self._create_date = datetime.datetime.now()

    @property
    def id(self) -> str:
        return self._id

    @property
    def secret(self) -> str:
        return self._secret

    def get(self, key: str) -> Any:
        if key == "AccessKeyId":
            return self._id
        elif key == "CreateDate":
            return self._create_date
        return None

    def __getitem__(self, key: str) -> Any:
        return self.get(key)


class MockBoto3IamUserAccessKeyPairCollection:
    def __init__(self) -> None:
        self._access_key_pairs = []

    def add(self, access_key_pair: object) -> None:
        self._access_key_pairs.append(access_key_pair)

    def get(self, key: str) -> Any:
        if key == "AccessKeyMetadata":
            return self._access_key_pairs
        return None

    def __getitem__(self, key: str) -> Any:
        return self.get(key)


class MockBoto3IamUser:
    def __init__(self, name: str) -> None:
        self._name = name
        self.mocked_access_keys = MockBoto3IamUserAccessKeyPairCollection()

    @property
    def name(self) -> str:
        return self._name

    def create_access_key_pair(self) -> object:
        access_key_pair = MockBoto3IamUserAccessKeyPair()
        self.mocked_access_keys.add(access_key_pair)
        return access_key_pair


class MockBoto3IamUserCollection:
    def __init__(self) -> None:
        self._users = []

    def all(self) -> list:
        return self._users


class MockBoto3IamRoleCollection:
    def __init__(self) -> None:
        self._roles = []

    def __getitem__(self, key: str) -> Any:
        if key == "Roles":
            return self._roles
        return None


class MockBoto3IamRole:
    def __init__(self, arn: str) -> None:
        self._arn = arn

    def __getitem__(self, key: str) -> Any:
        if key == "Arn":
            return self._arn
        return None


# This MockBoto3Iam class is a minimal implementation, just enough to support the
# original usage by 4dn-cloud-infra/setup-remaining-secrets unit tests (June 2022).
@MockBoto3.register_client(kind='iam')
class MockBoto3Iam:

    _SHARED_DATA_MARKER = "_IAM_SHARED_DATA_MARKER"

    def __init__(self, *, boto3=None) -> None:
        self.boto3 = boto3 or MockBoto3()

    def _mocked_shared_data(self) -> dict:
        shared_reality = self.boto3.shared_reality
        shared_data = shared_reality.get(self._SHARED_DATA_MARKER)
        if shared_data is None:
            shared_data = shared_reality[self._SHARED_DATA_MARKER] = {}
        return shared_data

    def _mocked_users(self) -> MockBoto3IamUserCollection:
        mocked_shared_data = self._mocked_shared_data()
        mocked_users = mocked_shared_data.get("users")
        if not mocked_users:
            mocked_shared_data["users"] = mocked_users = MockBoto3IamUserCollection()
        return mocked_users

    def _mocked_roles(self) -> MockBoto3IamRoleCollection:
        mocked_shared_data = self._mocked_shared_data()
        mocked_roles = mocked_shared_data.get("roles")
        if not mocked_roles:
            mocked_shared_data["roles"] = mocked_roles = MockBoto3IamRoleCollection()
        return mocked_roles

    def put_users_for_testing(self, users: list) -> None:
        if isinstance(users, list) and len(users) > 0:
            existing_users = self._mocked_users().all()
            for user in users:
                if user not in existing_users:
                    existing_users.append(MockBoto3IamUser(user))

    def put_roles_for_testing(self, roles: list) -> None:
        if isinstance(roles, list) and len(roles) > 0:
            existing_roles = self._mocked_roles()["Roles"]
            for role in roles:
                if role not in existing_roles:
                    existing_roles.append(MockBoto3IamRole(role))

    @property
    def users(self) -> object:
        return self._mocked_users()

    def list_roles(self) -> object:
        return self._mocked_roles()

    def list_access_keys(self, UserName: str) -> Optional[MockBoto3IamUserAccessKeyPairCollection]:  # noQA - Argument names chosen for AWS consistency
        existing_users = self._mocked_users().all()
        for existing_user in existing_users:
            if existing_user.name == UserName:
                return existing_user.mocked_access_keys
        return None


class MockBoto3OpenSearchDomain:
    def __init__(self, domain_name: str, domain_endpoint_vpc: str, domain_endpoint_https: bool) -> None:
        self._domain_name = domain_name
        self._domain_endpoint_vpc = domain_endpoint_vpc
        self._domain_endpoint_https = domain_endpoint_https

    def __getitem__(self, key: str) -> Any:
        if key == "DomainName":
            return self._domain_name
        elif key == "DomainStatus":
            return {
                "Endpoints": {
                    "vpc": self._domain_endpoint_vpc
                },
                "DomainEndpointOptions": {
                    "EnforceHTTPS": self._domain_endpoint_https
                }
            }
        return None


class MockBoto3OpenSearchDomains:
    def __init__(self) -> None:
        self._domains = []

    def add(self, domain: object) -> None:
        self._domains.append(domain)

    def __getitem__(self, key: str) -> Any:
        if key == "DomainNames":
            return self._domains
        return None


# This MockBoto3OpenSearch class is a minimal implementation, just enough to support
# the original usage by 4dn-cloud-infra/setup-remaining-secrets unit tests (June 2022).
@MockBoto3.register_client(kind='opensearch')
class MockBoto3OpenSearch:

    _SHARED_DATA_MARKER = '_OPENSEARCH_SHARED_DATA_MARKER'

    def __init__(self, boto3=None) -> None:
        self.boto3 = boto3 or MockBoto3()

    def _mocked_shared_data(self) -> dict:
        shared_reality = self.boto3.shared_reality
        shared_data = shared_reality.get(self._SHARED_DATA_MARKER)
        if shared_data is None:
            shared_data = shared_reality[self._SHARED_DATA_MARKER] = {}
        return shared_data

    def _mocked_domains(self) -> MockBoto3OpenSearchDomains:
        mocked_shared_data = self._mocked_shared_data()
        mocked_domains = mocked_shared_data.get("domains")
        if not mocked_domains:
            mocked_shared_data["domains"] = mocked_domains = MockBoto3OpenSearchDomains()
        return mocked_domains

    def put_domain_for_testing(self, domain_name: str, domain_endpoint_vpc: str, domain_endpoint_https: bool) -> None:
        domains = self._mocked_domains()
        domains.add(MockBoto3OpenSearchDomain(domain_name, domain_endpoint_vpc, domain_endpoint_https))

    def list_domain_names(self) -> MockBoto3OpenSearchDomains:
        return self._mocked_domains()

    def describe_domain(self, DomainName: str) -> Optional[dict]:  # noQA - Argument names chosen for AWS consistency
        domains = self._mocked_domains()["DomainNames"]
        if domains:
            for domain in domains:
                if domain["DomainName"] == DomainName:
                    return domain
        return None


# This MockBoto3Sts class is a minimal implementation, just enough to support the
# original usage by 4dn-cloud-infra/setup-remaining-secrets unit tests (June 2022).
@MockBoto3.register_client(kind='sts')
class MockBoto3Sts:

    _SHARED_DATA_MARKER = '_STS_SHARED_DATA_MARKER'

    def __init__(self, boto3=None) -> None:
        self.boto3 = boto3 or MockBoto3()

    def _mocked_shared_data(self) -> dict:
        shared_reality = self.boto3.shared_reality
        shared_data = shared_reality.get(self._SHARED_DATA_MARKER)
        if shared_data is None:
            shared_data = shared_reality[self._SHARED_DATA_MARKER] = {}
        return shared_data

    def _mocked_caller_identity(self) -> dict:
        mocked_shared_data = self._mocked_shared_data()
        mocked_caller_identity = mocked_shared_data.get("caller_identity")
        if not mocked_caller_identity:
            mocked_shared_data["caller_identity"] = mocked_caller_identity = {}
        return mocked_caller_identity

    def put_caller_identity_for_testing(self, account: str, user_arn: str = None) -> None:
        caller_identity = self._mocked_caller_identity()
        caller_identity["Account"] = account
        caller_identity["Arn"] = user_arn

    def get_caller_identity(self) -> dict:
        return self._mocked_caller_identity()


class MockBoto3KmsKey:
    def __init__(self, key_id: str):
        self._key_id = key_id

    def __getitem__(self, key: str) -> Any:
        if key == "KeyId":
            return self._key_id
        return None


class MockBoto3KmsKeys:
    def __init__(self):
        self._keys = []

    def add(self, key: object) -> None:
        self._keys.append(key)

    def __getitem__(self, key: str):
        if key == "Keys":
            return self._keys
        return None


# This MockBoto3Kms class is a minimal implementation, just enough to support the
# original usage by 4dn-cloud-infra/setup-remaining-secrets unit tests (June 2022).
@MockBoto3.register_client(kind='kms')
class MockBoto3Kms:

    _SHARED_DATA_MARKER = '_KMS_SHARED_DATA_MARKER'

    def __init__(self, *, boto3=None) -> None:
        self.boto3 = boto3 or MockBoto3()

    def _mocked_shared_data(self) -> dict:
        shared_reality = self.boto3.shared_reality
        shared_data = shared_reality.get(self._SHARED_DATA_MARKER)
        if shared_data is None:
            shared_data = shared_reality[self._SHARED_DATA_MARKER] = {}
        return shared_data

    def _mocked_keys(self) -> MockBoto3KmsKeys:
        mocked_shared_data = self._mocked_shared_data()
        mocked_keys = mocked_shared_data.get("keys")
        if not mocked_keys:
            mocked_shared_data["keys"] = mocked_keys = MockBoto3KmsKeys()
        return mocked_keys

    def _mocked_key_policies(self) -> dict:
        mocked_shared_data = self._mocked_shared_data()
        mocked_key_policies = mocked_shared_data.get("key_policies")
        if not mocked_key_policies:
            mocked_shared_data["key_policies"] = mocked_key_policies = {}
        return mocked_key_policies

    def put_key_for_testing(self, key_id: str) -> None:
        keys = self._mocked_keys()
        keys.add(MockBoto3KmsKey(key_id))

    def put_key_policy_for_testing(self, key_id: str, key_policy: dict) -> None:
        if isinstance(key_policy, dict):
            key_policy_string = json_dumps(key_policy)
        elif isinstance(key_policy, str):
            key_policy_string = key_policy
        else:
            raise ValueError("Policy must but dictionary or string.")
        self.put_key_policy(KeyId=key_id, Policy=key_policy_string, PolicyName="default")

    def list_keys(self) -> MockBoto3KmsKeys:
        return self._mocked_keys()

    def describe_key(self, KeyId: str) -> Optional[dict]:  # noQA - Argument names chosen for AWS consistency
        keys = self._mocked_keys()["Keys"]
        if keys:
            for key in keys:
                if key["KeyId"] == KeyId:
                    return {
                        "KeyMetadata": {
                            "KeyManager": "CUSTOMER"
                        }
                    }
        return None

    def put_key_policy(self, KeyId: str, Policy: str, PolicyName: str) -> None:  # noQA - Argument names chosen for AWS consistency
        if not KeyId:
            raise ValueError(f"KeyId value must be set for kms.put_key_policy.")
        if PolicyName != "default":
            raise ValueError(f"PolicyName value must be 'default' for kms.put_key_policy.")
        key_policies = self._mocked_key_policies()
        key_policies[KeyId] = {"Policy": Policy}

    def get_key_policy(self, KeyId: str, PolicyName: str) -> Optional[dict]:  # noQA - Argument names chosen for AWS consistency
        if not KeyId:
            raise ValueError(f"KeyId value must be set for kms.get_key_policy.")
        if PolicyName != "default":
            raise ValueError(f"PolicyName value must be 'default' for kms.get_key_policy.")
        key_policies = self._mocked_key_policies()
        return key_policies.get(KeyId)


class MockBoto3Client:

    MOCK_CONTENT_TYPE = 'text/xml'
    MOCK_CONTENT_LENGTH = 350
    MOCK_RETRY_ATTEMPTS = 0
    MOCK_STATUS_CODE = 200

    @classmethod
    def compute_mock_response_metadata(cls, request_id=None, http_status_code=None, retry_attempts=None):
        # It may be that uuid.uuid4() is further mocked, but either way it needs to return something
        # that is used in two places consistently.
        request_id = request_id or str(uuid.uuid4())
        http_status_code = http_status_code or cls.MOCK_STATUS_CODE
        retry_attempts = retry_attempts or cls.MOCK_RETRY_ATTEMPTS
        return {
            'RequestId': request_id,
            'HTTPStatusCode': http_status_code,
            'HTTPHeaders': cls.compute_mock_request_headers(request_id=request_id),
            'RetryAttempts': retry_attempts,
        }

    @classmethod
    def compute_mock_request_headers(cls, request_id):
        # request_date_str = 'Thu, 01 Oct 2020 06:00:00 GMT'
        #   or maybe pytz.UTC.localize(datetime.datetime.utcnow()), where .utcnow() may be further mocked
        # request_content_type = self.MOCK_CONTENT_TYPE
        return {
            'x-amzn-requestid': request_id,
            # We probably don't need these other values, and if we do we might need different values,
            # so we prefer not to provide mock values until/unless need is shown. -kmp 15-Oct-2020
            #
            # 'date': request_date_str,  # see above
            # 'content-type': 'text/xml',
            # 'content-length': 350,
        }


@MockBoto3.register_client(kind='lambda')
class MockBoto3Lambda(MockBoto3Client):

    _UNSUPPLIED = object()
    _MOCKED_LAMBDAS = '_MOCKED_LAMBDAS'
    _DEFAULT_MAX_ITEMS = 50

    def __init__(self, region_name=None, boto3=None):
        self.region_name = region_name
        self.boto3 = boto3 or MockBoto3()

    def _lambdas(self):
        shared_reality = self.boto3.shared_reality
        lambdas = shared_reality.get(self._MOCKED_LAMBDAS)
        if lambdas is None:
            # Export the list in case other clients want the same list.
            shared_reality[self._MOCKED_LAMBDAS] = lambdas = {}
        return lambdas

    def _some_lambdas(self, marker=_UNSUPPLIED, max_items=_DEFAULT_MAX_ITEMS):
        all_entries = list(self._lambdas().values())
        if not all_entries:
            return all_entries, None
        idx = None
        if marker is not self._UNSUPPLIED:
            for i in range(len(all_entries)):
                entry = all_entries[i]
                if entry['FunctionName'] == marker:
                    idx = i
                    break
            if idx is None:
                raise RuntimeError(f"Invalid marker: {marker}")
        if idx is None:
            idx = 0
        rest = all_entries[idx:]
        more = rest[max_items:]
        some = rest[:max_items]
        next = more[0]['FunctionName'] if more else None
        return some, next

    def register_lambda_for_testing(self, key, **data):
        entry = data.copy()
        entry['FunctionName'] = key
        self._lambdas()[key] = entry

    def register_lambdas_for_testing(self, lambdas: dict) -> None:
        for key, data in lambdas.items():
            self.register_lambda_for_testing(key, **data)

    def list_functions(self, MaxItems=_DEFAULT_MAX_ITEMS, Marker=_UNSUPPLIED):
        assert Marker is self._UNSUPPLIED or isinstance(Marker, str)
        functions, next_marker = self._some_lambdas(max_items=MaxItems, marker=Marker)
        result = {
            'MetaData': self.compute_mock_response_metadata(),
            'Functions': functions,
        }
        if next_marker is not None:
            result['NextMarker'] = next_marker
        return result


@MockBoto3.register_client(kind='ecr')
class MockBotoECR(MockBoto3Client):

    # Our mock acts as if this is the current account number
    _MOCK_ACCOUNT_NUMBER = '111222333444'

    # Our mocked image creation times are this plus a small number of days
    _MOCK_PUSH_TIME_BASE = datetime.datetime(2020, 1, 1)   # arbitrary

    # Our mocked image sizes are this plus a small amount
    _MOCK_IMAGE_SIZE_BASE = 900000000

    # When nextToken is not in play for some operations, this can be used.
    # It is important that it be both a string and false, hence the empty string.
    _NO_NEXT_TOKEN = ""

    def __init__(self, region_name: str = None, boto3: MockBoto3 = None) -> None:
        self.region_name = region_name
        self.account_number = self._MOCK_ACCOUNT_NUMBER
        self.boto3 = boto3 or MockBoto3()
        self.images = None
        self.digest_counter = 0
        reality = self.boto3.shared_reality
        if 'ecs_repositories' in reality:
            self.ecs_repositories = reality['ecs_repositories']
        else:
            reality['ecs_repositories'] = repositories = {}
            self.ecs_repositories = repositories

    def describe_repositories(self):
        return {
            "repositories": [
                repo['metadata'] for repo in self.ecs_repositories.values()
            ],
            "ResponseMetadata": self.compute_mock_response_metadata()
        }

    def add_image_repository_for_testing(self, repository):
        # A sample of a result from boto3.client('ecr').describe_repositories() might return.
        # {
        #     "repositories":
        #         {
        #             "repositoryArn": "arn:aws:ecr:us-east-1:643366669028:repository/fourfront-mastertest",
        #             "registryId": "643366669028",
        #             "repositoryName": "fourfront-mastertest",
        #             "repositoryUri": "643366669028.dkr.ecr.us-east-1.amazonaws.com/fourfront-mastertest",
        #             "createdAt": "2021-10-07 14:57:06-04:00",
        #             "imageTagMutability": "MUTABLE",
        #             "imageScanningConfiguration": {
        #             "scanOnPush": true
        #         },
        #         "encryptionConfiguration": {
        #             "encryptionType": "AES256"
        #         }
        #     },
        #     "ResponseMetadata": {...}
        # }
        entry = {
            "metadata": {
                "repositoryName": repository,
                "repositoryUri": f"{self.account_number}.dkr.ecr.{self.region_name}.amazonaws.com/{repository}",
            },
            "images": [],  # not something AWS keeps track of
        }
        self.ecs_repositories[repository] = entry
        return entry

    def get_repository_for_testing(self, repository):
        try:
            return self.ecs_repositories[repository]
        except Exception:
            raise AssertionError(f"Undefined mock ECS repository {repository}."
                                 f" You may need to add_image_repository_for_testing.")

    def get_repository_images_for_testing(self, repository) -> List[dict]:
        return self.get_repository_for_testing(repository)['images']

    def get_image_repository_metadata_for_testing(self, repository) -> List[dict]:
        return self.get_repository_for_testing(repository)['metadata']

    def add_image_metadata_for_testing(self, repository, image_digest=None, pushed_at=None, tags=None):
        # Typical image restriction from AWS...
        # {
        #     "registryId": "262461168236",
        #     "repositoryName": "main",
        #     "imageDigest": "sha256:177aa1b7188e9eb81b0a8405653c2e0131119c74b04d8a70ed3ec5cedc833ab2",
        #     "imageTags": ["latest"],
        #     "imageSizeInBytes": 975668379,
        #     "imagePushedAt": "2022-08-29 10:46:49-04:00",
        #     "imageManifestMediaType": "application/vnd.docker.distribution.manifest.v2+json",
        #     "artifactMediaType": "application/vnd.docker.container.image.v1+json",
        #     "lastRecordedPullTime": "2022-09-01 14:20:08.711000-04:00"
        # }
        self.digest_counter += 1
        tag_set = set(tags) if tags else set()
        entry = {
            "_mock_digest_counter": self.digest_counter,
            "imageDigest": image_digest or f"sha256:{str(self.digest_counter).rjust(64, '0')}",
            # Note that mock won't error-check duplication, but tags are supposed to be unique
            "imageTags": tag_set,
            "imageManifestMediateType": "application/vnd.docker.distribution.manifest.v2+json",
            "artifactMediaType": "application/vnd.docker.container.image.v1+json",
            "imageSizeInBytes": self._MOCK_IMAGE_SIZE_BASE + self.digest_counter,  # make all have different sizes
            "imagePushedAt": pushed_at or self._MOCK_PUSH_TIME_BASE + datetime.timedelta(days=self.digest_counter),
            "lastRecordedPullTime": None,
        }
        images = self.get_repository_images_for_testing(repository)
        for image in images:
            image['imageTags'] = image['imageTags'] - tag_set
        images.append(entry)
        return entry

    def describe_images(self, repositoryName, imageIds=None, maxResults=None, nextToken=_NO_NEXT_TOKEN):  # noQA - AWS choices
        if maxResults is None:
            maxResults = 100
        assert isinstance(maxResults, int) and 1 <= maxResults <= 1000, "maxResults must be an integer from 1 to 1000."
        images = self.get_repository_images_for_testing(repositoryName)
        new_next_token = None
        if nextToken:
            startpos = int(nextToken)
            endpos = startpos + maxResults
            new_next_token = str(endpos + 1) if len(images) > endpos else self._NO_NEXT_TOKEN
            found_images = images[startpos:endpos]
        elif imageIds:
            image_digests_to_find = set()
            image_tags_to_find = set()
            for entry in imageIds:
                digest = entry.get('imageDigest')
                if digest:
                    image_digests_to_find.add(digest)
                else:
                    tag = entry.get('imageTag')
                    image_tags_to_find.add(tag)
            found_images = []
            for image in images:
                digest = image['imageDigest']
                # print(f"Searching for digest={digest!r} in {image_digests_to_find!r}...")
                # print(f"Then searching image_tags_to_find={image_tags_to_find!r} & tags={image['imageTags']}")
                if digest in image_digests_to_find:
                    # print("Found digest")
                    found_images.append(image)
                elif image_tags_to_find & image['imageTags']:
                    # print("Found tags")
                    found_images.append(image)
                else:
                    # print("Neither found")
                    pass
        else:
            found_images = images[:maxResults]
            new_next_token = str(maxResults + 1) if len(images) > maxResults else self._NO_NEXT_TOKEN
        found_images = [dict(image,
                             imageTags=sorted(list(image['imageTags'])),
                             registryId=self.account_number,
                             repositoryName=repositoryName)
                        for image in found_images]
        result = {
            'imageDetails': found_images,
        }
        if new_next_token:
            result['nextToken'] = new_next_token
        return result


# This MockBoto3Ec2 class is a minimal implementation, just enough to support the
# original usage by 4dn-cloud-infra/update-sentieon-security unit tests (July 2022).
@MockBoto3.register_client(kind='ec2')
class MockBoto3Ec2:

    _SHARED_DATA_MARKER = '_EC2_SHARED_DATA_MARKER'

    def __init__(self, boto3: MockBoto3 = None) -> None:
        self.boto3 = boto3 or MockBoto3()

    def _mocked_shared_data(self) -> dict:
        shared_reality = self.boto3.shared_reality
        shared_data = shared_reality.get(self._SHARED_DATA_MARKER)
        if shared_data is None:
            shared_data = shared_reality[self._SHARED_DATA_MARKER] = {}
        return shared_data

    def _mocked_security_groups(self) -> list:
        mocked_shared_data = self._mocked_shared_data()
        mocked_security_groups = mocked_shared_data.get("security_groups")
        if not mocked_security_groups:
            mocked_shared_data["security_groups"] = mocked_security_groups = []
        return mocked_security_groups

    @staticmethod
    def _security_group_rules_are_equal(security_group_rule_a: dict, security_group_rule_b: dict) -> bool:
        """
        Returns True iff the two given security group rules are equal, otherwise False.
        Note the description is not included in this comparison.

        :param security_group_rule_a: Security group rule.
        :param security_group_rule_b: Security group rule.
        :return: True if the given security group rules are equal, otherwise False.
        """
        return (security_group_rule_a.get("IpProtocol") == security_group_rule_b.get("IpProtocol")
                and security_group_rule_a.get("ToPort") == security_group_rule_b.get("ToPort")
                and security_group_rule_a.get("FromPort") == security_group_rule_b.get("FromPort")
                and security_group_rule_a.get("IsEgress") == security_group_rule_b.get("IsEgress")
                and security_group_rule_a.get("CidrIpv4") == security_group_rule_b.get("CidrIpv4"))

    def put_security_group_rule_for_testing(self, security_group_name: str, security_group_rule: dict) -> str:
        """
        Define the given security group rule for the given security group name.
        The given security group rule must contain a key for "GroupId".
        And if the "SecurityGroupRuleId" key is not set one (a UUID) will be generated/set for it.

        FYI: Rules returned by describe_security_group_rules look like:
        { "SecurityGroupRuleId": "sgr-004642a15cf58f9ad",
          "GroupId": "sg-0561068965d07c4af",
          "IsEgress": true,
          "IpProtocol": "icmp",
          "FromPort": 4,
          "ToPort": -1,
          "CidrIpv4": "0.0.0.0/0",
          "Description": "ICMP for sentieon server",
          "Tags": [] }

        FYI: Rules passed to authorized_security_group_ingress authorized_security_group_egress look like:
        { "IpProtocol": "tcp",
           "FromPort": 8990,
           "ToPort": 8990,
           "IpRanges": [{ "CidrIp": sentieon_server_cidr, "Description": "allows comm with sentieon" }] }

        We will store rules via _mocked_security_groups as the former, like so:
        [ { "name": <your-security-group-name>
            "id": <your-security-group-id>
            "rules": [ <dict-for-security-group-rule-in-the-first-security-group-rule-format-above> ] } ]

        :param security_group_name: Security group name.
        :param security_group_rule: Security group rule.
        :return: Security group rule ID of given/defined rule.
        """

        if not security_group_name:
            raise ValueError(f"Missing name for mocked put_security_group_rule_for_testing.")
        if not security_group_rule:
            raise ValueError(f"Missing rule for mocked put_security_group_rule_for_testing.")
        security_group_id = security_group_rule.get("GroupId")
        if not security_group_id:
            raise ValueError(f"Missing rule GroupId for mocked put_security_group_rule_for_testing.")
        security_group_rule = copy.deepcopy(security_group_rule)
        security_group_rule_id = security_group_rule.get("SecurityGroupRuleId")
        if not security_group_rule_id:
            security_group_rule["SecurityGroupRuleId"] = security_group_rule_id = str(uuid.uuid4())
        mocked_security_groups = self._mocked_security_groups()
        mocked_security_group = [mocked_security_group
                                 for mocked_security_group in mocked_security_groups
                                 if mocked_security_group.get("name") == security_group_name]
        if mocked_security_group:
            # Here, a mocked security group already exists for the given security group name.
            mocked_security_group = mocked_security_group[0]
            if mocked_security_group["id"] != security_group_id:
                # The security group ID for/within the given security group rule does not match the
                # security group ID for the existing mocked security group of the given security group name.
                raise ValueError(f"Mismatched rule GroupId for mocked put_security_group_rule_for_testing.")
            # Check for inconsistencies between the given security group name and the
            # security group ID within the given rule, and any already existing mocked rules.
            for group in mocked_security_groups:
                if group["name"] != security_group_name:
                    for rule in group["rules"]:
                        if rule["GroupId"] == security_group_id:
                            # The security group ID for/within the given rule is already associated with an
                            # existing mocked security group with a different name than the given security group name.
                            raise ValueError(f"Mismatched rule GroupId for mocked put_security_group_rule_for_testing.")
            for rule in mocked_security_group["rules"]:
                if rule["GroupId"] != security_group_id:
                    # The given security group name already has an existing associated mocked rule
                    # with a security group ID different from the one for/within the given rule.
                    raise ValueError(f"Mismatched rule GroupId for mocked put_security_group_rule_for_testing.")
            # Check if this is a rule already exists.
            for mocked_security_group_rule in mocked_security_group["rules"]:
                if self._security_group_rules_are_equal(mocked_security_group_rule, security_group_rule):
                    # Duplicate rule. Real boto3 error looks like this FYI:
                    # An error occurred (InvalidPermission.Duplicate) when calling the AuthorizeSecurityGroupIngress
                    # operation: the specified rule "peer: 0.0.0.0/0, ICMP, type: ALL, code: ALL, ALLOW" already exists
                    op = f"AuthorizeSecurityGroup{'Egress' if security_group_rule.get('IsEgress') else 'Ingress'}"
                    message = "the specified rule already exists"
                    code = "InvalidPermission.Duplicate"
                    raise ClientError(operation_name=op, error_response={"Error": {"Code": code, "Message": message}})
            mocked_security_group["rules"].append(security_group_rule)
        else:
            # Here, no mocked security group yet exists for the given security group name.
            mocked_security_groups.append(
                {"name": security_group_name, "id": security_group_id, "rules": [security_group_rule]})
        return security_group_rule_id

    def put_security_group_for_testing(self, security_group_name: str, security_group_id: str = None) -> str:
        if not security_group_name:
            raise ValueError(f"Missing name for mocked put_security_group_for_testing.")
        mocked_security_groups = self._mocked_security_groups()
        mocked_security_group = [mocked_security_group
                                 for mocked_security_group in mocked_security_groups
                                 if mocked_security_group.get("name") == security_group_name]
        if not mocked_security_group:
            if not security_group_id:
                security_group_id = str(uuid.uuid4())
            mocked_security_groups.append({"name": security_group_name, "id": security_group_id, "rules": []})
            return security_group_id
        else:
            return mocked_security_group[0].get("id")

    def describe_security_groups(self, Filters: Optional[list] = None) -> dict:  # noQA - Argument names chosen for AWS consistency
        if Filters:
            if len(Filters) != 1:
                raise ValueError(f"Filters must be single-item list for mocked describe_security_groups.")
            filter_name = Filters[0].get("Name")
            if filter_name != "tag:Name":
                raise ValueError(f"Name must be tag:Name for mocked describe_security_groups.")
            filter_values = Filters[0].get("Values")
            if not isinstance(filter_values, list) or len(filter_values) != 1:
                raise ValueError(f"Values must be a single-item list for mocked describe_security_groups.")
            security_group_name = filter_values[0]
            if not security_group_name:
                raise ValueError(f"Values must contain a security group name for mocked describe_security_groups.")
        else:
            security_group_name = None
        mocked_security_groups = self._mocked_security_groups()
        result_groups = []
        for group in mocked_security_groups:
            if not security_group_name or group["name"] == security_group_name:
                result_groups.append({"GroupId": group["id"]})
        return {"SecurityGroups": result_groups}

    def describe_security_group_rules(self, Filters: list) -> dict:  # noQA - Argument names chosen for AWS consistency
        if not Filters or len(Filters) != 1:
            raise ValueError(f"Filters must be single-item list for mocked describe_security_group_rules.")
        filter_name = Filters[0].get("Name")
        if filter_name != "group-id":
            raise ValueError(f"Name must be group-id for mocked describe_security_group_rules.")
        filter_values = Filters[0].get("Values")
        if not isinstance(filter_values, list) or len(filter_values) != 1:
            raise ValueError(f"Values must be a single-item list for mocked describe_security_group_rules.")
        security_group_id = filter_values[0]
        if not security_group_id:
            raise ValueError(f"Values must contain a security group ID for mocked describe_security_group_rules.")
        mocked_security_groups = self._mocked_security_groups()
        for group in mocked_security_groups:
            for rule in group["rules"]:
                if rule["GroupId"] == security_group_id:
                    return {"SecurityGroupRules": group["rules"]}
        return {"SecurityGroupRules": []}

    def _authorize_one_security_group(self, security_group_id: str, security_group_rule: dict, egress: bool) -> str:
        if not security_group_id:
            raise ValueError(f"Missing security group ID for mocked _authorize_one_security_group.")
        if not security_group_rule:
            raise ValueError(f"Missing security group rule for mocked _authorize_one_security_group.")
        mocked_security_groups = self._mocked_security_groups()
        security_group_name = None
        for group in mocked_security_groups:
            if group["id"] == security_group_id:
                security_group_name = group["name"]
        if not security_group_name:
            raise ValueError("Security group ID does not exist for _authorize_one_security_group")
        ip_ranges = security_group_rule.get("IpRanges")
        if isinstance(ip_ranges, list) and ip_ranges:
            cidr_ip = ip_ranges[0].get("CidrIp")
            description = ip_ranges[0].get("Description")
        else:
            cidr_ip = ""
            description = ""
        security_group_rule = {
            "SecurityGroupRuleId": str(uuid.uuid4()),
            "GroupId": security_group_id,
            "IsEgress": egress,
            "IpProtocol": security_group_rule.get("IpProtocol"),
            "FromPort": security_group_rule.get("FromPort"),
            "ToPort": security_group_rule.get("ToPort"),
            "CidrIpv4": cidr_ip,
            "Description": description
        }
        return self.put_security_group_rule_for_testing(security_group_name, security_group_rule)

    def _authorize_security_group(self, GroupId: str, IpPermissions: list, egress: bool) -> dict:  # noQA - Argument names chosen for AWS consistency
        response = {"SecurityGroupRules": []}
        for ip_permission in IpPermissions:
            security_group_rule_id = self._authorize_one_security_group(GroupId, ip_permission, egress)
            response["SecurityGroupRules"].append({"SecurityGroupRuleId": security_group_rule_id})
        return response

    def authorize_security_group_ingress(self, GroupId: str, IpPermissions: list) -> dict:  # noQA - Argument names chosen for AWS consistency
        return self._authorize_security_group(GroupId, IpPermissions, egress=False)

    def authorize_security_group_egress(self, GroupId: str, IpPermissions: list) -> dict:  # noQA - Argument names chosen for AWS consistency
        return self._authorize_security_group(GroupId, IpPermissions, egress=True)

    def _revoke_security_group(self, GroupId: str, SecurityGroupRuleIds: list, egress: bool) -> None:  # noQA - Argument names chosen for AWS consistency
        if GroupId and SecurityGroupRuleIds:
            mocked_security_groups = self._mocked_security_groups()
            for group in mocked_security_groups:
                if group["id"] == GroupId:
                    rules = group.get("rules")
                    for rule_index, rule in enumerate(rules):
                        if rule.get("IsEgress") == egress and rule["SecurityGroupRuleId"] in SecurityGroupRuleIds:
                            del rules[rule_index]

    def revoke_security_group_ingress(self, GroupId: str, SecurityGroupRuleIds: list) -> None:  # noQA - Argument names chosen for AWS consistency
        self._revoke_security_group(GroupId, SecurityGroupRuleIds, egress=False)

    def revoke_security_group_egress(self, GroupId: str, SecurityGroupRuleIds: list) -> None:  # noQA - Argument names chosen for AWS consistency
        self._revoke_security_group(GroupId, SecurityGroupRuleIds, egress=True)


@MockBoto3.register_client(kind='secretsmanager')
class MockBoto3SecretsManager:

    _SECRETS_MARKER = '_MOCKED_SECRETS'

    def __init__(self, region_name=None, boto3=None):
        self.region_name = region_name
        self.boto3 = boto3 or MockBoto3()

    def _mocked_secrets(self):
        shared_reality = self.boto3.shared_reality
        secrets = shared_reality.get(self._SECRETS_MARKER)
        if secrets is None:
            # Export the list in case other clients want the same list.
            shared_reality[self._SECRETS_MARKER] = secrets = {}
        return secrets

    def put_secret_value_for_testing(self, SecretId, Value):  # noQA - Argument names chosen for AWS consistency
        secrets = self._mocked_secrets()
        secrets[SecretId] = Value

    def get_secret_value(self, SecretId):  # noQA - Argument names must be compatible with AWS
        secrets = self._mocked_secrets()
        secret_value = secrets[SecretId]
        if isinstance(secret_value, dict):
            return {'SecretString': json_dumps(secret_value)}
        else:
            return {'SecretString': secret_value}

    def put_secret_key_value_for_testing(self, SecretId: str, SecretKey: str, SecretKeyValue: str):  # noQA - Argument names chosen for AWS consistency
        if SecretId and SecretKey:
            secrets = self._mocked_secrets()
            secret_value = secrets.get(SecretId)
            if not secret_value:
                secrets[SecretId] = {}
            secrets[SecretId][SecretKey] = SecretKeyValue

    def get_secret_key_value_for_testing(self, SecretId, SecretKey):  # noQA - Argument names must be compatible with AWS
        secrets = self._mocked_secrets()
        secret_value = secrets[SecretId]
        if isinstance(secret_value, dict):
            secret_value_json = secret_value
        else:
            secret_value_json = json_loads(secret_value)
        return secret_value_json[SecretKey]

    def list_secrets(self):
        secrets = self._mocked_secrets()
        # This really returns dictionaries with lots more things, but we'll start slow. :) -kmp 17-Feb-2022
        return {'SecretList': [{'Name': key} for key, _ in secrets.items()]}

    def update_secret(self, SecretId: str, SecretString: str) -> None:  # noQA - Argument names chosen for AWS consistency
        secrets = self._mocked_secrets()
        secrets[SecretId] = SecretString


@MockBoto3.register_client(kind='cloudformation')
class MockBotoCloudFormationClient:

    _SHARED_STACKS_MARKER = '_cloudformation_stacks'

    @property
    def mocked_stacks(self):
        return self.boto3.shared_reality.get(self._SHARED_STACKS_MARKER)

    @classmethod
    def setup_boto3_mocked_stacks(cls, boto3, mocked_stacks=None, mock_stack_names=None):

        shared_reality = boto3.shared_reality
        stacks = shared_reality.get(cls._SHARED_STACKS_MARKER)
        if stacks is None:
            # Export the list in case other clients want the same list.
            shared_reality[cls._SHARED_STACKS_MARKER] = stacks = []

        for mocked_stack in mocked_stacks or []:
            if mocked_stack not in stacks:
                stacks.append(mocked_stack)

        for mock_stack_name in mock_stack_names or []:
            for s in stacks:
                if s.name == mock_stack_name:
                    raise ValueError(f"duplicated mock stack name: {mock_stack_name}")
            stacks.append(MockBotoCloudFormationStack(mock_name=mock_stack_name))

    def __init__(self, *, mocked_stacks=None, mock_stack_names=None, boto3=None):
        self.boto3 = boto3 or MockBoto3()
        self.setup_boto3_mocked_stacks(self.boto3, mocked_stacks=mocked_stacks, mock_stack_names=mock_stack_names)
        self.stacks = MockStackCollectionManager(self)


class MockStackCollectionManager:

    def __init__(self, mock_cloudformation_client):
        self.mocked_cloudformation_client = mock_cloudformation_client

    def all(self):
        for mocked_stack in self.mocked_cloudformation_client.mocked_stacks:
            yield mocked_stack


class MockResourceSummaryCollectionManager:

    def __init__(self, mocked_stack):
        self.mocked_stack = mocked_stack

    def all(self):
        for mocked_resource_summary in self.mocked_stack.mocked_resource_summaries:
            yield mocked_resource_summary


class MockBotoCloudFormationStack:

    def __str__(self):
        return f"<{full_class_name(self)} {self.name} {id(self)}>"

    def __repr__(self):
        return str(self)

    def __init__(self, mock_name, mock_outputs=None, mock_resource_summaries=None,
                 mock_resource_summary_logical_ids=None):
        self.name = mock_name
        self.outputs = mock_outputs
        self.mocked_resource_summaries = mock_resource_summaries.copy() if mock_resource_summaries else []
        for resource_logical_id in mock_resource_summary_logical_ids or []:
            self.mocked_resource_summaries.append(
                MockBotoCloudFormationResourceSummary(logical_id=resource_logical_id))
        for r in self.mocked_resource_summaries:
            r.stack_name = self.name
        self.resource_summaries = MockResourceSummaryCollectionManager(self)


class MockBotoCloudFormationResourceSummary:

    def __init__(self, logical_id=None, physical_resource_id=None):
        self.logical_id = logical_id or f"MockedLogicalId{id(self)}"
        self.physical_resource_id = physical_resource_id or f"MockedPhysicalResourceId{id(self)}"
        self.stack_name = None  # This will get filled out if used as a resource on a mock stack


class MockTemporaryRestoration:

    def __init__(self, delay_seconds: Union[int, float], duration_days: Union[int, float],
                 storage_class: S3StorageClass):
        self._available_after = future_datetime(seconds=delay_seconds)
        self._available_until = future_datetime(now=self.available_after, days=duration_days)
        self._storage_class = storage_class

    @property
    def available_after(self):
        return self._available_after

    @property
    def available_until(self):
        return self._available_until

    @property
    def storage_class(self):
        return self._storage_class

    def is_expired(self, now=None):
        now = now or datetime.datetime.now()
        return now >= self.available_until

    def is_active(self, now=None):
        now = now or datetime.datetime.now()
        return now >= self.available_after and not self.is_expired(now=now)

    def hurry_restoration(self):
        self._available_after = datetime.datetime.now()

    def hurry_restoration_expiry(self):
        self._available_until = datetime.datetime.now()


class MockObjectBasicAttributeBlock:

    def __init__(self, filename, s3):
        self.last_modified = datetime.datetime.now()
        self.s3: MockBotoS3Client = s3
        self.filename: str = filename
        self.version_id: str = self._generate_version_id()

    def __str__(self):
        return f"<{self.filename}#{self.version_id}>"

    VERSION_ID_INITIAL_WIDTH = len(format_in_radix(time.time_ns(), radix=36))
    #  In a single instantiation of python, now 50+ years into the epoch, this mock could overflow one
    #  digit of this, but not realistically more. So reserving space for one digit of overflow after
    #  whatever time it is on load.
    VERSION_ID_MAX_WIDTH = VERSION_ID_INITIAL_WIDTH + 1

    MONTONIC_VERSIONS = False

    @classmethod
    def _generate_version_id(cls):
        if cls.MONTONIC_VERSIONS:
            return format_in_radix(time.time_ns(), radix=36)
        else:
            return str(uuid.uuid4()).replace('-', '.').lower()

    @property
    def storage_class(self) -> S3StorageClass:
        raise NotImplementedError(f"The method 'storage_class' is expected to be implemented"
                                  f" in subclasses of MockObjectBasicAttributeBlock: {self}")

    def initialize_storage_class(self, value: S3StorageClass):
        raise NotImplementedError(f"The method 'initialize_storage_class' is expected to be implemented"
                                  f" in subclasses of MockObjectBasicAttributeBlock: {self}")

    @property
    def tagset(self) -> List[Dict[Literal['Key', 'Value'], str]]:
        """
        Returns a list of Key/Value dictionaries: [{'Key': ..., 'Value':  ...}, ...]
        """
        raise NotImplementedError(f"The method 'tagset' is expected to be implemented"
                                  f" in subclasses of MockObjectBasicAttributeBlock: {self}")

    def set_tagset(self, value: List[Dict[Literal['Key', 'Value'], str]]):
        """
        Sets the tagset to a given list of Key/Value dictionaries: [{'Key': ..., 'Value':  ...}, ...]
        """
        raise NotImplementedError(f"The method 'set_tagset' is expected to be implemented"
                                  f" in subclasses of MockObjectBasicAttributeBlock: {self}")


class MockObjectDeleteMarker(MockObjectBasicAttributeBlock):

    @property
    def storage_class(self) -> S3StorageClass:
        raise Exception(f"Attempt to find storage class for mock-deleted S3 filename {self.filename}")

    def initialize_storage_class(self, value: S3StorageClass):
        raise Exception(f"Attempt to initialize storage class for mock-deleted S3 filename {self.filename}")

    @property
    def tagset(self) -> List[Dict[Literal['Key', 'Value'], str]]:
        raise Exception(f"Attempt to set tagset for mock-deleted S3 filename {self.filename}")

    def set_tagset(self, value: List[Dict[Literal['Key', 'Value'], str]]):
        raise Exception(f"Attempt to set tagset for mock-deleted S3 filename {self.filename}")


class MockObjectAttributeBlock(MockObjectBasicAttributeBlock):

    def __init__(self, filename, s3):
        super().__init__(filename=filename, s3=s3)
        self._storage_class: S3StorageClass = s3.storage_class
        self._tagset = []
        # Content must be added later. The file system at time this object is created may still have a stale value.
        self._content = None
        self._restoration: Optional[MockTemporaryRestoration] = None

    @property
    def content(self):
        return self._content

    def set_content(self, content):
        if self._content is None:
            self._content = content
        else:
            raise RuntimeError("Attempt to set content for an attribute block that already had content.")

    @property
    def tagset(self) -> List[Dict[Literal['Key', 'Value'], str]]:
        return copy.deepcopy(self._tagset)  # don't rely on caller to leave what we give them unmodified

    def set_tagset(self, value: List[Dict[Literal['Key', 'Value'], str]]):
        self._tagset = copy.deepcopy(value)  # don't rely on caller not to later modify the value given

    @property
    def storage_class(self):
        restoration = self.restoration
        return restoration.storage_class if restoration else self._storage_class

    def initialize_storage_class(self, value):
        if self.restoration:
            # It's ambiguous what is intended, but also this interface is really intended only for initialization
            # and should not be used in the middle of a mock operation. The storage class is not dynamically mutable.
            # You need to use the mock operations to make new versions with the right storage class after that.
            raise Exception("Tried to set storage class in an attribute block"
                            " while a temporary restoration is ongoing.")
        self._storage_class = value

    _RESTORATION_LOCK = threading.Lock()

    def hurry_restoration(self):
        with self._RESTORATION_LOCK:
            restoration = self.restoration
            if restoration:
                restoration.hurry_restoration()

    def hurry_restoration_expiry(self):
        with self._RESTORATION_LOCK:
            restoration = self.restoration
            if restoration:
                restoration.hurry_restoration_expiry()

    def storage_class_for_copy_source(self):
        with self._RESTORATION_LOCK:
            restoration = self.restoration
            if restoration:
                if not restoration.is_active():
                    raise Exception("Restoration request still in progress.")
                return restoration.storage_class
            else:
                return self._storage_class

    @property
    def restoration(self):
        restoration = self._restoration
        if restoration and restoration.is_expired():
            self._restoration = restoration = None
        return restoration

    def restore_temporarily(self, delay_seconds: Union[int, float], duration_days: Union[int, float],
                            storage_class: S3StorageClass):
        with self._RESTORATION_LOCK:
            if self.restoration:
                raise Exception("You are already have an active restoration and cannot re-restore it.")
            self._restoration = MockTemporaryRestoration(delay_seconds=delay_seconds,
                                                         duration_days=duration_days,
                                                         storage_class=storage_class)


@MockBoto3.register_client(kind='s3')
class MockBotoS3Client(MockBoto3Client):
    """
    This is a mock of certain S3 functionality.
    """

    MOCK_STATIC_FILES = {}
    MOCK_REQUIRED_ARGUMENTS = {}

    DEFAULT_STORAGE_CLASS: S3StorageClass = "STANDARD"

    def __init__(self, *,
                 region_name: str = None,
                 mock_other_required_arguments: Optional[List[str]] = None,
                 mock_s3_files: Dict[str, Union[str, bytes]] = None,
                 storage_class: Optional[S3StorageClass] = None,
                 boto3: Optional[MockBoto3] = None,
                 versioning_buckets: Union[Literal[True], list, set, tuple] = True):
        self.boto3 = boto3 = boto3 or MockBoto3()
        if region_name not in (None, 'us-east-1'):
            raise ValueError(f"Unexpected region: {region_name}")

        files_cache_marker = '_s3_file_data'
        shared_reality = boto3.shared_reality
        s3_files = shared_reality.get(files_cache_marker)
        if s3_files is None:
            files = self.MOCK_STATIC_FILES.copy()
            for name, content in (mock_s3_files or {}).items():
                files[name] = content
            shared_reality[files_cache_marker] = s3_files = MockAWSFileSystem(files=files, boto3=boto3, s3=self)
        self.s3_files = s3_files
        s3_files.add_bucket_versioning(versioning_buckets)

        other_required_arguments = self.MOCK_REQUIRED_ARGUMENTS.copy()
        for name, content in mock_other_required_arguments or {}:
            other_required_arguments[name] = content
        self.other_required_arguments = other_required_arguments
        self.storage_class: S3StorageClass = storage_class or self.DEFAULT_STORAGE_CLASS

    def check_for_kwargs_required_by_mock(self, operation, Bucket, Key, **kwargs):
        ignored(Bucket, Key)
        if kwargs != self.other_required_arguments:
            raise MockKeysNotImplemented(operation, self.other_required_arguments.keys())

    def create_object_for_testing(self, object_content: str, *, Bucket: str, Key: str):
        assert isinstance(object_content, str)
        self.upload_fileobj(Fileobj=io.BytesIO(object_content.encode('utf-8')), Bucket=Bucket, Key=Key)

    def upload_fileobj(self, Fileobj, Bucket, Key, **kwargs):  # noqa - Uppercase argument names are chosen by AWS
        self.check_for_kwargs_required_by_mock("upload_fileobj", Bucket=Bucket, Key=Key, **kwargs)
        data = Fileobj.read()
        PRINT(f"Uploading {Fileobj} ({len(data)} bytes) to bucket {Bucket} key {Key}")
        with self.s3_files.open(os.path.join(Bucket, Key), 'wb') as fp:
            fp.write(data)

    def upload_file(self, Filename, Bucket, Key, **kwargs):  # noqa - Uppercase argument names are chosen by AWS
        self.check_for_kwargs_required_by_mock("upload_file", Bucket=Bucket, Key=Key, **kwargs)
        with io.open(Filename, 'rb') as fp:
            self.upload_fileobj(Fileobj=fp, Bucket=Bucket, Key=Key)

    def download_fileobj(self, Bucket, Key, Fileobj, **kwargs):  # noqa - Uppercase argument names are chosen by AWS
        self.check_for_kwargs_required_by_mock("download_fileobj", Bucket=Bucket, Key=Key, **kwargs)
        with self.s3_files.open(os.path.join(Bucket, Key), 'rb') as fp:
            data = fp.read()
        PRINT(f"Downloading bucket {Bucket} key {Key} ({len(data)} bytes) to {Fileobj}")
        Fileobj.write(data)

    def download_file(self, Bucket, Key, Filename, **kwargs):  # noqa - Uppercase argument names are chosen by AWS
        self.check_for_kwargs_required_by_mock("download_file", Bucket=Bucket, Key=Key, **kwargs)
        with io.open(Filename, 'wb') as fp:
            self.download_fileobj(Bucket=Bucket, Key=Key, Fileobj=fp)

    def get_object(self, Bucket, Key, **kwargs):  # noqa - Uppercase argument names are chosen by AWS
        self.check_for_kwargs_required_by_mock("get_object", Bucket=Bucket, Key=Key, **kwargs)

        head_metadata = self.head_object(Bucket=Bucket, Key=Key, **kwargs)

        pseudo_filename = os.path.join(Bucket, Key)

        return dict(head_metadata,
                    Body=self.s3_files.open(pseudo_filename, 'rb'))

    PUT_OBJECT_CONTENT_TYPES = {
        "text/html": [".html"],
        "image/png": [".png"],
        "application/json": [".json"],
        "text/plain": [".txt", ".text"],
        "binary/octet-stream": [".fo"],
        # The below is because on Ubuntu 22.04 (docker), as opposed to 20.04, the mimetypes.guess_type
        # function returns this strange value for an argument ending in ".fo"; this manifested as a test
        # failure (put_object assert below) in test_s3_utils.test_unzip_s3_to_s3_unit. dmichaels/2023-09-28.
        "application/vnd.software602.filler.form+xml": [".fo"],
    }

    def put_object(self, *, Bucket, Key, Body, ContentType=None, **kwargs):  # noqa - Uppercase argument names are chosen by AWS
        # TODO: This is not mocking other args like ACL that we might use ACL='public-read' or ACL='private'
        #       grep for ACL= in tibanna, tibanna_ff, or dcicutils for examples of these values.
        # TODO: Shouldn't this be checking for required arguments (e.g., for SSE)? -kmp 9-May-2022
        if ContentType is not None:
            exts = self.PUT_OBJECT_CONTENT_TYPES.get(ContentType)
            assert exts, "Unimplemented mock .put_object content type %s for Key=%s" % (ContentType, Key)
            assert any(Key.endswith(ext) for ext in exts), (
                    "mock .put_object expects Key=%s to end in one of %s for ContentType=%s" % (Key, exts, ContentType))
        assert not kwargs, "put_object mock doesn't support %s." % kwargs
        if isinstance(Body, str):
            Body = Body.encode('utf-8')  # we just assume utf-8, which AWS seems to as well
        self.s3_files.files[Bucket + "/" + Key] = Body
        return {
            'ETag': self._content_etag(Body)
        }

    @staticmethod
    def _content_etag(content):
        # For reasons known only to AWS, the ETag, though described as an MD5 hash, begins and ends with
        # doublequotes, so an example from
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/list_object_versions.html
        # shows:  'ETag': '"6805f2cfc46c0f04559748bb039d69ae"',
        return f'"{hashlib.md5(content).hexdigest()}"'

    def Bucket(self, name):  # noQA - AWS function naming style
        return MockBotoS3Bucket(s3=self, name=name)

    def head_object(self, Bucket, Key, **kwargs):  # noQA - AWS argument naming style
        self.check_for_kwargs_required_by_mock("head_object", Bucket=Bucket, Key=Key, **kwargs)

        pseudo_filename = os.path.join(Bucket, Key)

        if self.s3_files.exists(pseudo_filename):
            content = self.s3_files.files[pseudo_filename]
            attribute_block = self._object_attribute_block(filename=pseudo_filename)
            assert isinstance(attribute_block, MockObjectAttributeBlock)  # if file exists, should be normal block
            result = {
                'Bucket': Bucket,
                'Key': Key,
                'ETag': self._content_etag(content),
                'ContentLength': len(content),
                'StorageClass': attribute_block.storage_class,  # self._object_storage_class(filename=pseudo_filename)
                # Numerous others, but this is enough to make the dictionary non-empty and to satisfy some of our tools
            }
            restoration = attribute_block.restoration
            if restoration:
                assert isinstance(restoration, MockTemporaryRestoration)
                if restoration.is_active():
                    result['Restore'] = f'ongoing-request="false", expiry-date="{restoration.available_until}"'
                else:
                    result['Restore'] = f'ongoing-request="true"'
            # if datetime.datetime.now() < attribute_block.available_after:
            #     result['Restore'] = 'ongoing-request="true"'
            return result
        else:
            # I would need to research what specific error is needed here and hwen,
            # since it might be a 404 (not found) or a 403 (permissions), depending on various details.
            # For now, just fail in any way since maybe our code doesn't care.
            raise Exception(f"Mock File Not Found: {pseudo_filename}."
                            f" Existing files: {list(self.s3_files.files.keys())}")

    def head_bucket(self, Bucket):  # noQA - AWS argument naming style
        bucket_prefix = Bucket + "/"
        for filename, content in self.s3_files.files.items():
            if filename.startswith(bucket_prefix):
                # Returns other things probably, but this will do to start for our mocking.
                return {"ResponseMetadata": {"HTTPStatusCode": 200}}
        raise ClientError(operation_name='HeadBucket',
                          error_response={  # noQA - PyCharm wrongly complains about this dictionary
                              "Error": {"Code": "404", "Message": "Not Found"},
                              "ResponseMetadata": self.compute_mock_response_metadata(http_status_code=404),
                          })

    def get_object_tagging(self, Bucket, Key):
        pseudo_file = f"{Bucket}/{Key}"
        return {
            'ResponseMetadata': {
                # Not presently mocked: RequestId, HostId
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    # Not presently mocked: x-amz-id-2, x-amz-request-id, date, transfer-encoding
                    'server': 'AmazonS3',
                },
                'RetryAttempts': 0,
            },
            'TagSet': self._object_tagset(pseudo_file)
        }

    def put_object_tagging(self, Bucket, Key, Tagging):
        pseudo_file = f"{Bucket}/{Key}"
        assert isinstance(Tagging, dict), "The Tagging argument must be a dictionary."
        assert list(Tagging.keys()) == ['TagSet'], "The Tagging argument dictionary should have only the 'TagSet' key."
        tagset = Tagging['TagSet']
        assert isinstance(tagset, list), "The Tagging argument's TagSet must be a list."
        for tag in tagset:
            assert set(tag.keys()) == {'Key', 'Value'}, "Each tag must be a dictionary of Key and Value."
            assert isinstance(tag['Key'], str), "Each tag's key must be a string."
            assert isinstance(tag['Value'], str), "Each tag's value must be a string."
        self._set_object_tagset(pseudo_file, Tagging['TagSet'])
        return {
            'ResponseMetadata': {
                # Not presently mocked: RequestId, HostId
                'HTTPStatusCode': 200,
                'HTTPHeaders': {
                    # Not presently mocked: x-amz-id-2, x-amz-request-id, date, transfer-encoding
                    'server': 'AmazonS3',
                },
                'RetryAttempts': 0,
            },
        }

    def _object_attribute_map(self):
        """
        Returns the storage class map for this mock.

        Note that this is a property of the boto3 instance (through its .shared_reality) not of the s3 mock itself
        so that if another client is created by that same boto3 mock, it will see the same storage classes.
        """
        object_attribute_map = self.boto3.shared_reality.get('object_attribute_map')
        if not object_attribute_map:
            self.boto3.shared_reality['object_attribute_map'] = object_attribute_map = {}
        return object_attribute_map

    def _object_attribute_blocks(self, filename):
        attribute_map = self._object_attribute_map()
        attribute_blocks = attribute_map.get(filename) or []
        if not attribute_blocks:
            attribute_map[filename] = attribute_blocks
        return attribute_blocks

    def _object_all_versions(self, filename):
        attribute_blocks = self._object_attribute_blocks(filename)
        if not attribute_blocks and self.s3_files.files.get(filename) is not None:
            # print(f"AUTOCREATING {filename}")
            # This will demand-create the first one because our mock initialization protocols are sloppy
            new_attribute_block = MockObjectAttributeBlock(filename=filename, s3=self)
            attribute_blocks.append(new_attribute_block)
        # else:
        #     print(f"NOT AUTOCREATING {filename} for {self} because {self.s3_files.files}")
        return attribute_blocks

    def _object_attribute_block(self, filename) -> MockObjectBasicAttributeBlock:
        """
        Returns the attribute_block for an S3 object.
        This contains information like storage class and tagsets.

        Because this is an internal routine, 'filename' is 'bucket/key' to match the mock file system we use internally.

        Note that this is a property of the boto3 instance (through its .shared_reality) not of the s3 mock itself
        so that if another client is created by that same boto3 mock, it will see the same storage classes.
        """
        all_versions: List[MockObjectBasicAttributeBlock] = self._object_all_versions(filename)
        if not all_versions:
            # This situation and usually we should not be calling this function at this point,
            # but try to help developer debug what's going on...
            if filename in self.s3_files.files:
                context = f"mock special non-file (bucket?) s3 item: {filename}"
            else:
                context = f"mock non-existent S3 file: {filename}"
            raise ValueError(f"Attempt to obtain object attribute block for {context}.")
        return all_versions[-1]

    _ARCHIVE_LOCK = threading.Lock()

    def hurry_restoration_for_testing(self, s3_filename, attribute_block=None):
        """
        This can be used in testing to hurry up the wait for a temporary restore to become available.
        """
        attribute_block = attribute_block or self._object_attribute_block(s3_filename)
        assert isinstance(attribute_block, MockObjectAttributeBlock)
        attribute_block.hurry_restoration()

    def hurry_restoration_expiry_for_testing(self, s3_filename, attribute_block=None):
        """
        This can be used in testing to hurry up the wait for a temporary restore to expire.
        """
        attribute_block = attribute_block or self._object_attribute_block(s3_filename)
        assert isinstance(attribute_block, MockObjectAttributeBlock)
        attribute_block.hurry_restoration_expiry()

    def archive_current_version(self, filename,
                                replacement_class: Type[MockObjectBasicAttributeBlock] = MockObjectAttributeBlock,
                                init: Optional[callable] = None) -> Optional[MockObjectBasicAttributeBlock]:
        with self._ARCHIVE_LOCK:
            # The file system dictionary is the source of content authority until we archive things,
            # and then the current version has to be archived.
            content = self.s3_files.files.get(filename)  # might be missing. you can delete a file that doesn't exist
            preexisting_version = None
            if content:
                preexisting_version = self._object_attribute_block(filename)
                assert isinstance(preexisting_version, MockObjectAttributeBlock)  # should be no content if deleted
                preexisting_version.set_content(content)
            new_block = self._prepare_new_attribute_block(filename, attribute_class=replacement_class)
            self._check_versions_registered(filename, preexisting_version, new_block)
            if init is not None:
                init()  # caller can supply an init function to be run while still inside lock
            return new_block

    def _check_versions_registered(self, filename, *versions: Optional[MockObjectBasicAttributeBlock]):
        """
        Performs a useful consistency check to identify problems early, but has no functional effect on other code
        other than to raise an error if there is a problem.

        :param filename: the S3 filename (in the form bucket/key)
        :param versions: the versions to check. each should be an attribute block, a delete marker, or None
        """
        all_versions = self._object_all_versions(filename)
        # Check that each of the given versions is present in all_versions
        for version in versions:
            if version:
                assert version in all_versions
        # We know that all versions are stored in time order. Make sure their simulated last-modified dates
        # are in time order to match.
        prior_date = None
        for version in all_versions:
            if version:
                if prior_date:
                    assert version.last_modified > prior_date
                prior_date = version.last_modified

    def _prepare_new_attribute_block(self, filename,
                                     attribute_class: Type[MockObjectBasicAttributeBlock] = MockObjectAttributeBlock
                                     ) -> MockObjectBasicAttributeBlock:  # given the type, instantiates that type
        new_block = attribute_class(filename=filename, s3=self)
        all_versions = self._object_all_versions(filename)
        all_versions.append(new_block)
        # NOTE WELL: This would seem information-destroying, but this method should only be called when you're
        #            about to write new data anyway, after having archived previous data, which means it should
        #            really only be called by archive_current_version after it has done any necessary saving away
        #            of a prior version (depending on whether versioning is enabled).
        self.s3_files.files[filename] = None
        return new_block

    def _object_tagset(self, filename):
        """
        Returns the tagset for the 'filename' in this S3 mock.
        Because this is an internal routine, 'filename' is 'bucket/key' to match the mock file system we use internally.

        Note that this is a property of the boto3 instance (through its .shared_reality) not of the s3 mock itself
        so that if another client is created by that same boto3 mock, it will see the same storage classes.
        """
        attribute_block = self._object_attribute_block(filename)
        return attribute_block.tagset

    def _set_object_tagset(self, filename, tagset):
        """
        Sets the tagset for the 'filename' in this S3 mock to the given value.
        Because this is an internal routine, 'filename' is 'bucket/key' to match the mock file system we use internally.

        Presently the value is not error-checked. That might change.
        By special exception, passing value=None will revert the storage class to the default for the given mock,
        for which the default default is 'STANDARD'.

        Note that this is a property of the boto3 instance (through its .shared_reality) not of the s3 mock itself
        so that if another client is created by that same boto3 mock, it will see the same storage classes.
        """
        attribute_block = self._object_attribute_block(filename)
        attribute_block.set_tagset(tagset)

    def _object_version_id(self, filename):
        """
        Returns the VersionId for the 'filename' in this S3 mock.
        Because this is an internal routine, 'filename' is 'bucket/key' to match the mock file system we use internally.

        Note that this is a property of the boto3 instance (through its .shared_reality) not of the s3 mock itself
        so that if another client is created by that same boto3 mock, it will see the same storage classes.
        """
        attribute_block = self._object_attribute_block(filename)
        return attribute_block.version_id

    def _object_last_modified(self, filename) -> datetime.datetime:

        attribute_block = self._object_attribute_block(filename)
        return attribute_block.last_modified

    def _object_storage_class(self, filename) -> S3StorageClass:
        """
        Returns the storage class for the 'filename' in this S3 mock.
        Because this is an internal routine, 'filename' is 'bucket/key' to match the mock file system we use internally.

        Note that this is a property of the boto3 instance (through its .shared_reality) not of the s3 mock itself
        so that if another client is created by that same boto3 mock, it will see the same storage classes.
        """
        attribute_block = self._object_attribute_block(filename)
        return attribute_block.storage_class

    def _set_object_storage_class_for_testing(self, s3_filename, value: S3StorageClass):
        """
        Sets the storage class for the 'filename' in this S3 mock to the given value.
        Because this is an internal routine, 'filename' is 'bucket/key' to match the mock file system we use internally.

        Presently the value is not error-checked. That might change.
        By special exception, passing value=None will revert the storage class to the default for the given mock,
        for which the default default is 'STANDARD'.

        Note that this is a property of the boto3 instance (through its .shared_reality) not of the s3 mock itself
        so that if another client is created by that same boto3 mock, it will see the same storage classes.
        """
        attribute_block = self._object_attribute_block(s3_filename)
        attribute_block.initialize_storage_class(value)

    def list_objects(self, Bucket, Prefix=None):  # noQA - AWS argument naming style
        # Ref: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.list_objects
        bucket_prefix = Bucket + "/"
        bucket_prefix_length = len(bucket_prefix)
        search_prefix = bucket_prefix + (Prefix or '')
        found = []
        for filename, content in self.s3_files.files.items():
            if filename.startswith(search_prefix):
                found.append({
                    'Key': filename[bucket_prefix_length:],
                    'ETag': self._content_etag(content),
                    'LastModified': self._object_last_modified(filename=filename),
                    # "Owner": {"DisplayName": ..., "ID"...},
                    "Size": len(content),
                    "StorageClass": self._object_storage_class(filename=filename),
                })
        return {
            # "CommonPrefixes": {"Prefix": ...},
            "Contents": found,
            # "ContinuationToken": ...,
            # "Delimiter": ...,
            # "EncodingType": ...,
            "KeyCount": len(found),
            "IsTruncated": False,
            # "MaxKeys": ...,
            # "NextContinuationToken": ...,
            "Name": Bucket,
            "Prefix": Prefix,
            # "StartAfter": ...,
        }

    def list_objects_v2(self, Bucket):  # noQA - AWS argument naming style
        # This is different but similar to list_objects. However, we don't really care about that.
        return self.list_objects(Bucket=Bucket)

    def copy_object(self, CopySource, Bucket, Key, CopySourceVersionId=None,
                    StorageClass: Optional[S3StorageClass] = None):
        return self._copy_object(CopySource=CopySource, Bucket=Bucket, Key=Key,
                                 CopySourceVersionId=CopySourceVersionId, StorageClass=StorageClass)

    def _copy_object(self, CopySource, Bucket, Key, CopySourceVersionId, StorageClass: Optional[S3StorageClass] = None,
                     allow_glacial=False):
        target_storage_class: S3StorageClass = StorageClass or self.storage_class
        source_bucket = CopySource['Bucket']
        source_key = CopySource['Key']
        source_version_id = CopySource.get('VersionId')  # Optional
        source_s3_filename = f"{source_bucket}/{source_key}"
        target_bucket = Bucket
        target_key = Key
        target_version_id = CopySourceVersionId
        target_s3_filename = f"{target_bucket}/{target_key}"
        copy_in_place = False  # might be overridden below
        if CopySourceVersionId:
            if CopySourceVersionId != source_version_id or source_bucket != Bucket or source_key != Key:
                raise AssertionError(f"This mock expected that if CopySourceVersionId is given,"
                                     f" a matching Bucket, Key, and VersionId appears in CopySource."
                                     f" CopySource={CopySource!r} Bucket={Bucket!r} Key={Key!r}"
                                     f" CopySourceVersionId={CopySourceVersionId!r}")
            copy_in_place = True
        source_data = self.s3_files.files.get(source_s3_filename)
        if source_version_id:
            source_version = self._get_versioned_object(source_s3_filename, source_version_id)
            source_data = source_data if source_version.content is None else source_version.content
        else:
            source_version = self._object_attribute_block(source_s3_filename)
        source_storage_class = source_version.storage_class_for_copy_source()
        if source_data is None:
            # Probably this will only happen for VersionId=None, but show the VersionId anyway,
            # but if there's a data inconsistency, we could get here for other reasons,
            # so show the version id just in case to help debugging. -kmp 21-Apr-2023
            raise Exception(f"S3 location Bucket={Bucket} Key={Key} VersionId{source_version_id} does not exist.")
        assert isinstance(source_version, MockObjectAttributeBlock)  # we know this because it has data
        if not allow_glacial:
            if GlacierUtils.is_glacier_storage_class(source_storage_class):
                raise Exception(f"The Copy source {CopySource!r} is in storage class {source_storage_class!r}"
                                f" and must be restored first.")
        if not copy_in_place:  # We don't archive previous version or update content for copy-in-place.
            self.archive_current_version(target_s3_filename)
            # In this case, we've made a new version and it will be current.
            # In that case, the files dictionary needs the content copied.
            self.s3_files.files[target_s3_filename] = source_data
        target_attribute_block = self._get_versioned_object(target_s3_filename, target_version_id)
        new_storage_class = target_storage_class
        if (copy_in_place
                and GlacierUtils.transition_involves_glacier_restoration(source_storage_class, target_storage_class)):
            new_storage_class = None  # For a restoration, don't update the glacier data. It's restored elsewhere.
            target_attribute_block.restore_temporarily(delay_seconds=self.RESTORATION_DELAY_SECONDS,
                                                       duration_days=1, storage_class=target_storage_class)
            PRINT(f"Set up restoration {target_attribute_block.restoration}")
        else:
            PRINT(f"The copy was not a temporary restoration.")
        if new_storage_class:
            target_attribute_block.initialize_storage_class(new_storage_class)
        return {'Success': True}

    RESTORATION_DELAY_SECONDS = 2

    def delete_object(self, Bucket, Key, VersionId, **unimplemented_keyargs):
        # Doc:
        #  * https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_object.html
        #  * https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html
        assert not unimplemented_keyargs, (f"The mock for delete_object needs to be extended."
                                           f" {there_are(unimplemented_keyargs, kind='unimplemented key')}")
        assert VersionId and isinstance(VersionId, str), "A VersionId must be supplied to delete_object."
        s3_filename = f"{Bucket}/{Key}"
        result = {
            'ResponseMetadata': self.compute_mock_response_metadata(),
            'VersionId': VersionId
        }
        if not VersionId:
            # Just like AWS, we don't actually verify that this file even exists in this case.
            # We allow you to delete a file that does not exist. We just add a delete marker
            # on the top of the existing versions.
            props = self._delete_current_version(s3_filename=s3_filename)
        else:
            # Just like in AWS, when a version id is given, that version is in-place deleted
            # with no new delete marker creates.
            props = self._delete_versioned_object(s3_filename=s3_filename, version_id=VersionId)
        for key, value in props.items():
            result[key] = value
        return result

    def _delete_current_version(self, s3_filename) -> Dict[str, Any]:
        """
        Deletes the current version of a versioned file, by piling a delete marker atop it.
        """
        delete_marker = self.archive_current_version(s3_filename, replacement_class=MockObjectDeleteMarker)
        if delete_marker:
            assert isinstance(delete_marker, MockObjectDeleteMarker)
            all_versions = self._object_all_versions(s3_filename)
            all_versions.append(delete_marker)
            return {'DeleteMarker': True}
        else:
            return {}

    def _get_versioned_object(self, s3_filename, version_id=None) -> MockObjectAttributeBlock:
        found = self._find_versioned_object(s3_filename, version_id)
        if not found:
            raise Exception(f"Mock S3 file {s3_filename!r} with version_id {version_id!r} not found.")
        if isinstance(found, MockObjectDeleteMarker):
            raise Exception(f"Mock S3 file {s3_filename!r} with version id {version_id!r} is a delete marker.")
        assert isinstance(found, MockObjectAttributeBlock)
        return found

    def _find_versioned_object(self, s3_filename, version_id=None) -> Optional[MockObjectBasicAttributeBlock]:
        all_versions = self._object_all_versions(s3_filename)
        if version_id is None:
            if all_versions:
                return all_versions[-1]
        else:
            for version in all_versions:
                if version.version_id == version_id:
                    return version
        return None

    def _delete_versioned_object(self, s3_filename, version_id) -> Dict[str, Any]:
        """
        Deletes the current version of a versioned file, by removing it in-place.
        This path never makes a new delete marker.
        """
        result = {}
        found_version = self._find_versioned_object(s3_filename, version_id)
        if not found_version:
            raise ClientError(operation_name="DeleteObject",
                              error_response={  # noQA - PyCharm wrongly complains about this dictionary
                                  "Error": {
                                      "Code": "InvalidArgument",
                                      "Message": "Invalid version id specified",
                                      "ArgumentName": "versionId",
                                      "ArgumentValue": version_id
                                  }})
        # Delete the old version
        all_versions = self._object_all_versions(s3_filename)
        all_versions.remove(found_version)
        if isinstance(found_version, MockObjectDeleteMarker):
            # 'DeleteMarker': True appears in a result if a delete marker is added or removed.
            # In this case, no new delete marker was added, but one was removed.
            result['DeleteMarker'] = True
        # Reset state for the version that shows through
        if all_versions:
            # If there are still versions remaining, update the content dictionary in self.s3_files.files
            new_content = None
            new_current_version: MockObjectBasicAttributeBlock = all_versions[-1]
            if isinstance(new_current_version, MockObjectAttributeBlock):
                new_content = new_current_version.content
            self.s3_files.files[s3_filename] = new_content
        else:
            # If there are no versions remaining, we've completely deleted the thing. Just remove all record.
            del self.s3_files.files[s3_filename]
        return result

    def restore_object(self, Bucket, Key, RestoreRequest, VersionId: Optional[str] = None,
                       StorageClass: Optional[S3StorageClass] = None):
        # TODO: VersionId is unused in the arglist. Is that OK? -kmp 19-Aug-2023
        duration_days: int = RestoreRequest.get('Days')
        storage_class: S3StorageClass = StorageClass or self.storage_class
        s3_filename = f"{Bucket}/{Key}"
        if not self.s3_files.exists(s3_filename):
            raise Exception(f"S3 file at Bucket={Bucket!r} Key={Key!r} does not exist,"
                            f" so cannot be restored from glacier.")
        attribute_block = self._object_attribute_block(s3_filename)
        assert isinstance(attribute_block, MockObjectAttributeBlock)  # since the file exists, this should be good
        attribute_block.restore_temporarily(delay_seconds=self.RESTORATION_DELAY_SECONDS,
                                            duration_days=duration_days,
                                            storage_class=storage_class)
        return {'Success': True}

    def list_object_versions(self, Bucket, Prefix='', **unimplemented_keyargs):  # noQA - AWS argument naming style
        assert not unimplemented_keyargs, (f"The mock for list_object_versions needs to be extended."
                                           f" {there_are(unimplemented_keyargs, kind='unimplemented key')}")
        version_descriptions = []
        delete_marker_descriptions = []
        bucket_prefix = Bucket + "/"
        bucket_prefix_length = len(bucket_prefix)
        search_prefix = bucket_prefix + (Prefix or '')
        aws_file_system = self.s3_files
        for filename, content in aws_file_system.files.items():
            key = filename[bucket_prefix_length:]
            if filename.startswith(search_prefix):
                all_versions = self._object_all_versions(filename)
                most_recent_version = all_versions[-1]
                for version in all_versions:
                    if isinstance(version, MockObjectAttributeBlock):
                        version_descriptions.append({
                            # 'Owner': {
                            #     "DisplayName": "4dn-dcic-technical",
                            #     "ID": "9e7e144b18724b65641286dfa355edb64c424035706bd1674e9096ee77422a45"
                            # },
                            'Key': key,
                            'VersionId': version.version_id,
                            'IsLatest': version == most_recent_version,
                            'ETag': self._content_etag(content),
                            'Size': len(content if version.content is None else version.content),
                            'StorageClass': version.storage_class,
                            'LastModified': version.last_modified,  # type datetime.datetime
                        })
                    else:
                        delete_marker_descriptions.append({
                            # 'Owner': {
                            #     "DisplayName": "4dn-dcic-technical",
                            #     "ID": "9e7e144b18724b65641286dfa355edb64c424035706bd1674e9096ee77422a45"
                            # },
                            'Key': key,
                            'VersionId': version.version_id,
                            'IsLatest': version == most_recent_version,
                            'LastModified': version.last_modified,  # type datetime.datetime
                        })
                # for other_versions in self.s3_files.other_files[filename]:
        return {
            'ResponseMetadata': self.compute_mock_response_metadata(),
            'IsTruncated': False,
            'Versions': version_descriptions,
            'DeleteMarkers': delete_marker_descriptions,
            'Name': Bucket,
            'Prefix': Prefix,
            # 'KeyMarker': "",
            # 'VersionIdMarker': "",
            # 'MaxKeys': 1000,
            # 'EncodingType': "url",
        }


class MockBotoS3Bucket:

    def __init__(self, name, s3=None):
        assert s3, "missing s3"
        self.s3 = s3
        self.name = name
        self.objects = MockBotoS3BucketObjects(bucket=self)

    def _delete(self, delete_bucket_too=False):
        prefix = self.name + "/"
        files = self.s3.s3_files.files
        to_delete = set()
        for pseudo_filename, _ in [files.items()]:
            if pseudo_filename.startswith(prefix):
                if pseudo_filename != prefix:
                    to_delete.add(pseudo_filename)
        for pseudo_filename in to_delete:
            del files[pseudo_filename]
        if not delete_bucket_too:
            files[prefix] = b''
        # TODO: Does anything need to be returned here?

    def _keys(self):
        found = False
        keys = set()  # In real S3, this would be cached info, but for testing we just create it on demand
        prefix = self.name + "/"
        for pseudo_filename, content in self.s3.s3_files.files.items():
            if pseudo_filename.startswith(prefix):
                found = True
                key = remove_prefix(prefix, pseudo_filename)
                if key != prefix:
                    keys.add(key)
        if not keys:
            if not found:
                # It's OK if we only found "<bucketname>/", which is an empty, but not missing bucket
                raise Exception("NoSuchBucket")
        return sorted(keys)

    def delete(self):
        self._delete(delete_bucket_too=True)

    def _all(self):
        """A callback for <bucket>.objects.all()"""
        return [MockBotoS3ObjectSummary(attributes=self.s3.head_object(Bucket=self.name, Key=key))
                for key in self._keys()]


class MockBotoS3ObjectSummary:
    # Not sure if we need to expose the strucutre of this yet. -kmp 13-Jan-2021
    def __init__(self, attributes):
        self._attributes = attributes

    @property
    def key(self):
        return self._attributes['Key']


class MockBotoS3BucketObjects:

    def __init__(self, bucket):
        self.bucket = bucket

    def all(self):
        return self.bucket._all()  # noQA - we are effectively a friend of this instance and are intended to call this.

    def delete(self):
        return self.bucket._delete(delete_bucket_too=False)  # noQA - we are effectively a friend of this instance


@MockBoto3.register_client(kind='sqs')
class MockBotoSQSClient(MockBoto3Client):
    """
    This is a mock of certain SQS functionality.
    """

    def __init__(self, *, region_name=None, boto3=None):
        if region_name not in (None, 'us-east-1'):
            raise ValueError(f"Unexpected region: {region_name}")
        self._mock_queue_name_seen = None
        self.boto3 = boto3 or MockBoto3()

    def check_mock_queue_url_consistency(self, queue_url):
        __tracebackhide__ = True
        if self._mock_queue_name_seen:
            assert self._mock_queue_name_seen in queue_url, "This mock only supports one queue at a time."

    MOCK_QUEUE_URL_PREFIX = 'https://sqs.us-east-1.amazonaws.com.mock/12345/'  # just something to look like a URL

    def compute_mock_queue_url(self, queue_name):
        return self.MOCK_QUEUE_URL_PREFIX + queue_name

    def get_queue_url(self, QueueName):  # noQA - AWS argument naming style
        self._mock_queue_name_seen = QueueName
        request_url = self.compute_mock_queue_url(QueueName)
        self.check_mock_queue_url_consistency(request_url)
        return {
            'QueueUrl': request_url,
            'ResponseMetadata': self.compute_mock_response_metadata()
        }

    MOCK_QUEUE_ATTRIBUTES_DEFAULT = 0

    def compute_mock_queue_attribute(self, queue_url, attribute_name):
        self.check_mock_queue_url_consistency(queue_url)
        ignored(attribute_name)  # This mock doesn't care which attribute, but you could subclass and override this
        return str(self.MOCK_QUEUE_ATTRIBUTES_DEFAULT)

    def compute_mock_queue_attributes(self, queue_url, attribute_names):
        self.check_mock_queue_url_consistency(queue_url)
        return {attribute_name: self.compute_mock_queue_attribute(queue_url, attribute_name)
                for attribute_name in attribute_names}

    def get_queue_attributes(self, QueueUrl, AttributeNames):  # noQA - AWS argument naming style
        self.check_mock_queue_url_consistency(QueueUrl)
        return {
            'Attributes': self.compute_mock_queue_attributes(QueueUrl, AttributeNames),
            'ResponseMetadata': self.compute_mock_response_metadata()
        }


def raises_regexp(error_class, pattern):
    """
    A context manager that works like pytest.raises but allows a required error message pattern to be specified as well.
    """
    # Mostly compatible with unittest style, so that (approximately):
    #  pytest.raises(error_class, match=exp) == unittest.TestCase.assertRaisesRegexp(error_class, exp)
    # They differ on what to do if an error_class other than the expected one is raised.
    return pytest.raises(error_class, match=pattern)


def check_duplicated_items_by_key(key, items, url=None, formatter=str):

    __tracebackhide__ = True

    search_res_by_keyval = {}
    for item in items:
        keyval = item[key]
        search_res_by_keyval[keyval] = entry = search_res_by_keyval.get(keyval, [])
        entry.append(item)
    duplicated_keyvals = {}
    for keyval, items in search_res_by_keyval.items():
        if len(items) > 1:
            duplicated_keyvals[keyval] = items
    prefix = ""
    if url is not None:
        prefix = "For %s: " % url
    assert not duplicated_keyvals, (
        '\n'.join([
            "%sDuplicated %s %s in %s." % (prefix, key, keyval, " and ".join(map(formatter, items)))
            for keyval, items in duplicated_keyvals.items()
        ])
    )


@contextlib.contextmanager
def known_bug_expected(jira_ticket=None, fixed=False, error_class=None):
    """
    A context manager for provisionally catching errors due to known bugs.

    For a bug that has a ticket filed against it, a use of the functionality in testing can be wrapped by
    an expression such as:

        with known_bug_expected(jira_ticket="TST-00001", error_class=RuntimeError):
            ... stuff that fails ...

    If the expected error does not occur, an error will result so that it's easy to notice that it may have changed
    or been fixed.

    Later, when the bug is fixed, just add fixed=True, as in:

        with known_bug_expected(jira_ticket="TST-00001", error_class=RuntimeError, fixed=True):
            ... stuff that fails ...

    If the previously-expected error (now thought to be fixed) happens, an error will result
    so that it's easy to tell if there's been a regression.

    Parameters:

        jira_ticket:  a string identifying the bug, or None. There is no syntax checking or validation.
        fixed: a boolean that says whether the bug is expected to be fixed.
        error_class: the class of error that would result if the bug were not fixed.

    Returns:

        N/A - This is intended for use as a context manager.
    """
    error_class = error_class or Exception
    if fixed is False:
        try:
            yield
        except error_class:
            # The expected error was seen, so nothing to do.
            pass
        except Exception as e:
            raise WrongErrorSeen(jira_ticket=jira_ticket, expected_class=error_class, error_seen=e)
        else:
            raise ExpectedErrorNotSeen(jira_ticket=jira_ticket)
    else:
        try:
            yield
        except error_class as e:
            # Once fixed, we should complain if it recurs.
            raise UnexpectedErrorAfterFix(jira_ticket=jira_ticket, expected_class=error_class, error_seen=e)
        except Exception as e:
            raise WrongErrorSeenAfterFix(jira_ticket=jira_ticket, expected_class=error_class, error_seen=e)
        else:
            # If no error occurs, that's probably the fix in play.
            pass


def client_failer(operation_name, code=400):
    def fail(message, code=code):
        raise ClientError(
            {  # noQA - PyCharm wrongly complains about this dictionary
                "Error": {"Message": message, "Code": code}  # noQA - Boto3 declares a string here but allows int code
            },
            operation_name=operation_name)
    return fail


@MockBoto3.register_client(kind='elasticbeanstalk')
class MockBotoElasticBeanstalkClient:

    DEFAULT_MOCKED_BEANSTALKS = []
    DEFAULT_MOCKED_CONFIGURATION_SETTINGS = []

    def _default_mocked_beanstalks(self, mocked_beanstalks):
        return mocked_beanstalks or self.DEFAULT_MOCKED_BEANSTALKS

    def _default_mocked_configuration_settings(self, mocked_configuration_settings):
        return mocked_configuration_settings or self.DEFAULT_MOCKED_CONFIGURATION_SETTINGS

    def __init__(self, mocked_beanstalks=None, mocked_configuration_settings=None, boto3=None, region_name=None):
        self.boto3 = boto3 or MockBoto3()
        self.region_name = region_name
        self.mocked_beanstalks = self._default_mocked_beanstalks(mocked_beanstalks)
        self.mocked_configuration_settings = self._default_mocked_configuration_settings(mocked_configuration_settings)

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/elasticbeanstalk.html#ElasticBeanstalk.Client.describe_environments  # noQA
    def describe_environments(self, ApplicationName=None, EnvironmentNames=None):  # noQA - AWS picks these names
        criteria = {}
        if ApplicationName:
            criteria['ApplicationName'] = ApplicationName
        if EnvironmentNames:
            criteria['EnvironmentName'] = lambda name: name in EnvironmentNames
        result = find_associations(self.mocked_beanstalks, **criteria)
        return {
            "Environments": copy.deepcopy(result),
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            },
        }

    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/elasticbeanstalk.html#ElasticBeanstalk.Client.describe_configuration_settings  # noQA
    def describe_configuration_settings(self, ApplicationName=None, EnvironmentName=None):  # noQA - AWS picks these names
        criteria = {}
        if ApplicationName:
            criteria['ApplicationName'] = ApplicationName
        if EnvironmentName:
            criteria['EnvironmentName'] = EnvironmentName
        result = find_associations(self.mocked_configuration_settings, **criteria)
        return {
            "ConfigurationSettings": copy.deepcopy(result),
            "ResponseMetadata": {
                "HTTPStatusCode": 200,
            },
        }

    def swap_environment_cnames(self, SourceEnvironmentName, DestinationEnvironmentName):  # noQA - AWS picks these names
        fail = client_failer("SwapEnvironmentCNAMEs", code=404)
        source_beanstalk = find_associations(self.mocked_beanstalks, EnvironmentName=SourceEnvironmentName)
        if not source_beanstalk:
            fail(f"SourceEnvironmentName {SourceEnvironmentName} not found.")
        destination_beanstalk = find_associations(self.mocked_beanstalks, EnvironmentName=DestinationEnvironmentName)
        if not destination_beanstalk:
            fail(f"DestinationEnvironmentName {DestinationEnvironmentName} not found.")
        old_source_cname = source_beanstalk['CNAME']
        source_beanstalk['CNAME'] = destination_beanstalk['CNAME']
        destination_beanstalk['CNAME'] = old_source_cname

    def restart_app_server(self, EnvironmentName):  # noQA - AWS picks these names
        fail = client_failer("RestartAppServer", code=404)
        beanstalk = find_associations(self.mocked_beanstalks, EnvironmentName=EnvironmentName)
        if not beanstalk:
            fail(f"EnvironmentName {EnvironmentName} not found.")
        beanstalk['_restarted_count'] = beanstalk.get('_restarted_count', 0) + 1  # a way to tell it happened.

    def create_environment(self, ApplicationName, EnvironmentName, TemplateName, OptionSettings):  # noQA - AWS picks these names
        raise NotImplementedError("create_environment")

    def update_environment(self, EnvironmentName, TemplateName=None, OptionSettings=None):  # noQA - AWS picks these names
        raise NotImplementedError("update_environment")


def make_mock_beanstalk_cname(env_name):
    # return f"{env_name}.9wzadzju3p.us-east-1.elasticbeanstalk.com"
    return f"{short_env_name(env_name)}.4dnucleome.org"


def make_mock_beanstalk(env_name, cname=None):
    return {
        "EnvironmentName": env_name,
        "ApplicationName": "4dn-web",
        "CNAME": cname or make_mock_beanstalk_cname(env_name),
    }


_NAMESPACE_AUTOSCALING_LAUNCHCONFIG = "aws:autoscaling:launchconfiguration"
_NAMESPACE_CLOUDFORMATION_PARAMETER = "aws:cloudformation:template:parameter"
_NAMESPACE_ENVIRONMENT_VARIABLE = "aws:elasticbeanstalk:application:environment"


def make_mock_beanstalk_environment_variables(var_str):
    # An extra option just as a decoy
    spec0 = [{"Namespace": _NAMESPACE_AUTOSCALING_LAUNCHCONFIG, "OptionName": "InstanceType", "Value": "c5.xlarge"}]
    # The string form of the environment variables, as given to CloudFormation
    spec1 = [{"Namespace": _NAMESPACE_CLOUDFORMATION_PARAMETER, "OptionName": "EnvironmentVariables", "Value": var_str}]
    # The individually parsed form of the environment variables, like beanstalks use.
    spec2 = [{"Namespace": _NAMESPACE_ENVIRONMENT_VARIABLE, "OptionName": name, "Value": value}
             for name, value in [spec.split("=") for spec in var_str.split(",")]]
    return spec0 + spec1 + spec2


class MockedCommandArgs:

    VALID_ARGS = []

    def __init__(self, **args):
        for arg in self.VALID_ARGS:
            setattr(self, arg, None)
        for arg, v in args.items():
            assert arg in self.VALID_ARGS
            setattr(self, arg, v)


@contextlib.contextmanager
def input_mocked(*inputs, module):  # module is a required keyword arg (only a single module is supported)
    input_stack = list(reversed(inputs))

    def mocked_input(*args, **kwargs):
        ignored(args, kwargs)
        assert input_stack, "There are not enough mock inputs."
        return input_stack.pop()

    with mock.patch.object(module, "input") as mock_input:
        mock_input.side_effect = mocked_input
        yield mock_input
        if input_stack:
            # e.g., "There is 1 unused input." or "There are 2 unused inputs."
            raise AssertionError(there_are(kind="unused mock input", items=input_stack, punctuate=True, show=False))


class MockLog:

    def __init__(self, *, messages=None, key=None, allow_warn=False):
        self.messages = messages or {}
        self.key = key
        self._all_messages = []
        self.allow_warn = allow_warn

    def _addmsg(self, kind, msg):
        self.messages[kind] = msgs = self.messages.get(kind, [])
        msgs.append(msg[self.key] if self.key else msg)
        item = f"{kind.upper()}: {msg}"
        self._all_messages.append(item)

    def debug(self, msg):
        self._addmsg('debug', msg)

    def info(self, msg):
        self._addmsg('info', msg)

    def warning(self, msg):
        self._addmsg('warning', msg)

    def warn(self, msg):
        if self.allow_warn:
            self._addmsg('warning', msg)
        else:
            raise AssertionError(f"{self}.warn called. Should be 'warning'")

    def error(self, msg):
        self._addmsg('error', msg)

    def critical(self, msg):
        self._addmsg('critical', msg)

    @property
    def all_log_messages(self):
        return self._all_messages


@contextlib.contextmanager
def logged_messages(*args, module,  # module is a required keyword arg (only a single module is supported)
                    log_class=MockLog, logvar="log", allow_warn=False, **kwargs):
    mocked_log = log_class(allow_warn=allow_warn)
    with mock.patch.object(module, logvar, mocked_log):
        yield mocked_log
        if args or not kwargs:
            expected = list(args)
            assert mocked_log.all_log_messages == expected, (
                f"Expected {logvar}.all_log_messages = {expected}, but got {mocked_log.all_log_messages}"
            )
        if kwargs:
            expected = dict(**kwargs)
            assert mocked_log.messages == expected, (
                f"Expected {logvar}.all_log_messages = {expected}, but got {mocked_log.messages}"
            )


class MockId:

    COUNTER_BASE = 0x1000

    def __init__(self, counter_base: Optional[int] = None):
        self.counter_base = self.COUNTER_BASE if counter_base is None else counter_base
        self.seen = {}

    def __call__(self, object):
        if object in self.seen:
            return self.seen[object]
        else:
            n = self.counter_base + len(self.seen)
            self.seen[object] = n
            return n


class Eventually:

    """
    Intended use requires you to define a tesre for the assertions that might have to be tried more than once
    until they eventually succeed after some number of tries:

        def my_assertions():
            assert flakey_foo() == 17

    and later call it with the expected error_class and/or error_message, and information about how many times
    to try and after what kind of wait:

        Eventually.call_assertion(my_assertions, tries=3, wait_seconds=2, error_class=SomeException)

    """

    @classmethod
    def call_assertion(cls, assertion_function, error_message=None, *,
                       error_class=AssertionError, threshold_seconds=None, tries=None, wait_seconds=None):

        """
        Calls an assertion function some number of times before giving up, waiting between calls.

        :param assertion_function: The function that does the assertions, erring if they fail.
        :param error_class: The expected error if the tests fail in the expected way (before hopefully succeeding).
            If some other error occurs, a retry will not occur.
        :param error_message: The expected error messsage (a regular expression) if the tests fail in the expected way.
            If some other error message occurs (the pattern does not match), a retry will not occur.
        :param threshold_seconds: (deprecated) the number of seconds to keep trying.
            Please use tries or wait_seconds instead.
        :param tries: the numbe rof times to try (one more time than the number of retries)
        :param wait_seconds: how long (in integer or float seconds) between tries
        """

        if threshold_seconds is not None:
            assert isinstance(threshold_seconds, int), "The threshold_seconds must be an integer."
            assert tries is None and wait_seconds is None, (
                "The threshold_seconds= argument may not be mixed with tries= or wait_seconds=.")
            tries = threshold_seconds

        tries = tries or 10
        wait_seconds = wait_seconds or 1

        # Try once a second
        errors = []
        for _ in range(tries):
            try:
                return assertion_function()
            except error_class as e:
                msg = get_error_message(e)
                if error_message and not re.search(error_message, msg):
                    raise
                PRINT(msg)
                errors.append(msg)
            time.sleep(wait_seconds)
        else:
            raise AssertionError(f"Eventual consistency not achieved after {threshold_seconds} seconds.")

    @classmethod
    def consistent(cls, error_message=None, error_class=AssertionError, tries=None, wait_seconds=None):
        """
        A decorator for a function of zero arguments that does assertions.
        In fact, the decorated function, once defined, CAN be called with arguments, but those are the
        additional keyword arguments to Eventually.call_assertion and will not be passed to the decorated function.

        This decoration can also have arguments, which establish defaults for the decorated function.

        For example:

            @Eventually.consistent(tries=5)
            def foo():
                assert something()

        followed later by

            foo(wait_seconds=2)

        will call the oringally-defined no-argument function foo like this:

            Eventually.call_assertion(foo, tries=5, wait_seconds=2)
        """

        def _wrapper(fn):

            default_error_message = error_message
            default_error_class = error_class
            default_tries = tries
            default_wait_seconds = wait_seconds

            @functools.wraps(fn)
            def _wrapped(error_message=None, error_class=None, tries=None, wait_seconds=None):
                return cls.call_assertion(fn,
                                          error_message=error_message or default_error_message,
                                          error_class=error_class or default_error_class,
                                          tries=tries or default_tries,
                                          wait_seconds=wait_seconds or default_wait_seconds)

            return _wrapped

        return _wrapper


class Timer:

    def __init__(self):
        self.start = None
        self.end = None

    def start_timer(self):
        # Resets it even if it was already set.
        self.end = None
        self.start = datetime.datetime.now()

    def stop_timer(self):
        # Does not move the time if already stopped. Safe to call twice.
        if self.end is None:
            self.end = datetime.datetime.now()

    def __enter__(self):
        self.start_timer()
        return self

    def __exit__(self, *exc):
        ignored(exc)
        self.stop_timer()
        return False

    def duration_seconds(self):
        if self.start is None:
            return None
        end = datetime.datetime.now() if self.end is None else self.end
        return (end - self.start).total_seconds()


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
    with mock.patch.object(misc_utils_module, 'input') as mock_input:

        def mocked_input(*args, **kwargs):
            ignored(kwargs)
            (arg,) = args
            if not inputs:
                raise OutOfInputs()
            result = inputs.pop()
            builtin_print(arg)
            builtin_print(result)
            return result

        mock_input.side_effect = mocked_input

        inputs = []

        for item in reversed(items):
            inputs.append(item)
        yield
        assert not inputs, "Did not use all inputs."


def is_subdict(json1, json2, desc1="json1", desc2="json2", verbose=True):
    """
    Does asymmetric testing of dictionary equivalence, assuring json2 has all the content of json1,
    even if not vice versa. In other words, the dictionary structure is equivalent, to the extent
    that (recursively) all dictionary keys on the left hand side also occur on the right hand side
    even if not necessarily all dictionary keys on the right occur in the left.

    For example,
       x = {"foo": 3}
       y = {"foo": 3, "bar": 4}
       is_subdict(x, y) is True
       is_subdict(y, x) is False

    The desc1 and desc2 can be provided to help with verbose mode, identifying what is on the left
    and what is on the right.

    :param json1: a JSON structure, the outer part of which is a dictionary
    :param json2: a JSON structure, the outer part of which is a dictionary
    :param desc1: a name or brief description for the json1 (default "json1")
    :param desc2: a name or brief description for the json2 (default "json2")
    :param verbose: a boolean (default True) that controls whether the comparison is verbose, showing
        output explaining failures or near-failures when True, and otherwise, if False, not showing such output.
    """

    def out(x):
        if verbose:
            PRINT(x)

    def sorted_set_repr(x):
        return f"{{{repr(sorted(x))[1:-1]}}}"

    def recurse(json1, json2, path=""):
        if isinstance(json1, dict) and isinstance(json2, dict):
            k1 = set(json1.keys())
            k2 = set(json2.keys())
            result = k1 <= k2
            if result:
                if k1 != k2:
                    out(f"Non-fatal keyword mismatch at {path!r}:")
                    out(f" {desc1} keys: {sorted_set_repr(k1)}")
                    out(f" {desc2} keys: {sorted_set_repr(k2)}")
                result = all(recurse(value, json2[key], path=f"{path}.{key}")
                             for key, value in json1.items())
                if not result:
                    # out(f"Recursive failure at {path!r} in object comparison")
                    pass
            else:
                out(f"Failed at {path!r} in object comparison due to key set mismatch:")
                out(f" {desc1} keys: {sorted_set_repr(k1)}")
                out(f" {desc2} keys: {sorted_set_repr(k2)}")
        elif isinstance(json1, list) and isinstance(json2, list):
            len1 = len(json1)
            len2 = len(json2)
            result = len1 == len2
            if not result:
                out(f"Failed at {path!r} in list comparison due to length mismatch: {len1} vs {len2}")
            else:
                result = all(recurse(json1[i], json2[i], path=f"{path}[{i}]") for i in range(len1))
                if not result:
                    # out(f"Recursive failure at {path!r} in list comparison")
                    pass
        elif type(json1) is type(json2):
            result = json1 == json2
            if not result:
                out(f"Failed at {path!r} due to value mismatch: {json.dumps(json1)} != {json.dumps(json2)}")
        else:
            result = False
            if not result:
                out(f"Type mismatch ({json1.__class__.__name__} vs {json2.__class__.__name__}) at {path!r}:")
                out(f" {desc1}: {json1}")
                out(f" {desc2}: {json2}")
        return result
    return recurse(json1, json2)
