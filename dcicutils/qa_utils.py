"""
qa_utils: Tools for use in quality assurance testing.
"""

import contextlib
import datetime
import dateutil.tz as dateutil_tz
import hashlib
import io
import os
import pytest
import pytz
import re
import time
import toml
import uuid
import warnings

from dcicutils.misc_utils import environ_bool
from json import dumps as json_dumps, loads as json_loads
from unittest import mock
from .misc_utils import PRINT, ignored, Retry, CustomizableProperty, getattr_customized, remove_prefix, REF_TZ


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
    with override_dict(os.environ, **overrides):
        yield


@contextlib.contextmanager
def override_dict(d, **overrides):
    to_delete = []
    to_restore = {}
    try:
        for k, v in overrides.items():
            if k in d:
                to_restore[k] = d[k]
            else:
                to_delete.append(k)
            if v is None:
                d.pop(k, None)  # Delete key k, tolerating it being already gone
            else:
                d[k] = v
        yield
    finally:
        for k in to_delete:
            d.pop(k, None)  # Delete key k, tolerating it being already gone
        for k, v in to_restore.items():
            d[k] = v


LOCAL_TIMEZONE_MAPPINGS = {
    ('EST', 'EDT'): "US/Eastern",
    ('CST', 'CDT'): "US/Central",
    ('MST', 'MDT'): "US/Mountain",
    ('PST', 'PDT'): "US/Pacific",
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
        if FILE_SYSTEM_VERBOSE:
            print("Writing %r to %s." % (content, self.file))
        self.file_system.files[self.file] = content if isinstance(content, bytes) else content.encode(self.encoding)


class MockFileSystem:
    """Extremely low-tech mock file system."""

    def __init__(self, files=None, default_encoding='utf-8', auto_mirror_files_for_read=False, do_not_auto_mirror=()):
        self.default_encoding = default_encoding
        # Setting this dynamically will make things inconsistent
        self._auto_mirror_files_for_read = auto_mirror_files_for_read
        self._do_not_auto_mirror = set(do_not_auto_mirror or [])
        self.files = {filename: content.encode(default_encoding) for filename, content in (files or {}).items()}
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

    def exists(self, file):
        self._maybe_auto_mirror_file(file)
        return bool(self.files.get(file))

    def remove(self, file):
        self._maybe_auto_mirror_file(file)
        if not self.files.pop(file, None):
            raise FileNotFoundError("No such file or directory: %s" % file)

    def open(self, file, mode='r', encoding=None):
        if FILE_SYSTEM_VERBOSE:
            print("Opening %r in mode %r." % (file, mode))
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
        if FILE_SYSTEM_VERBOSE:
            print("Read %r from %s." % (content, file))
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
        self.lines = []
        self.last = None

    def mock_print_handler(self, *args, **kwargs):
        text = " ".join(map(str, args))
        print(text, **kwargs)
        # This only captures non-file output output.
        if kwargs.get('file') is None:
            self.last = text
            self.lines.append(text)


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
    with local_attrs(PRINT, _printer=printed.mock_print_handler):
        yield printed


class MockKeysNotImplemented(NotImplementedError):

    def __init__(self, operation, keys):
        self.operation = operation
        self.keys = keys
        super().__init__("Mocked %s does not implement keywords: %s" % (operation, ", ".join(keys)))


class MockBoto3:

    @classmethod
    def _default_mappings(cls):
        return {
            's3': MockBotoS3Client,
            'sqs': MockBotoSQSClient,
        }

    def __init__(self, **override_mappings):
        self._mappings = dict(self._default_mappings(), **override_mappings)
        self.shared_reality = {}

    def resource(self, key, **kwargs):
        return self._mappings[key](boto3=self, **kwargs)

    def client(self, kind, **kwargs):
        mapped_class = self._mappings.get(kind)
        if not mapped_class:
            raise NotImplementedError("Unsupported boto3 mock kind:", kind)
        return mapped_class(boto3=self, **kwargs)


class MockBotoS3Client:
    """
    This is a mock of certain S3 functionality.
    """

    MOCK_STATIC_FILES = {}
    MOCK_REQUIRED_ARGUMENTS = {}

    DEFAULT_STORAGE_CLASS = "STANDARD"

    def __init__(self, region_name=None, mock_other_required_arguments=None, mock_s3_files=None,
                 storage_class=None, boto3=None):
        self.boto3 = boto3 or MockBoto3()
        if region_name not in (None, 'us-east-1'):
            raise ValueError("Unexpected region:", region_name)

        files_cache_marker = '_s3_file_data'
        shared_reality = self.boto3.shared_reality
        s3_files = shared_reality.get(files_cache_marker)
        if s3_files is None:
            files = self.MOCK_STATIC_FILES.copy()
            for name, content in mock_s3_files or {}:
                files[name] = content
            shared_reality[files_cache_marker] = s3_files = MockFileSystem(files=files)
        self.s3_files = s3_files

        other_required_arguments = self.MOCK_REQUIRED_ARGUMENTS.copy()
        for name, content in mock_other_required_arguments or {}:
            other_required_arguments[name] = content
        self.other_required_arguments = other_required_arguments
        self.storage_class = storage_class or self.DEFAULT_STORAGE_CLASS


    def upload_fileobj(self, Fileobj, Bucket, Key, **kwargs):  # noqa - Uppercase argument names are chosen by AWS
        if kwargs != self.other_required_arguments:
            raise MockKeysNotImplemented("upload_file_obj", kwargs.keys())

        data = Fileobj.read()
        print("Uploading %s (%s bytes) to bucket %s key %s"
              % (Fileobj, len(data), Bucket, Key))
        with self.s3_files.open(os.path.join(Bucket, Key), 'wb') as fp:
            fp.write(data)

    def upload_file(self, Filename, Bucket, Key, **kwargs):  # noqa - Uppercase argument names are chosen by AWS
        if kwargs != self.other_required_arguments:
            raise MockKeysNotImplemented("upload_file", kwargs.keys())

        with io.open(Filename, 'rb') as fp:
            self.upload_fileobj(Fileobj=fp, Bucket=Bucket, Key=Key)

    def download_fileobj(self, Bucket, Key, Fileobj, **kwargs):  # noqa - Uppercase argument names are chosen by AWS
        if kwargs != self.other_required_arguments:
            raise MockKeysNotImplemented("download_fileobj", kwargs.keys())

        with self.s3_files.open(os.path.join(Bucket, Key), 'rb') as fp:
            data = fp.read()
        print("Downloading bucket %s key %s (%s bytes) to %s"
              % (Bucket, Key, len(data), Fileobj))
        Fileobj.write(data)

    def download_file(self, Bucket, Key, Filename, **kwargs):  # noqa - Uppercase argument names are chosen by AWS
        if kwargs != self.other_required_arguments:
            raise MockKeysNotImplemented("download_file", kwargs.keys())

        with io.open(Filename, 'wb') as fp:
            self.download_fileobj(Bucket=Bucket, Key=Key, Fileobj=fp)

    def get_object(self, Bucket, Key, **kwargs):  # noqa - Uppercase argument names are chosen by AWS
        if kwargs != self.other_required_arguments:
            raise MockKeysNotImplemented("get_object", kwargs.keys())

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
    }

    def put_object(self, *, Bucket, Key, Body, ContentType=None, **kwargs):  # noqa - Uppercase argument names are chosen by AWS
        if ContentType is not None:
            exts = self.PUT_OBJECT_CONTENT_TYPES.get(ContentType)
            assert exts, "Unimplemented mock .put_object content type %s for Key=%s" % (ContentType, Key)
            assert any(Key.endswith(ext) for ext in exts), (
                    "mock .put_object expects Key=%s to end in one of %s for ContentType=%s" % (Key, exts, ContentType))
        assert not kwargs, "put_object mock doesn't support %s." % kwargs
        self.s3_files.files[Bucket + "/" + Key] = Body
        return {
            'ETag': self._content_etag(Body)
        }

    @staticmethod
    def _content_etag(content):
        return hashlib.md5(content).hexdigest()

    def Bucket(self, name):  # noQA - AWS function naming style
        return MockBotoS3Bucket(s3=self, name=name)

    def head_object(self, Bucket, Key, **kwargs):  # noQA - AWS argument naming style
        if kwargs != self.other_required_arguments:
            raise MockKeysNotImplemented("get_object", kwargs.keys())

        pseudo_filename = os.path.join(Bucket, Key)

        if self.s3_files.exists(pseudo_filename):
            content = self.s3_files.files[pseudo_filename]
            return {
                'Bucket': Bucket,
                'Key': Key,
                'ETag': self._content_etag(content),
                'ContentLength': len(content),
                # Numerous others, but this is enough to make the dictionary non-empty and to satisfy some of our tools
            }
        else:
            # I would need to research what specific error is needed here and hwen,
            # since it might be a 404 (not found) or a 403 (permissions), depending on various details.
            # For now, just fail in any way since maybe our code doesn't care.
            raise Exception("Mock File Not Found")

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
                    # "LastModified": ...,
                    # "Owner": {"DisplayName": ..., "ID"...},
                    "Size": len(content),
                    "StorageClass": self.storage_class,
                })
        return {
            # "CommonPrefixes": {"Prefix": ...},
            "Contents": found,
            # "ContinuationToken": ...,
            # "Delimiter": ...,
            # "EncodingType": ...,
            # "KeyCount": ...,
            "IsTruncated": False,
            # "MaxKeys": ...,
            # "NextContinuationToken": ...,
            "Name": Bucket,
            "Prefix": Prefix,
            # "StartAfter": ...,
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
        return [self.s3.head_object(Bucket=self.name, Key=key)
                for key in self._keys()]


class MockBotoS3ObjectSummary:
    # Not sure if we need to expose the strucutre of this yet. -kmp 13-Jan-2021
    def __init__(self, **attributes):
        self._attributes = attributes


class MockBotoS3BucketObjects:

    def __init__(self, bucket):
        self.bucket = bucket

    def all(self):
        return self.bucket._all()  # noQA - we are effectively a friend of this instance and are intended to call this.

    def delete(self):
        return self.bucket._delete(delete_bucket_too=False)  # noQA - we are effectively a friend of this instance


class MockBotoSQSClient:
    """
    This is a mock of certain SQS functionality.
    """

    def __init__(self, region_name=None, boto3=None):
        if region_name not in (None, 'us-east-1'):
            raise RuntimeError("Unexpected region:", region_name)
        self._mock_queue_name_seen = None
        self.boto3 = boto3 or MockBoto3()

    def check_mock_queue_url_consistency(self, queue_url):
        __tracebackhide__ = True
        if self._mock_queue_name_seen:
            assert self._mock_queue_name_seen in queue_url, "This mock only supports one queue at a time."

    MOCK_CONTENT_TYPE = 'text/xml'
    MOCK_CONTENT_LENGTH = 350
    MOCK_RETRY_ATTEMPTS = 0
    MOCK_STATUS_CODE = 200

    def compute_mock_response_metadata(self):
        # It may be that uuid.uuid4() is further mocked, but either way it needs to return something
        # that is used in two places consistently.
        request_id = str(uuid.uuid4())
        http_status_code = self.MOCK_STATUS_CODE
        return {
            'RequestId': request_id,
            'HTTPStatusCode': http_status_code,
            'HTTPHeaders': self.compute_mock_request_headers(request_id),
            'RetryAttempts': self.MOCK_RETRY_ATTEMPTS,
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

    MOCK_QUEUE_URL_PREFIX = 'https://queue.amazonaws.com.mock/12345/'  # just something to make it look like a URL

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


class VersionChecker:

    """
    Given appropriate customizations, this allows cross-checking of pyproject.toml and a changelog for consistency.

    You must subclass this class, specifying both the pyproject filename and the changelog filename as
    class variables PYPROJECT and CHANGELOG, respectively.

    def test_version():

        class MyAppVersionChecker(VersionChecker):
            PYPROJECT = os.path.join(ROOT_DIR, "pyproject.toml")
            CHANGELOG = os.path.join(ROOT_DIR, "CHANGELOG.rst")

        MyAppVersionChecker.check_version()

    """

    PYPROJECT = CustomizableProperty('PYPROJECT', description="The repository-relative name of the pyproject file.")
    CHANGELOG = CustomizableProperty('CHANGELOG', description="The repository-relative name of the change log.")

    # I wanted to use pytest.PytestConfigWarning, but that creates a dependency
    # on particular versions of pytest, and we don't export a delivery
    # constraint of a particular pytest version. So RuntimeWarning is a
    # safer setting for now. -kmp 14-Jan-2021
    WARNING_CATEGORY = RuntimeWarning

    @classmethod
    def check_version(cls):
        version = cls._check_version()
        if getattr_customized(cls, "CHANGELOG"):
            cls._check_change_history(version)

    @classmethod
    def _check_version(cls):

        __tracebackhide__ = True

        pyproject_file = getattr_customized(cls, 'PYPROJECT')
        assert os.path.exists(pyproject_file), "Missing pyproject file: %s" % pyproject_file
        pyproject = toml.load(pyproject_file)
        version = pyproject.get('tool', {}).get('poetry', {}).get('version', None)
        assert version, "Missing version in %s." % pyproject_file
        PRINT("Version = %s" % version)
        return version

    VERSION_LINE_PATTERN = re.compile("^[#* ]*([0-9]+[.][^ \t\n]*)([ \t\n].*)?$")
    VERSION_IS_BETA_PATTERN = re.compile("^.*[0-9][Bb][0-9]+$")

    @classmethod
    def _check_change_history(cls, version=None):

        if version and cls.VERSION_IS_BETA_PATTERN.match(version):
            # Don't require beta versions to match up in change log.
            # We don't just strip the version and look at that because sometimes we use other numbers on betas.
            # Better to just not do it at all.
            return

        changelog_file = getattr_customized(cls, "CHANGELOG")

        if not changelog_file:
            if version:
                raise AssertionError("Cannot check version without declaring a CHANGELOG file.")
            return

        assert os.path.exists(changelog_file), "Missing changelog file: %s" % changelog_file

        with io.open(changelog_file) as fp:
            versions = []
            for line in fp:
                m = cls.VERSION_LINE_PATTERN.match(line)
                if m:
                    versions.append(m.group(1))

        assert versions, "No version info was parsed from %s" % changelog_file

        # Might be sorted top to bottom or bottom to top, but ultimately the current version should be first or last.
        if versions[0] != version and versions[-1] != version:
            warnings.warn("Missing entry for version %s in %s." % (version, changelog_file),
                          category=cls.WARNING_CATEGORY, stacklevel=2)
            return


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
