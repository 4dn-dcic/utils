"""
qa_utils: Tools for use in quality assurance testing.
"""

import contextlib
import datetime
import time
import io
import os
import pytz

from json import dumps as json_dumps, loads as json_loads
from .misc_utils import PRINT, ignored, Retry


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


FILE_SYSTEM_VERBOSE = True


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

    def __init__(self, files=None, default_encoding='utf-8'):
        self.default_encoding = default_encoding
        self.files = {filename: content.encode(default_encoding) for filename, content in (files or {}).items()}

    def exists(self, file):
        return bool(self.files.get(file))

    def remove(self, file):
        if not self.files.pop(file, None):
            raise FileNotFoundError("No such file or directory: %s" % file)

    def open(self, file, mode='r'):
        if FILE_SYSTEM_VERBOSE:
            print("Opening %r in mode %r." % (file, mode))
        if mode == 'w':
            return self._open_for_write(file_system=self, file=file, binary=False)
        elif mode == 'wb':
            return self._open_for_write(file_system=self, file=file, binary=True)
        elif mode == 'r':
            return self._open_for_read(file, binary=False)
        elif mode == 'rb':
            return self._open_for_read(file, binary=True)
        else:
            raise AssertionError("Mocked io.open doesn't handle mode=%r." % mode)

    def _open_for_read(self, file, binary=False, encoding=None):
        content = self.files.get(file)
        if content is None:
            raise FileNotFoundError("No such file or directory: %s" % file)
        if FILE_SYSTEM_VERBOSE:
            print("Read %r to %s." % (content, file))
        return io.BytesIO(content) if binary else io.StringIO(content.decode(encoding or self.default_encoding))

    def _open_for_write(self, file_system, file, binary=False, encoding=None):
        return MockFileWriter(file_system=file_system, file=file, binary=binary,
                              encoding=encoding or self.default_encoding)


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

    def __init__(self, status_code=200, json=None, content=None):
        self.status_code = status_code
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


class MockBotoS3Client:
    """
    This is a mock of certain S3 functionality.
    """

    def __init__(self):
        self.s3_files = MockFileSystem()

    def upload_fileobj(self, Fileobj, Bucket, Key, **kwargs):  # noqa - Uppercase argument names are chosen by AWS
        if kwargs:
            raise MockKeysNotImplemented("upload_fileobj", kwargs.keys())
        data = Fileobj.read()
        print("Uploading %s (%s bytes) to bucket %s key %s"
              % (Fileobj, len(data), Bucket, Key))
        with self.s3_files.open(os.path.join(Bucket, Key), 'wb') as fp:
            fp.write(data)

    def upload_file(self, Filename, Bucket, Key, **kwargs):  # noqa - Uppercase argument names are chosen by AWS
        if kwargs:
            raise MockKeysNotImplemented("upload_file", kwargs.keys())

        with io.open(Filename, 'rb') as fp:
            self.upload_fileobj(Fileobj=fp, Bucket=Bucket, Key=Key)

    def download_fileobj(self, Bucket, Key, Fileobj, **kwargs):  # noqa - Uppercase argument names are chosen by AWS
        if kwargs:
            raise MockKeysNotImplemented("upload_file", kwargs.keys())

        with self.s3_files.open(os.path.join(Bucket, Key), 'rb') as fp:
            data = fp.read()
        print("Downloading bucket %s key %s (%s bytes) to %s"
              % (Bucket, Key, len(data), Fileobj))
        Fileobj.write(data)

    def download_file(self, Bucket, Key, Filename, **kwargs):
        if kwargs:
            raise MockKeysNotImplemented("upload_file", kwargs.keys())
        with io.open(Filename, 'wb') as fp:
            self.download_fileobj(Bucket=Bucket, Key=Key, Fileobj=fp)
