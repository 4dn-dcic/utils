"""
This file contains functions that might be generally useful.
"""

from collections import namedtuple
import appdirs
import contextlib
import datetime
import functools
import hashlib
import inspect
import io
import json
import logging
import math
import os
import platform
import pytz
import re
import rfc3986.validators
import rfc3986.exceptions
import shortuuid
import time
import uuid
import warnings
import webtest  # importing the library makes it easier to mock testing

from collections import defaultdict
from datetime import datetime as datetime_type
from dateutil.parser import parse as dateutil_parse
from typing import Any, Callable, List, Optional, Tuple, Union


# Is this the right place for this? I feel like this should be done in an application, not a library.
# -kmp 27-Apr-2020
logging.basicConfig()


class NamedObject(object):

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f"<{self.name}>"

    def __repr__(self):
        return f"<{self.name}@{id(self):x}>"


# Using PRINT(...) for debugging, rather than its more familiar lowercase form) for intended programmatic output,
# makes it easier to find stray print statements that were left behind in debugging. -kmp 30-Mar-2020

class _MOCKABLE_IO:

    def __init__(self, wrapped_action):
        self.wrapped_action = wrapped_action  # necessary indirection for sake of qa_utils.printed_output

    def __call__(self, *args, timestamped=False, **kwargs):
        """
        Prints its args space-separated, as 'print' would, possibly with an hh:mm:ss timestamp prepended.

        :param args: an object to be printed
        :param with_time: a boolean specifying whether to prepend a timestamp
        """
        if timestamped:
            hh_mm_ss = str(datetime.datetime.now().strftime("%H:%M:%S"))
            return self.wrapped_action(' '.join(map(str, (hh_mm_ss, *args))), **kwargs)
        else:
            return self.wrapped_action(' '.join(map(str, args)), **kwargs)


def _mockable_input(*args):
    return input(*args)


builtin_print = print

PRINT = _MOCKABLE_IO(wrapped_action=print)
PRINT.__name__ = 'PRINT'

INPUT = _MOCKABLE_IO(wrapped_action=_mockable_input)
INPUT.__name__ = 'INPUT'

prompt_for_input = INPUT  # In Python 3, input does 'safe' input reading. INPUT is our mockable alternative


@contextlib.contextmanager
def lines_printed_to(file):
    """
    This context manager opens a file and returns a function that can be called repeatedly during the body context
    to do do line-by-line output to that file. It uses PRINT to do that output, so the unit test tools for PRINT
    can be used to test this.
    """
    with io.open(file, 'w') as fp:
        def write_line(s=""):
            PRINT(s, file=fp)
        yield write_line


def print_error_message(exception, full=False):
    """
    Prints an error message (using dcicutils.misc_utils.PRINT) in the conventional way, as:
      <error-type>: <error-message>
    With full=True, the error-type can be made to use dcicutils.misc_utils.full_class_name to get a module name, so:
      <module-qualified-error-type>: <error-message>
    """
    PRINT(get_error_message(exception, full=full))


def get_error_message(exception, full=False):
    """
    Returns an error message (using dcicutils.misc_utils.PRINT) formatted in the conventional way, as:
      "<error-type>: <error-message>"
    With full=True, the error-type can be made to use dcicutils.misc_utils.full_class_name to get a module name, so:
      "<module-qualified-error-type>: <error-message>"
    """
    exception_type_name = full_class_name(exception) if full else exception.__class__.__name__
    error_message = f"{exception_type_name}: {exception}"
    return error_message


absolute_uri_validator = (
    rfc3986.validators.Validator()
    # Validation qualifiers
    .allow_schemes('http', 'https')
    # TODO: We might want to consider the possibility of forbidding the use of a password. -kmp 20-Apr-2021
    # .forbid_use_of_password()
    .require_presence_of('scheme', 'host')
    .check_validity_of('scheme', 'host', 'path'))


def is_valid_absolute_uri(text):
    """
    Returns True if the given text is a string in the proper format to be an 'absolute' URI,
    by which we mean the URI has a scheme (http or https) and a host specification.

    For more info, see "Uniform Resource Identifier (URI): Generic Syntax" at https://tools.ietf.org/html/rfc3986
    """
    # Technically something like 'foo/bar.html' is also a URI, but it is a relative one, and
    # the intended use of this function is to verify the URI specification of a resource on the web,
    # independent of browser context, so a relative specification would be meaningless. We can add
    # a separate operation for that later if we need one.
    #
    # We don't use rfc3987 (IRIs) both because it allows some resource locators we're not sure we're
    # committed to accepting. Wikipedia, in https://en.wikipedia.org/wiki/Internationalized_Resource_Identifier,
    # hints that there is some controversy about whether IRIs are even a good idea. We can revisit the idea if
    # someone is demanding it. (And, as a practical matter, the rfc3987 library has a problematic license.)
    # -kmp 21-Apr-2021
    try:
        uri_ref = rfc3986.uri_reference(text)
    except Exception:
        return False
    try:
        absolute_uri_validator.validate(uri_ref)
        return True
    except rfc3986.exceptions.ValidationError:
        return False


class VirtualAppError(Exception):
    """ Special Exception to be raised by VirtualApp that contains some additional info """

    def __init__(self, msg, url, body, raw_exception):
        super(VirtualAppError, self).__init__(msg)
        self.msg = msg
        self.query_url = url
        self.query_body = body
        self.raw_exception = raw_exception

    def __repr__(self):
        return ("Exception encountered on VirtualApp\n"
                "URL: %s\n"
                "BODY: %s\n"
                "MSG: %s\n"
                "Raw Exception: %s\n" % (self.query_url, self.query_body, self.msg, self.raw_exception))

    def __str__(self):
        return self.__repr__()


# So whether people import this from webtest or here, they will get the same object.
TestApp = webtest.TestApp

# This ("monkey patch") side-effect will affect webtest.TestApp, too, but that's OK for us.
# https://en.wikipedia.org/wiki/Monkey_patch
# We want any use of WebTest.TestApp to NOT be seen as a test for the purposes of PyTest.

TestApp.__test__ = False


class _VirtualAppHelper(webtest.TestApp):
    """
    A helper class equivalent to webtest.TestApp, except that it isn't intended for test use.
    """

    pass


class AbstractVirtualApp:
    pass


class VirtualApp(AbstractVirtualApp):
    """
    Wrapper class for TestApp, to allow custom control over submitting Encoded requests,
    simulating a number of conditions, including permissions.

    IMPORTANT: We use webtest.TestApp is used as substrate technology here, but use of this class
        occurs in the main application, not just in testing. Among other things, we have
        renamed the app here in order to avoid confusions created by the name when it is used
        in production settings.
    """
    HELPER_CLASS = _VirtualAppHelper

    def __init__(self, app, environ):
        """
        Builds an encoded application, allowing you to submit requests to an encoded application

        :param app: return value of get_app(config_uri, app_name)
        :param environ: options to pass to the application. Usually permissions.
        """
        #  NOTE: The TestApp class that we're wrapping takes a richer set of initialization parameters
        #        (including relative_to, use_unicode, cookiejar, parser_features, json_encoder, and lint),
        #        but we'll add them conservatively here. If there is a need for any of them, we should add
        #        them explicitly here one-by-one as the need is shown so we have tight control of what
        #        we're depending on and what we're not. -kmp 27-Apr-2020
        self.wrapped_app = self.HELPER_CLASS(app, environ)

    def get(self, url, **kwargs):
        """ Wrapper for TestApp.get that logs the outgoing GET

        :param url: url to GET
        :param kwargs: args to pass to the GET
        :return: result of GET
        """
        logging.info('OUTGOING HTTP GET: %s' % url)
        try:
            return self.wrapped_app.get(url, **kwargs)
        except webtest.AppError as e:
            raise VirtualAppError(msg='HTTP GET failed.', url=url, body='<empty>', raw_exception=e)

    def post(self, url, obj, **kwargs):
        """ Wrapper for TestApp.post that logs the outgoing POST

        :param url: url to POST to
        :param obj: object body to POST
        :param kwargs: args to pass to the POST
        :return: result of POST
        """
        logging.info('OUTGOING HTTP POST on url: %s with object: %s' % (url, obj))
        try:
            return self.wrapped_app.post(url, obj, **kwargs)
        except webtest.AppError as e:
            raise VirtualAppError(msg='HTTP POST failed.', url=url, body=obj, raw_exception=e)

    def post_json(self, url, obj, **kwargs):
        """ Wrapper for TestApp.post_json that logs the outgoing POST

        :param url: url to POST to
        :param obj: object body to POST
        :param kwargs: args to pass to the POST
        :return: result of POST
        """
        logging.info('OUTGOING HTTP POST on url: %s with object: %s' % (url, obj))
        try:
            return self.wrapped_app.post_json(url, obj, **kwargs)
        except webtest.AppError as e:
            raise VirtualAppError(msg='HTTP POST failed.', url=url, body=obj, raw_exception=e)

    def put_json(self, url, obj, **kwargs):
        """ Wrapper for TestApp.put_json that logs the outgoing PUT

        :param url: url to PUT to
        :param obj: object body to PUT
        :param kwargs: args to pass to the PUT
        :return: result of PUT
        """
        logging.info('OUTGOING HTTP PUT on url: %s with object: %s' % (url, obj))
        try:
            return self.wrapped_app.put_json(url, obj, **kwargs)
        except webtest.AppError as e:
            raise VirtualAppError(msg='HTTP PUT failed.', url=url, body=obj, raw_exception=e)

    def patch_json(self, url, fields, **kwargs):
        """ Wrapper for TestApp.patch_json that logs the outgoing PATCH

        :param url: url to PATCH to, should contain an object uuid
        :param fields: fields to PATCH on uuid in URL
        :param kwargs: args to pass to the PATCH
        :return: result of PATCH
        """
        logging.info('OUTGOING HTTP PATCH on url: %s with changes: %s' % (url, fields))
        try:
            return self.wrapped_app.patch_json(url, fields, **kwargs)
        except webtest.AppError as e:
            raise VirtualAppError(msg='HTTP PATCH failed.', url=url, body=fields, raw_exception=e)

    @property
    def app(self):
        """ Returns the .app of the wrapped_app.

            For example, this allows one to refer to myapp.app.registry without having to know
            if myapp is a TestApp or a VirtualApp.
        """
        return self.wrapped_app.app


VirtualAppResponse = webtest.response.TestResponse  # NoQA - PyCharm sees a problem, but none occurs in practice


def exported(*variables):
    """
    This function does nothing but is used for declaration purposes.
    It is useful for the situation where one module imports names from another module merely to allow
    functions in another module to import them, usually for legacy compatibility.
    Otherwise, the import might look unnecessary.
    e.g.,

    ---file1.py---
    def identity(x):
        return x

    ---file2.py---
    from .file1 import identity
    from dcicutils.misc_utils import exported

    # This function used to be defined here, but now is defined in file1.py
    exported(identity)

    ---file3.py---
    # This file has not been updated to realize that file1.py is the new home of identity.
    from .file2 import identity
    print("one=", identity(1))  # noQA - code example
    """
    ignored(variables)


def ignored(*args, **kwargs):
    """
    This is useful for defeating flake warnings.
    Call this function to use values that really should be ignored.
    This is intended as a declaration that variables are intentionally ignored,
    but no enforcement of that is done. Some sample uses:

    def foo(x, y):
        ignored(x, y)  # so flake8 won't complain about x and y being unused.
        return 3

    def map_action(action, data, options, precheck=ignored):
        precheck(data, **options)
        action(data, **options)
    """
    return args, kwargs


def ignorable(*args, **kwargs):
    """
    This is useful for defeating flake warnings.
    Call this function to use values that really might be ignored.
    This is intended as a declaration that variables are or might be intentionally ignored,
    but no enforcement of that is done. Some sample uses:

    def foo(x, y):
        ignorable(x, y)  # so flake8 won't complain about unused vars, whether or not next line is commented out.
        # print(x, y)
        return 3

    foo_synonym = foo
    ignorable(foo_synonym)  # We might or might not use foo_synonym, but we don't want it reported as unused
    """
    return args, kwargs


def get_setting_from_context(settings, ini_var, env_var=None, default=None):
    """
    This gets a value from either an environment variable or a config file.

    The environment variable overrides, since it is more dynamic in nature than a config file,
    which might be checked into source control.

    If the value of env_var is None, it will default to a name similar to ini_var,
    but in uppercase and with '.' replaced by '_'. So a 'foo.bar' ini file setting
    will defaultly correspond to a 'FOO_BAR' environment variable. This can be overridden
    by using an string argument for env_var to specify the environment variable, or using False
    to indicate that no env_var is allowed.
    """
    if env_var is not False:  # False specially means don't allow an environ variable, in case that's ever needed.
        if env_var is None:
            # foo.bar.baz in config file corresponds to FOO_BAR_BAZ as an environment variable setting.
            env_var = ini_var.upper().replace(".", "_")
        # NOTE WELL: An implication of this is that an environment variable of an empty string
        #            will override a config file setting that is non-empty. This uses 'principle of least surprise',
        #            that if environment variable settings appear to set a null string, that's what should prevail.
        if env_var in os.environ:
            return os.environ.get(env_var)
    return settings.get(ini_var, default)


@contextlib.contextmanager
def filtered_warnings(action, message="", category=None, module="", lineno=0, append=False):
    """
    Context manager temporarily filters deprecation messages for the duration of the body.

    Except for its dynamic scope, this is used otherwise the same as warnings.filterwarnings would be used.

    If category is unsupplied, it should be a class object that is Warning (the default) or one of its subclasses.

    For example:

           with filtered_warnings('ignore', category=DeprecationWarning):
               ... use something that's obsolete without a lot of fuss ...

    Note: This is not threadsafe. It's OK while loading system and during testing,
          but not in worker threads.
    """
    if category is None:
        category = Warning
    with warnings.catch_warnings():
        warnings.filterwarnings(action, message=message, category=category, module=module,
                                lineno=lineno, append=append)
        yield


class Retry:

    """
    This class exists primarily to hold onto data relevant to the Retry.retry_allowed decorator.
    There is no need to instantiate the class in order for it to work.

    This class also has a subclass qa_utils.RetryManager that adds the ability to locally bind data
    that has been declared with this decorator.
    """

    class RetryOptions:
        """
        A helper class used internally by the Retry class.

        One of these objects is created and registered for each decorated function unless the name_key is 'anonymous'.
        See Retry._RETRY_OPTIONS_CATALOG.
        """

        def __init__(self, retries_allowed=None, wait_seconds=None, wait_increment=None, wait_multiplier=None):
            self.retries_allowed = retries_allowed
            self.wait_seconds = wait_seconds or 0  # None or False mean 0 seconds
            self.wait_increment = wait_increment
            self.wait_multiplier = wait_multiplier
            self.wait_adjustor = self.make_wait_adjustor(wait_increment=wait_increment, wait_multiplier=wait_multiplier)

        @staticmethod
        def make_wait_adjustor(wait_increment=None, wait_multiplier=None):
            """
            Returns a function that can be called to adjust wait_seconds based on wait_increment or wait_multiplier
            before doing a retry at each step.
            """
            if wait_increment and wait_multiplier:
                raise SyntaxError("You may not specify both wait_increment and wait_multiplier.")

            if wait_increment:
                return lambda x: x + wait_increment
            elif wait_multiplier:
                return lambda x: x * wait_multiplier
            else:
                return lambda x: x

        @property
        def tries_allowed(self):
            return 1 + self.retries_allowed

    _RETRY_OPTIONS_CATALOG = {}

    DEFAULT_RETRIES_ALLOWED = 1
    DEFAULT_WAIT_SECONDS = 0
    DEFAULT_WAIT_INCREMENT = None
    DEFAULT_WAIT_MULTIPLIER = None

    @classmethod
    def _defaulted(cls, value, default):
        """ Triages between argument values and class-declared defaults. """
        return default if value is None else value

    @classmethod
    def retry_allowed(cls, name_key=None, retries_allowed=None, wait_seconds=None,
                      wait_increment=None, wait_multiplier=None):
        """
        Used as a decorator on a function definition, makes that function do retrying before really failing.
        For example:

            @Retry.retry_allowed(retries_allowed=4, wait_seconds=2, wait_multiplier=1.25)
            def something_that_fails_a_lot(...):
                ... flaky code ...

        will cause the something_that_fails_a_lot(...) code to retry several times before giving up,
        either using the same wait each time or, if given a wait_multiplier or wait_increment, using
        that advice to adjust the wait time upward on each time.

        Args:

            name_key: An optional key that can be used by qa_utils.RetryManager to adjust these parameters in testing.
                      If the argument is 'anonymous', no record will be created.
            retries_allowed: The number of retries allowed. Default is cls.DEFAULT_RETRIES_ALLOWED.
            wait_seconds: The number of wait_seconds between retries. Default is cls.DEFAULT_WAIT_SECONDS.
            wait_increment: A fixed increment by which the number of wait_seconds is adjusted on each retry.
            wait_multiplier: A multiplier by which the number of wait_seconds is adjusted on each retry.
        """

        def _decorator(function):
            function_name = name_key or function.__name__
            function_profile = cls.RetryOptions(
                retries_allowed=cls._defaulted(retries_allowed, cls.DEFAULT_RETRIES_ALLOWED),
                wait_seconds=cls._defaulted(wait_seconds, cls.DEFAULT_WAIT_SECONDS),
                wait_increment=cls._defaulted(wait_increment, cls.DEFAULT_WAIT_INCREMENT),
                wait_multiplier=cls._defaulted(wait_multiplier, cls.DEFAULT_WAIT_MULTIPLIER),
            )

            check_true(isinstance(retries_allowed, int) and retries_allowed >= 0,
                       "The retries_allowed must be a non-negative integer.",
                       error_class=ValueError)

            # See the 'retrying' method to understand what this is about. -kmp 8-Jul-2020
            if function_name != 'anonymous':
                cls._RETRY_OPTIONS_CATALOG[function_name] = function_profile  # Only for debugging.

            @functools.wraps(function)
            def wrapped_function(*args, **kwargs):
                tries_allowed = function_profile.tries_allowed
                wait_seconds = function_profile.wait_seconds or 0
                last_error = None
                for i in range(tries_allowed):
                    if i > 0:
                        if i > 1:
                            wait_seconds = function_profile.wait_adjustor(wait_seconds)
                        if wait_seconds > 0:
                            time.sleep(wait_seconds)
                    try:
                        success = function(*args, **kwargs)
                        return success
                    except Exception as e:
                        last_error = e
                if last_error is not None:
                    raise last_error

            return wrapped_function

        return _decorator

    @classmethod
    def retrying(cls, fn, retries_allowed=None, wait_seconds=None, wait_increment=None, wait_multiplier=None):
        """
        Similar to the @Retry.retry_allowed decorator, but used around individual calls. e.g.,

            res = Retry.retrying(testapp.get)(url)

        If you don't like the defaults, you can override them with arguments:

            res = Retry.retrying(testapp.get, retries_allowed=5, wait_seconds=1)(url)

        but if you need to do it a lot, you can make a subclass:

            class MyRetry(Retry):
                DEFAULT_RETRIES_ALLOWED = 5
                DEFAULT_WAIT_SECONDS = 1
            retrying = MyRetry.retrying  # Avoids saying MyRetry.retrying(...) everywhere
            ...
            res1 = retrying(testapp.get)(url)
            res2 = retrying(testapp.get)(url)
            ...etc.

        Args:

            fn: A function that will be retried on failure.
            retries_allowed: The number of retries allowed. Default is cls.DEFAULT_RETRIES_ALLOWED.
            wait_seconds: The number of wait_seconds between retries. Default is cls.DEFAULT_WAIT_SECONDS.
            wait_increment: A fixed increment by which the number of wait_seconds is adjusted on each retry.
            wait_multiplier: A multiplier by which the number of wait_seconds is adjusted on each retry.

        Returns: whatever the fn returns, assuming it returns normally/successfully.
        """
        # A special name_key of 'anonymous' is the default, which causes there not to be a name key.
        # This cannot work in conjunction with RetryManager because different calls may result in different
        # function values at the same point in code. -kmp 8-Jul-2020
        decorator_function = Retry.retry_allowed(
            name_key='anonymous', retries_allowed=retries_allowed, wait_seconds=wait_seconds,
            wait_increment=wait_increment, wait_multiplier=wait_multiplier
        )
        return decorator_function(fn)


def apply_dict_overrides(dictionary: dict, **overrides) -> dict:
    """
    Assigns a given set of overrides to a dictionary, ignoring any entries with None values, which it leaves alone.
    """
    # I'm not entirely sure the treatment of None is the right thing. Need to look into that.
    # Then again, if None were stored, then apply_dict_overrides(d, var1=1, var2=2, var3=None)
    # would be no different than (dict(d, var1=1, var2=2, var3=None). It might be more useful
    # and/or interesting if it would actually remove the key instead. -kmp 18-Jul-2020
    for k, v in overrides.items():
        if v is not None:
            dictionary[k] = v
    # This function works by side effect, but getting back the changed dict may be sometimes useful.
    return dictionary


def utc_now_str():
    # from jsonschema_serialize_fork date-time format requires a timezone
    return datetime.datetime.utcnow().isoformat() + '+00:00'


def utc_today_str():
    """Returns a YYYY-mm-dd date string, relative to the UTC timezone."""
    return datetime.datetime.strftime(datetime.datetime.utcnow(), "%Y-%m-%d")


def as_seconds(*, seconds=0, minutes=0, hours=0, days=0, weeks=0, milliseconds=0, as_type=None):
    """
    Coerces a relative amount of time (keyword arguments seconds, minutes, etc. like timedelta) into seconds.

    If the number of seconds is an integer, it will be coerced to an integer. Otherwise, it will be a float.
    If as_float is given and not None, it will be applied as a function to the result, allowing it to be coerced
    to another value than an integer or float.  For example,
      >>> as_seconds(seconds=1, minutes=1)
      61
      >>> as_seconds(seconds=1, minutes=1, as_type=str)
      '61'
    """
    delta = datetime.timedelta(seconds=seconds, minutes=minutes, hours=hours,
                               days=days, weeks=weeks, milliseconds=milliseconds)
    seconds = delta.total_seconds()
    frac, intpart = math.modf(seconds)
    if frac == 0.0:
        seconds = int(intpart)
    if as_type is not None:
        seconds = as_type(seconds)
    return seconds


MIN_DATETIME = datetime.datetime(datetime.MINYEAR, 1, 1, 0, 0, 0)
MIN_DATETIME_UTC = datetime.datetime(datetime.MINYEAR, 1, 1, 0, 0, 0, tzinfo=pytz.UTC)


def future_datetime(*, now=None, seconds=0, minutes=0, hours=0, days=0, weeks=0, milliseconds=0):
    delta = datetime.timedelta(seconds=seconds, minutes=minutes, hours=hours,
                               days=days, weeks=weeks, milliseconds=milliseconds)
    return (now or datetime.datetime.now()) + delta


REF_TZ = pytz.timezone(os.environ.get("REF_TZ") or "US/Eastern")


class DatetimeCoercionFailure(ValueError):

    def __init__(self, timespec, timezone):
        self.timespec = timespec
        self.timezone = timezone
        extra = ""
        if timezone:
            extra = " (for timezone %s)" % timezone
        super().__init__("Cannot coerce to datetime: %s%s" % (timespec, extra))


def as_datetime(timespec, tz=None, raise_error=True):
    """
    Parses the given date/time (which may be a string or a datetime.datetime), returning a datetime.datetime object.

    If the given datetime is already such an object, it is just returned (not necessarily in the given timezone).
    If the datetime to be returned has no timezone and a timezone argument has been given, that timezone is applied.
    If it is a string, it should be in a format such as 'yyyy-mm-dd hh:mm:ss' or 'yyyy-mm-dd hh:mm:ss-nnnn'
    (with -nnnn being a timezone specification).
    If the given time is not a datetime, and cannot be coerced to be done, an error is raised
    unless raise_error (default True) is False.
    """
    try:
        # This type check has to work even if datetime is mocked, so we use it under another variable name to
        # make it harder to mock out. -kmp 6-Nov-2020
        dt = timespec
        if not isinstance(dt, datetime_type):
            dt = dateutil_parse(dt)
        if tz and not dt.tzinfo:
            dt = tz.localize(dt)
        return dt
    except Exception:
        # I decided to treat the returning None case as a bug. It was not advertised and not used.
        # Throwing an error by default will make this more consistent with as_ref_datetime and as_utc_datetime.
        # But just in case there is a use that wanted None, so it's easy to fix, raise_error=False can be supplied.
        # -kmp 29-Nov-2020
        if raise_error:
            raise DatetimeCoercionFailure(timespec=timespec, timezone=tz)
        else:
            return None


def as_ref_datetime(timespec):
    """
    Parses a given datetime, returning a rendition of that tie in the reference timezone (US/Eastern by default).

    If the input time is a string or a naive datetime with no timezone, it is assumed to be in the reference timezone
    (which is US/Eastern by default).
    If the time is already a datetime, no parsing occurs, but the time is still adjusted to use the reference timeszone.
    If the given time is not a datetime, and cannot be coerced to be done, an error is raised.
    """
    try:
        dt = as_datetime(timespec, tz=REF_TZ)
        hms_dt = dt.astimezone(REF_TZ)
        return hms_dt
    except Exception:
        raise DatetimeCoercionFailure(timespec=timespec, timezone=REF_TZ)


def as_utc_datetime(timespec):
    """
    Parses a given datetime, returning a rendition of that tie in UTC.

    If the input time is a string or a naive datetime with no timezone, it is assumed to be in the reference timezone
    (which is US/Eastern by default). UTC is only used as the output format, not as an assumption about the input.
    If the time is already a datetime, no parsing occurs, but the time is still adjusted to use UTC.
    If the given time is not a datetime, and cannot be coerced to be done, an error is raised.
    """
    try:
        dt = as_datetime(timespec, tz=REF_TZ)
        utc_dt = dt.astimezone(pytz.UTC)
        return utc_dt
    except Exception:
        raise DatetimeCoercionFailure(timespec=timespec, timezone=pytz.UTC)


def in_datetime_interval(when, *, start=None, end=None):
    """
    Returns true if the first argument ('when') is in the range given by the other arguments.

    The comparison is upper- and lower-inclusive.
    The string will be parsed as a datetime in the reference timezone (REF_TZ) if it doesn't have an explicit timezone.
    """
    when = as_ref_datetime(when)  # This is not allowed to be None, but could be str and we need datetimes to compare.
    start = start and as_ref_datetime(start)
    end = end and as_ref_datetime(end)
    return (not start or start <= when) and (not end or end >= when)


def ref_now():
    """Returns the current time in the portal's reference timezone, as determined by REF_TZ.

       Because this software originates at Harvard Medical School, the reference timezone defaults to US/Eastern.
       It can be set to another value by binding the REF_TZ environment variable."""
    return as_datetime(datetime.datetime.now(), REF_TZ)


class LockoutManager:
    """
    This class is used as a guard of a critical operation that can only be called within a certain frequency.
    e.g.,

        class Foo:
            def __init__(self):
                # 60 seconds required between calls, with a 1 second margin of error (overhead, clocks varying, etc)
                self.lockout_manager = LockoutManager(action="foo", lockout_seconds=1, safety_seconds=1)
            def foo():
                self.lockout_manager.wait_if_needed()
                do_guarded_action()

        f = Foo()     # make a Foo
        v1 = f.foo()  # will immediately get a value
        time.sleep(58)
        v2 = f.foo()  # will wait about 2 seconds, then get a value
        v3 = f.foo()  # will wait about 60 seconds, then get a value

    Conceptually this is a special case of RateManager for n=1, though in practice it arose differently and
    the supplementary methods (which we happen to use mostly for testing) differ because the n=1 case is simpler
    and admits more questions. So, for now at least, this is not a subclass of RateManager but a separate
    implementation.
    """

    EARLIEST_TIMESTAMP = datetime.datetime(datetime.MINYEAR, 1, 1)  # maybe useful for testing

    def __init__(self, *, lockout_seconds, safety_seconds=0, action="metered action", enabled=True, log=None):
        """
        Creates a LockoutManager that cooperates in assuring a guarded operation is only happens at a certain rate.

        The rate is once person lockout_seconds. This is a special case of RateManager and might get phased out
        as redundant, but has slightly different operations available for testing.

        Args:

        lockout_seconds int: A theoretical number of seconds allowed between calls to the guarded operation.
        safety_seconds int: An amount added to interval_seconds to accommodate real world coordination fuzziness.
        action str: A noun or noun phrase describing the action being guarded.
        enabled bool: A boolean controlling whether this facility is enabled. If False, waiting is disabled.
        log object: A logger object (supporting operations like .debug, .info, .warning, and .error).
        """

        # This makes it easy to turn off the feature
        self.lockout_enabled = enabled
        self.lockout_seconds = lockout_seconds
        self.safety_seconds = safety_seconds
        self.action = action
        self._timestamp = self.EARLIEST_TIMESTAMP
        self.log = log or logging

    @property
    def timestamp(self):
        """The timestamp is read-only. Use update_timestamp() to set it."""
        return self._timestamp

    @property
    def effective_lockout_seconds(self):
        """
        The effective time between calls

        Returns: the sum of the lockout and the safety seconds
        """
        return self.lockout_seconds + self.safety_seconds

    def wait_if_needed(self):
        """
        This function is intended to be called immediately prior to each guarded operation.

        This function will wait (using time.sleep) only if necessary, and for the amount necessary,
        to comply with rate-limiting declared in the creation of this LockoutManager.

        NOTE WELL: It is presumed that all calls are coming from this source. This doesn't have ESP that would
        detect or otherwise accommodate externally generated calls, so violations of rate-limiting can still
        happen that way. This should be sufficient for sequential testing, and better than nothing for
        production operation.  This is not a substitute for responding to server-initiated throttling protocols.
        """
        now = datetime.datetime.now()
        # Note that this quantity is always positive because now is always bigger than the timestamp.
        seconds_since_last_attempt = (now - self._timestamp).total_seconds()
        # Note again that because seconds_since_last_attempt is positive, the wait seconds will
        # never exceed self.effective_lockout_seconds, so
        #   0 <= wait_seconds <= self.effective_lockout_seconds
        wait_seconds = max(0.0, self.effective_lockout_seconds - seconds_since_last_attempt)
        if wait_seconds > 0.0:
            shared_message = ("Last %s attempt was at %s (%s seconds ago)."
                              % (self.action, self._timestamp, seconds_since_last_attempt))
            if self.lockout_enabled:
                action_message = "Waiting %s seconds before attempting another." % wait_seconds
                self.log.warning("%s %s" % (shared_message, action_message))
                time.sleep(wait_seconds)
            else:
                action_message = "Continuing anyway because lockout is disabled."
                self.log.warning("%s %s" % (shared_message, action_message))
        self.update_timestamp()

    def update_timestamp(self):
        """
        Explicitly sets the reference time point for computation of our lockout.
        This is called implicitly by .wait_if_needed(), and for some situations that may be sufficient.
        """
        self._timestamp = datetime.datetime.now()


class RateManager:
    """
    This class is used for functions that can only be called at a certain rate, described by calls per unit time.
    e.g.,

        class Foo:
            def __init__(self):
                # 60 seconds required between calls, with a 1 second margin of error (overhead, clocks varying, etc)
                self.rate_manager = RateManager(action="foo", interval_seconds=1, safety_seconds=1)
            def foo():
                self.lockout_manager.wait_if_needed()
                do_guarded_action()

        f = Foo()     # make a Foo
        v1 = f.foo()  # will immediately get a value
        time.sleep(58)
        v2 = f.foo()  # will wait about 2 seconds, then get a value
        v3 = f.foo()  # will wait about 60 seconds, then get a value

    Conceptually this is a special case of RateManager for n=1, though in practice it arose differently and
    the supplementary methods (which we happen to use mostly for testing) differ because the n=1 case is simpler
    and admits more questions. So, for now at least, this is not a subclass of RateManager but a separate
    implementation.
    """

    EARLIEST_TIMESTAMP = datetime.datetime(datetime.MINYEAR, 1, 1)  # maybe useful for testing

    def __init__(self, *, interval_seconds, safety_seconds=0, allowed_attempts=1,
                 action="metered action", enabled=True, log=None, wait_hook=None):
        """
        Creates a RateManager that cooperates in assuring that a guarded operation happens only at a certain rate.

        The rate is measured as allowed_attempts per interval_seconds.

        Args:

        interval_seconds int: A number of seconds (the theoretical denominator of the allowed rate)
        safety_seconds int: An amount added to interval_seconds to accommodate real world coordination fuzziness.
        allowed_attempts int: A number of attempts allowed for every interval_seconds.
        action str: A noun or noun phrase describing the action being guarded.
        enabled bool: A boolean controlling whether this facility is enabled. If False, waiting is disabled.
        log object: A logger object (supporting operations like .debug, .info, .warning, and .error).
        wait_hook: A hook not recommended for production, but intended for testing to know when waiting happens.

        """
        if not (isinstance(allowed_attempts, int) and allowed_attempts >= 1):
            raise TypeError("The allowed_attempts must be a positive integer: %s" % allowed_attempts)
        # This makes it easy to turn off the feature
        self.enabled = enabled
        self.interval_seconds = interval_seconds
        self.safety_seconds = safety_seconds
        self.allowed_attempts = allowed_attempts
        self.action = action
        self.timestamps = [self.EARLIEST_TIMESTAMP] * allowed_attempts
        self.log = log or logging
        self.wait_hook = wait_hook

    def set_wait_hook(self, wait_hook):
        """
        Use this to set the wait hook, which will be a function that notices we had to wait.

        Args:

        wait_hook: a function of two arguments (wait_seconds and next_expiration)
        """
        self.wait_hook = wait_hook

    def wait_if_needed(self):
        """
        This function is intended to be called immediately prior to each guarded operation.

        This function will wait (using time.sleep) only if necessary, and for the amount necessary,
        to comply with rate-limiting declared in the creation of this RateManager.

        NOTE WELL: It is presumed that all calls are coming from this source. This doesn't have ESP that would
        detect or otherwise accommodate externally generated calls, so violations of rate-limiting can still
        happen that way. This should be sufficient for sequential testing, and better than nothing for
        production operation.  This is not a substitute for responding to server-initiated throttling protocols.
        """
        now = datetime.datetime.now()
        expiration_delta = datetime.timedelta(seconds=self.interval_seconds)
        latest_expiration = now + expiration_delta
        soonest_expiration = latest_expiration
        # This initial value of soonest_expiration_pos is arbitrarily chosen, but it will normally be superseded.
        # The only case where it's not overridden is where there were no better values than the latest_expiration,
        # so if we wait that amount, all of the slots will be ready to be reused and we might as well use 0 as any.
        # -kmp 19-Jul-2020
        soonest_expiration_pos = 0
        for i, expiration_time in enumerate(self.timestamps):
            if expiration_time <= now:  # This slot was unused or has expired
                self.timestamps[i] = latest_expiration
                return
            elif expiration_time <= soonest_expiration:
                soonest_expiration = expiration_time
                soonest_expiration_pos = i
        sleep_time_needed = (soonest_expiration - now).total_seconds() + self.safety_seconds
        if self.enabled:
            if self.wait_hook:  # Hook primarily for testing
                self.wait_hook(wait_seconds=sleep_time_needed, next_expiration=soonest_expiration)
            self.log.warning("Waiting %s seconds before attempting %s." % (sleep_time_needed, self.action))
            time.sleep(sleep_time_needed)
        # It will have expired now, so grab that slot. We have to recompute the 'now' time because we slept in between.
        self.timestamps[soonest_expiration_pos] = datetime.datetime.now() + expiration_delta


def environ_bool(var, default=False):
    """
    Returns True if the named environment variable is set to 'true' (in any alphabetic case), False if something else.

    If the variable value is not set, the default is returned. False is the default default.
    This function is intended to allow boolean parameters to be initialized from environment variables.
    e.g.,
        DEBUG_FOO = environ_bool("FOO")
    or. if a special value is desired when the variable is not set:
        DEBUG_FOO = environ_bool("FOO", default=None)

    Args:
        var str: The name of an environment variable.
        default object: Any object.
    """
    if var not in os.environ:
        return default
    else:
        return str_to_bool(os.environ[var])


def str_to_bool(x: Optional[str]) -> Optional[bool]:
    if x is None:
        return None
    elif isinstance(x, str):
        return x.lower() == "true"
    else:
        raise ValueError(f"An argument to str_or_bool must be a string or None: {x!r}")


def to_integer(value: str, fallback: Optional[Any] = None) -> Optional[Any]:
    try:
        return int(value)
    except Exception:
        try:
            return int(float(value))
        except Exception:
            pass
    return fallback


def to_float(value: str, fallback: Optional[Any] = None) -> Optional[Any]:
    try:
        return float(value)
    except Exception:
        return fallback


def to_boolean(value: str, fallback: Optional[Any]) -> Optional[Any]:
    if isinstance(value, str) and (value := value.strip().lower()):
        if (lower_value := value.lower()) in ["true", "t"]:
            return True
        elif lower_value in ["false", "f"]:
            return False
    return fallback


def to_enum(value: str, enumerators: List[str]) -> Optional[str]:
    matches = []
    if isinstance(value, str) and (value := value.strip()) and isinstance(enumerators, List):
        enum_specifiers = {str(enum).lower(): enum for enum in enumerators}
        if (enum_value := enum_specifiers.get(lower_value := value.lower())) is not None:
            return enum_value
        for enum_canonical, _ in enum_specifiers.items():
            if enum_canonical.startswith(lower_value):
                matches.append(enum_canonical)
        if len(matches) == 1:
            return enum_specifiers[matches[0]]
    return enum_specifiers[matches[0]] if len(matches) == 1 else value


@contextlib.contextmanager
def override_environ(**overrides):
    """
    Overrides os.environ for the dynamic extent of the call, using the specified values.
    A value of None means to delete the property temporarily.
    (This uses override_dict to do the actual overriding. See notes for that function about lack of thread safety.)
    """
    with override_dict(os.environ, **overrides):
        yield


@contextlib.contextmanager
def override_dict(d, **overrides):
    """
    Overrides the given dictionary for the dynamic extent of the call, using the specified values.
    A value of None means to delete the property temporarily.

    This function is not threadsafe because it dynamically assigns and de-assigns parts of a dictionary.
    It should be reserved for use in test functions or command line tools or other contexts that are known
    to be single-threaded, or at least not competing for the resource of the dictionary. (It would be threadsafe
    to use a dictionary that is only owned by the current process.)
    """
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


def check_true(test_value, message, error_class=None):
    """
    If the first argument does not evaluate to a true value, an error is raised.

    The error, if one is raised, will be of type error_class, and its message will be given by message.
    The error_class defaults to RuntimeError, but may be any Exception class.
    """

    __tracebackhide__ = True

    if error_class is None:
        error_class = RuntimeError
    if not test_value:
        raise error_class(message)


def remove_element(elem, lst, raise_error=True):
    """
    Returns a shallow copy of the given list with the first occurrence of the given element removed.

    If the element doesn't occur in the list, an error is raised unless given raise_error=False,
    in which case a shallow copy of the original list is returned (with no elements removed).

    :param elem: an object
    :param lst: a list
    :param raise_error: a boolean (default True)
    """

    result = lst.copy()
    try:
        result.remove(elem)
    except ValueError:
        if raise_error:
            raise
    return result


def remove_prefix(prefix: str, text: str, required: bool = False):
    if not text.startswith(prefix):
        if required:
            raise ValueError('Prefix %s is not the initial substring of %s' % (prefix, text))
        else:
            return text
    return text[len(prefix):]


def remove_suffix(suffix: str, text: str, required: bool = False):
    if not text.endswith(suffix):
        if required:
            raise ValueError('Suffix %s is not the final substring of %s' % (suffix, text))
        else:
            return text
    return text[:len(text)-len(suffix)]


def remove_empty_properties(data: Optional[Union[list, dict]],
                            isempty: Optional[Callable] = None,
                            isempty_array_element: Optional[Callable] = None) -> None:
    def _isempty(value: Any) -> bool:  # noqa
        return isempty(value) if callable(isempty) else value in [None, "", {}, []]
    if isinstance(data, dict):
        for key in list(data.keys()):
            if _isempty(value := data[key]):
                del data[key]
            else:
                remove_empty_properties(value, isempty=isempty, isempty_array_element=isempty_array_element)
    elif isinstance(data, list):
        for item in data:
            remove_empty_properties(item, isempty=isempty, isempty_array_element=isempty_array_element)
        if callable(isempty_array_element):
            data[:] = [item for item in data if not isempty_array_element(item)]


class ObsoleteError(Exception):
    pass


def obsolete(func, fail=True):
    """ Decorator that allows you to mark methods as obsolete and raise an exception if called.
        You can also pass fail=False to the decorator to just emit an error log statement.
    """

    def inner(*args, **kwargs):
        if not fail:
            logging.error('Called obsolete function %s' % func.__name__)
            return func(*args, **kwargs)
        raise ObsoleteError('Tried to call function %s but it is marked as obsolete' % func.__name__)

    return inner


def ancestor_classes(cls, reverse=False):
    result = list(cls.__mro__[1:])
    if reverse:
        result.reverse()
    return result


def is_proper_subclass(cls, maybe_proper_superclass):
    """
    Returns true of its first argument is a subclass of the second argument, but is not that class itself.
    (Every class is a subclass of itself, but no class is a 'proper subclass' of itself.)
    """
    return cls is not maybe_proper_superclass and issubclass(cls, maybe_proper_superclass)


def full_class_name(obj):
    """
    Returns the fully-qualified name of the class of the given obj (an object).

    For built-in classes, just the class name is returned.
    For other classes, the class name with the module name prepended (separated by a dot) is returned.
    """

    # Source: https://stackoverflow.com/questions/2020014/get-fully-qualified-class-name-of-an-object-in-python
    return full_object_name(obj.__class__)


def full_object_name(obj):
    """
    Returns the fully-qualified name the given obj, if it has a name, or None otherwise.

    For built-in classes, just the class name is returned.
    For other objects, the name with the module name prepended (separated by a dot) is returned.
    If the object has no __module__ or __name__ attribute, None is returned.
    """

    try:
        module = obj.__module__
        if module is None or module == str.__class__.__module__:
            return obj.__name__  # Avoid reporting __builtin__
        else:
            return module + '.' + obj.__name__
    except Exception:
        return None


def constantly(value):
    def fn(*args, **kwargs):
        ignored(args, kwargs)
        return value
    return fn


def identity(x):
    """Returns its argument."""
    return x


def count_if(filter, seq):  # noQA - that's right, we're shadowing the built-in Python function 'filter'.
    return sum(1 for x in seq if filter(x))


def count(seq, filter=None):  # noQA - that's right, we're shadowing the built-in Python function 'filter'.
    return count_if(filter or identity, seq)


def find_associations(data, **kwargs):
    found = []
    for datum in data:
        mismatch = False
        for k, v in kwargs.items():
            defaulted_val = datum.get(k)
            if not (v(defaulted_val) if callable(v) else (v == defaulted_val)):
                mismatch = True
                break
        if not mismatch:
            found.append(datum)
    return found


def find_association(data, **kwargs):
    results = find_associations(data, **kwargs)
    n = len(results)
    if n == 0:
        return None
    elif n == 1:
        return results[0]
    else:
        raise ValueError("Got %s results when 1 was expected." % n)


def keys_and_values_to_dict(keys_and_values: list, key_name: str = "Key", value_name: str = "Value") -> dict:
    """
    Transforms the given list of key/value objects, each containing a "Key" and "Value" property,
    or alternately named via the key_name and/or value_name arguments, into a simple
    dictionary of keys/values, and returns this value. For example, given this:

      [
        { "Key": "env",
          "Value": "prod"
        },
        { "Key": "aws:cloudformation:stack-name",
          "Value": "c4-network-main-stack"
        }
      ]

    This function would return this:

      {
        "env": "prod",
        "aws:cloudformation:stack-name": "c4-network-main-stack"
      }

    :param keys_and_values: List of key/value objects as described above.
    :param key_name: Name of the given key property in the given list of key/value objects; default to "Key".
    :param value_name: Name of the given value property in the given list of key/value objects; default to "Value".
    :returns: Dictionary of keys/values from given list of key/value object as described above.
    :raises ValueError: if item in list does not contain key or value name; or on duplicate key name in list.
    """
    result = {}
    for item in keys_and_values:
        if key_name not in item:
            raise ValueError(f"Key {key_name} is not in {item}.")
        if value_name not in item:
            raise ValueError(f"Key {value_name} is not in {item}.")
        if item[key_name] in result:
            raise ValueError(f"Key {key_name} is duplicated in {keys_and_values}.")
        result[item[key_name]] = item[value_name]
    return result


def dict_to_keys_and_values(dictionary: dict, key_name: str = "Key", value_name: str = "Value") -> list:
    """
    Transforms the keys/values in the given dictionary to a list of key/value objects, each containing
    a "Key" and "Value" property, or alternately named via the key_name and/or value_name arguments,
    and returns this value. For example, given this:

      {
        "env": "prod",
        "aws:cloudformation:stack-name": "c4-network-main-stack"
      }

    This function would return this:

      [
        { "Key": "env",
          "Value": "prod"
        },
        { "Key": "aws:cloudformation:stack-name",
          "Value": "c4-network-main-stack"
        }
      ]

    :param dictionary: Dictionary of keys/values described above.
    :param key_name: Name of the given key property in the result list of key/value objects; default to "Key".
    :param value_name: Name of the given value property in the result list of key/value objects; default to "Value".
    :returns: List of key/value objects from the given dictionary as described above.
    """
    result = [{key_name: key, value_name: dictionary[key]} for key in dictionary]
    return result


def keyword_as_title(keyword):
    """
    Given a dictionary key or other token-like keyword, return a prettier form of it use as a display title.

    Underscores are replaced by spaces, but hyphens are not.
    It is assumed that underscores are word-separators but a hyphenated word is still a hyphenated word.

    Examples:

        >>> keyword_as_title('foo')
        'Foo'
        >>> keyword_as_title('some_text')
        'Some Text'
        >>> keyword_as_title('mary_smith-jones')
        'Mary Smith-Jones'

    :param keyword: a string to be used as a keyword, for example a dictionary key
    :return: a string to be used in a title: text in title case with underscores replaced by spaces.
    """

    return keyword.replace("_", " ").title()


def file_contents(filename, binary=False):
    with io.open(filename, 'rb' if binary else 'r') as fp:
        return fp.read()


def json_file_contents(filename):
    with io.open(filename, 'r') as fp:
        return json.load(fp)


def load_json_if(s: str, is_array: bool = False, is_object: bool = False,
                 fallback: Optional[Any] = None) -> Optional[Any]:
    if (isinstance(s, str) and
        ((is_object and s.startswith("{") and s.endswith("}")) or
         (is_array and s.startswith("[") and s.endswith("]")))):
        try:
            return json.loads(s)
        except Exception:
            pass
    return fallback


def camel_case_to_snake_case(s, separator='_'):
    """
    Converts CamelCase to snake_case.
    With a separator argument (default '_'), use that character instead for snake_case.
    e.g., with separator='-', you'll get snake-case.

    :param s: a string to convert
    :param separator: the snake-case separator character (default '_')
    """
    return ''.join(separator + c.lower() if c.isupper() else c for c in s).lstrip(separator)


def snake_case_to_camel_case(s, separator='_'):
    """
    Converts snake_case to CamelCase. (Note that "our" CamelCase always capitalizes the first character.)
    With a separator argument (default '_'), expect that character instead for snake_case.
    e.g., with separator='-', you'll expect snake-case.

    :param s: a string to convert
    :param separator: the snake-case separator character (default '_')
    """
    return s.title().replace(separator, '')


def to_camel_case(s):
    """
    Converts a string that might be in snake_case or CamelCase into CamelCase.
    """
    hyphen_found = False
    if '-' in s:
        hyphen_found = True
        s = s.replace('-', '_')
    if not hyphen_found and s[:1].isupper() and '_' not in s:
        return s
    else:
        return snake_case_to_camel_case(s)


def to_snake_case(s):
    """
    Converts a string that might be in snake_case or CamelCase into CamelCase.
    """
    return camel_case_to_snake_case(to_camel_case(s))


def capitalize1(s):
    """
    Capitalizes the first letter of a string and leaves the others alone.
    This is in contrast to the string's .capitalize() method, which would force the rest of the string to lowercase.
    """
    return s[:1].upper() + s[1:]


"""
Python's UUID ignores all dashes, whereas Postgres is more strict
http://www.postgresql.org/docs/9.2/static/datatype-uuid.html
See also http://www.postgresql.org/docs/9.2/static/datatype-uuid.html
And, anyway, this pattern is what our portals have been doing
for quite a while, so it's the most stable choice for us now.
"""

uuid_re = re.compile(r'(?i)[{]?(?:[0-9a-f]{4}-?){8}[}]?')


def is_uuid(instance):
    """
    Predicate returns true for any group of 32 hex characters with optional hyphens every four characters.
    We insist on lowercase to make matching faster. See other notes on this design choice above.
    """
    return bool(uuid_re.match(instance))


def string_list(s):
    """
    Turns a comma-separated list into an actual list, trimming whitespace and ignoring nulls.
        >>> string_list('foo, bar,,baz ')
        ['foo', 'bar', 'baz']
    """

    if not isinstance(s, str):
        raise ValueError(f"Not a string: {s!r}")
    return [p for p in [part.strip() for part in s.split(",")] if p]


def split_string(value: str, delimiter: str, escape: Optional[str] = None, unique: bool = False) -> List[str]:
    """
    Splits the given string into an array of string based on the given delimiter, and an optional escape character.
    If the given unique flag is True then duplicate values will not be included.
    """
    if not isinstance(value, str) or not (value := value.strip()):
        return []
    result = []
    if not isinstance(escape, str) or not escape:
        for item in value.split(delimiter):
            if (item := item.strip()) and (unique is not True or item not in result):
                result.append(item)
        return result
    item = r""
    escaped = False
    for c in value:
        if c == delimiter and not escaped:
            if (item := item.strip()) and (unique is not True or item not in result):
                result.append(item)
            item = r""
        elif c == escape and not escaped:
            escaped = True
        else:
            item += c
            escaped = False
    if (item := item.strip()) and (unique is not True or item not in result):
        result.append(item)
    return result


def right_trim(list_or_tuple: Union[List[Any], Tuple[Any]],
               remove: Optional[Callable] = None) -> Union[List[Any], Tuple[Any]]:
    """
    Removes training None (or emptry string) values from the give tuple or list arnd returns;
    does NOT change the given value.
    """
    i = len(list_or_tuple) - 1
    while i >= 0 and ((remove and remove(list_or_tuple[i])) or (not remove and list_or_tuple[i] in (None, ""))):
        i -= 1
    return list_or_tuple[:i + 1]


def create_dict(**kwargs) -> dict:
    result = {}
    for name in kwargs:
        if not (kwargs[name] is None):
            result[name] = kwargs[name]
    return result


def create_readonly_object(**kwargs):
    """
    Returns a new/unique object instance with readonly properties equal to the give kwargs.
    """
    readonly_class_name = "readonlyclass_" + str(uuid.uuid4()).replace("-", "")
    readonly_class_args = " ".join(kwargs.keys())
    readonly_class = namedtuple(readonly_class_name, readonly_class_args)
    readonly_object = readonly_class(**kwargs)
    return readonly_object


def is_c4_arn(arn: str) -> bool:
    """
    Returns True iff the given (presumed) AWS ARN string value looks like it
    refers to a CGAP or Fourfront Cloudformation entity, otherwise returns False.
    :param arn: String representing an AWS ARN.
    :return: True if the given ARN refers to a CGAP or Fourfront Cloudformation entity, else False.
    """
    pattern = r"(fourfront|cgap|[:/]c4-)"
    return True if re.search(pattern, arn) else False


def string_md5(unicode_string):
    """
    Returns the md5 signature for the given u unicode string.
    """
    return hashlib.md5(unicode_string.encode('utf-8')).hexdigest()


class CachedField:
    def __init__(self, name, update_function, timeout=600):
        """ Provides a named field that is cached for a certain period of time. The value is computed
            on calls to __init__, after which the get() method should be used.

        :param name: name of property
        :param update_function: lambda to be invoked to update the value
        :param timeout: TTL of this field, in seconds
        """
        self.name = name
        self._update_function = update_function
        self.timeout = timeout
        self.value = update_function()
        self.time_of_next_update = datetime.datetime.utcnow() + datetime.timedelta(seconds=timeout)

    def _update_timestamp(self):
        self.time_of_next_update = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.timeout)

    def _update_value(self):
        self.value = self._update_function()
        self._update_timestamp()

    def get(self):
        """ Intended for normal use - to get the value subject to the given TTL on creation. """
        now = datetime.datetime.utcnow()
        if now > self.time_of_next_update:
            self._update_value()
        return self.value

    def get_updated(self, push_ttl=False):
        """ Intended to force an update to the value and potentially push back the timeout from now. """
        self.value = self._update_function()
        if push_ttl:
            self.time_of_next_update = datetime.datetime.utcnow() + datetime.timedelta(seconds=self.timeout)
        return self.value

    def set_timeout(self, new_timeout):
        """ Sets a new value for timeout and restarts the timeout counter."""
        self.timeout = new_timeout
        self._update_timestamp()

    def __repr__(self):
        return str(self)

    def __str__(self):
        updater_name = self._update_function.__name__
        return f"CachedField(name={self.name!r},update_function={updater_name},timeout={self.timeout!r})"


class StorageCell:

    def __init__(self, initial_value=None):
        self.value = initial_value


def make_counter(start=0, step=1):
    """
    Creates a counter that generates values counting from a given start (default 0) by a given step (default 1).
    """
    storage = StorageCell(start)

    def counter():
        old_value = storage.value
        storage.value += step
        return old_value

    return counter


def copy_json(obj):
    """ This function is taken and renamed from ENCODE's snovault quick_deepcopy

    Deep copy an object consisting of dicts, lists, and primitives.
    This is faster than Python's `copy.deepcopy` because it doesn't
    do bookkeeping to avoid duplicating objects in a cyclic graph.
    This is intended to work fine for data deserialized from JSON,
    but won't work for everything.
    """
    if isinstance(obj, dict):
        obj = {k: copy_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        obj = [copy_json(v) for v in obj]
    return obj


class UncustomizedInstance(Exception):
    """
    Reports a helpful error for access to a CustomizableProperty that has not been properly set.
    """

    def __init__(self, instance, *, field):
        self.instance = instance
        self.field = field
        declaration_class, declaration = self._find_field_declared_class(instance, field)
        self.declaration_class = declaration_class
        context = ""
        if declaration_class == 'instance':
            context = " from instance"
        elif declaration_class:
            context = " from class %s" % full_object_name(declaration_class)
        message = ("Attempt to access field %s%s."
                   " It was expected to be given a custom value in a subclass: %s."
                   % (field, context, declaration.description))
        super().__init__(message)

    @staticmethod
    def _find_field_declared_class(instance, field):
        instance_value = instance.__dict__.get(field)
        is_class = isinstance(instance, type)
        if instance_value:
            return instance if is_class else 'instance', instance_value
        else:
            for cls in instance.__mro__ if is_class else instance.__class__.__mro__:
                cls_value = cls.__dict__.get(field)
                if cls_value:
                    return cls, cls_value
            raise RuntimeError("%s does not have a field %s." % (instance, field))


class CustomizableProperty(property):
    """
    Declares a class variable to require customization. See help on getattr_customized for details.
    """

    def __init__(self, field, *, description):

        self.field = field
        self.description = description

        def uncustomized(instance):
            raise UncustomizedInstance(instance=instance, field=field)

        super().__init__(uncustomized)

    def __str__(self):
        return "<%s %s>" % (self.__class__.__name__, self.field)


def getattr_customized(thing, key):
    """
    Like getattr, but if the value is a CustomizableProperty, gives a helpful error explaining need to customize.

    This avoids inscrutible errors or even dangerous confusions that happen when an abstract class requires setting
    of variables that the user of the class might forget to set or not realize they're supposed to set.

    So, for example, one might write:

    class AbstractFileClass:
        ALLOW_SOFT_DELETE = CustomizableProperty('ALLOW_SOFT_DELETE',
                                                 description='a boolean saying whether soft delete is allowed')
        def delete(self, file):
            if getattr_customized(cls, 'ALLOW_SOFT_DELETE'):
                self.soft_delete(file)
            else:
                self.hard_delete(file)

    Note that there may not be a reasonable default for ALLOW_SOFT_DELETE. Any value would be taken as a boolean.
    It would be possible to leave the variable unset, but then linters would complain about referring to it.
    And it would be confusing if accessed without setting it.
    """
    # This will raise an error if the attribute is a CustomizableProperty living in the class part of the dict,
    # but will return the object if it's in the instance.
    value = getattr(thing, key)
    if isinstance(value, CustomizableProperty):
        # This is an uncustomized instance variable, not a class variable.
        # That's not an intended use case, but just report it without involving mention of the class.
        raise UncustomizedInstance(instance=thing, field=key)
    else:
        return value


def url_path_join(*fragments):
    """
    Concatenates its arguments, returning a string with exactly one slash ('/') separating each of the path fragments.

    So, whether the path_fragments are ('foo', 'bar') or ('foo/', 'bar') or ('foo', '/bar') or ('foo/', '/bar')
    or even ('foo//', '///bar'), the result will be 'foo/bar'. The left side of the first thing and the
    right side of the last thing are unaffected.

    :param fragments: a list of URL path fragments
    :return: a slash-separated concatentation of the given path fragments
    """
    fragments = fragments or ("",)
    result = fragments[0]  # Tolerate an empty list
    for thing in fragments[1:]:
        result = result.rstrip("/") + "/" + thing.lstrip("/")
    return result


def _is_function_of_exactly_one_required_arg(x):
    if not callable(x):
        return False
    argspec = inspect.getfullargspec(x)
    return len(argspec.args) == 1 and not argspec.varargs and not argspec.defaults and not argspec.kwonlyargs


def _apply_decorator(fn, *args, **kwargs):
    """
    This implements a fix to the decorator syntax where it gets fussy about whether @foo and @foo() are synonyms.
    The price to be paid is you can't use it for decorators that take positional arguments.
    """
    if args and (kwargs or len(args) > 1):
        # Case 1
        # If both args and kwargs are in play, they have to have been passed explicitly like @foo(a1, k2=v2).
        # If more than one positional is given, that has to be something like @foo(a1, a2, ...)
        # Decorators using this function need to agree to only accept keyword arguments, so those cases can't happen.
        # They can do this by using an optional first positional argument, as in 'def foo(x=3):',
        # or they can do it by using a * as in 'def foo(*, x)' or if no arguments are desired, obviously, 'def foo():'.
        raise SyntaxError("Positional arguments to decorator (@%s) not allowed here." % fn.__name__)
    elif args:  # i.e., there is 1 positional arg (an no keys)
        # Case 2
        arg0 = args[0]  # At this point, we know there is a single positional argument.
        #
        # Here there are two cases.
        #
        # (a) The user may have done @foo, in which case we will have a fn which is the value of foo,
        #     but not the result of applying it.
        #
        # (b) Otherwise, the user has done @foo(), in which case what we'll have the function of one
        #     argument that does the wrapping of the subsequent function or class.
        #
        # So since case (a) expects fn to be a function that tolerates zero arguments
        # while case (b) expects fn to be a function that rejects positional arguments,
        # we can call fn with the positional argument, arg0. If that argument is rejected with a TypeError,
        # we know that it's really case (a) and that we need to call fn once with no arguments
        # before retrying on arg0.
        if _is_function_of_exactly_one_required_arg(fn):
            # Case 2A
            # We are ready to wrap the function or class in arg0
            return fn(arg0)
        else:
            # Case 2B
            # We are ALMOST ready to wrap the function or class in arg0,
            # but first we have to call ourselves with no arguments as in case (a) described above.
            return fn()(arg0)
    else:
        # Case 3
        # Here we have kwargs = {...} from @foo(x=3, y=4, ...) or maybe no kwargs either @foo().
        # Either way, we've already evaluated the foo(...) call, so all that remains is to call on our kwargs.
        # (There are no args to call it on because we tested that above.)
        return fn(**kwargs)


def _decorator(decorator_function):
    """See documentation for decorator."""
    @functools.wraps(decorator_function)
    def _wrap_decorator(*args, **kwargs):
        return _apply_decorator(decorator_function, *args, **kwargs)
    return _wrap_decorator


@_decorator
def decorator():
    """
    This defines a decorator, such that is can be used as either @foo or @foo()
    PROVIDED THAT the function doing the decorating is not a function a single required argument,
    since that would create an ambiguity that would inhibit the auto-correction this will do.

    @decorator
    def foo(...):
        ...
    """
    return _decorator


def dict_zip(dict1, dict2):
    """
    This is like the zip operator that zips two lists, but it takes two dictionaries and pairs matching elements.
    e.g.,

        >>> dict_zip({'a': 'one', 'b': 'two'}, {'a': 1, 'b': 2})
        [('one', 1), ('two', 2)]

    In Python 3.6+, the order of the result list is the same as the order of the keys in the first dict.
    If the two dictionaries do not have exactly the same set of keys, an error will be raised.
    """
    res = []
    for key1 in dict1:
        if key1 not in dict2:
            raise ValueError(f"Key {key1!r} is in dict1, but not dict2."
                             f" dict1.keys()={list(dict1.keys())}"
                             f" dict2.keys()={list(dict2.keys())}")

        res.append((dict1[key1], dict2[key1]))
    for key2 in dict2:
        if key2 not in dict1:
            raise ValueError(f"Key {key2!r} is in dict1, but not dict2."
                             f" dict1.keys()={list(dict1.keys())}"
                             f" dict2.keys()={list(dict2.keys())}")
    return res


def json_leaf_subst(exp, substitutions):
    """
    Given an expression and some substitutions, substitutes all occurrences of the given substitutions.
    For example:

    >>> json_leaf_subst({'xxx': ['foo', 'bar', 'baz']}, {'foo': 'fu', 'bar': 'bah', 'xxx': 'yyy'})
    {'yyy': ['fu', 'bah', 'baz']}

    :param exp: a JSON expression, represented in Python as a string, a number, a list, or a dict
    :param substitutions: a dictionary of replacements from keys to values
    """
    def do_subst(e):
        return json_leaf_subst(e, substitutions)
    if isinstance(exp, dict):
        return {do_subst(k): do_subst(v) for k, v in exp.items()}
    elif isinstance(exp, list):
        return [do_subst(e) for e in exp]
    elif exp in substitutions:  # Something atomic like a string or number
        return substitutions[exp]
    return exp


class SingletonManager:

    def __init__(self, singleton_class, *singleton_args, **singleton_kwargs):
        self._singleton = None
        self._singleton_class = singleton_class
        self._singleton_args = singleton_args
        self._singleton_kwargs = singleton_kwargs

    @property
    def singleton(self):
        if not self._singleton:
            self._singleton = self._singleton_class(*self._singleton_args or (), **self._singleton_kwargs or {})
        return self._singleton

    @property
    def singleton_class(self):
        return self._singleton_class


class classproperty(object):
    """
    This decorator is like 'classproperty', but the function is run only on first use, not every time, and then cached.

    Example:

        import time
        class Clock:
            @classproperty
            def sample():
                return time.time()

        # Different results each time, just like an instance method, but without having to instantiate the class.
        Clock.sample
        1665600812.008385
        Clock.sample
        1665600812.760394
    """

    def __init__(self, getter):
        self.getter = getter

    def __get__(self, instance, instance_class):
        ignored(instance)
        return self.getter(instance_class)


class managed_property(object):
    """
    Sample use:

        class Temperature:

            def __init__(self, fahrenheit=32):
                self.fahrenheit = fahrenheit

            @managed_property
            def centigrade(self, degrees):
                if degrees == managed_property.MISSING:
                    return (self.fahrenheit - 32) * 5 / 9.0
                else:
                    self.fahrenheit = degrees * 9 / 5.0 + 32

        t1 = Temperature()
        assert t1.fahrenheit == 32
        assert t1.centigrade == 0.0

        t2 = Temperature(fahrenheit=68)
        assert t2.fahrenheit == 68.0
        assert t2.centigrade == 20.0
        t2.centigrade = 5
        assert t2.centigrade == 5.0
        assert t2.fahrenheit == 41.0

        t2.fahrenheit = -40
        assert t2.centigrade == -40.0

    """

    MISSING = NamedObject("missing")

    def __init__(self, handler):

        self.handler = handler

    def __get__(self, instance, instance_class):
        ignored(instance_class)
        return self.handler(instance, self.MISSING)

    def __set__(self, instance, value):
        self.handler(instance, value)


class classproperty_cached(object):
    """
    This decorator is like 'classproperty', but the function is run only on first use, not every time, and then cached.

    Such a property returns the same value each time, just like any class property,
    but initialization is delayed until first use and determined by a call to the decorated function.

    Example:

        import time
        class Freeze:
            @classproperty_cached
            def sample():
                return time.time()

        Freeze.sample
        1665600374.4801269
        Freeze.sample
        1665600374.4801269

        class SubFreeze(Freeze):
            pass

        SubFreeze.sample
        1665600540.1467211
        SubFreeze.sample
        1665600540.1467211

        Freeze.sample
        1665600374.4801269

        SubFreeze.sample
        1665600540.1467211
    """

    ATTRIBUTE_CACHE_MAP = defaultdict(lambda: {})

    _USES_PER_SUBCLASS_CACHES = False

    def __init__(self, initializer):
        self.initializer = initializer
        self.name = initializer.__name__
        self.attribute_cache = {}

    def __get__(self, instance, instance_class):
        ignored(instance)
        reference_class = self._find_reference_class(instance_class, self.name)
        if reference_class not in self.attribute_cache:  # If there is no exact class matching, init it.
            initial_value = self.initializer(reference_class)
            self.attribute_cache[reference_class] = initial_value
        return self.attribute_cache[reference_class]

    @classmethod
    def _find_reference_class(cls, instance_class, attribute_name):
        if cls._USES_PER_SUBCLASS_CACHES:
            return instance_class
        else:
            return cls._find_cache_class(instance_class, attribute_name)

    @classmethod
    def _find_cache_class_and_attribute(cls, instance_class, attribute_name):
        for superclass in instance_class.__mro__:
            superclass_attributes = superclass.__dict__
            if attribute_name in superclass_attributes:
                attribute_value = superclass_attributes[attribute_name]
                if isinstance(attribute_value, cls):
                    return superclass, attribute_value
                raise ValueError(f"The slot {instance_class.__name__}.{attribute_name}"
                                 f" does not contain a cached value.")
        raise ValueError(f"The slot {instance_class.__name__}.{attribute_name} is not defined.")

    @classmethod
    def _find_cache_class(cls, instance_class, attribute_name):
        return cls._find_cache_class_and_attribute(instance_class, attribute_name)[0]

    @classmethod
    def _find_cache_attribute(cls, instance_class, attribute_name):
        return cls._find_cache_class_and_attribute(instance_class, attribute_name)[1]

    @classmethod
    def _find_cache_map(cls, instance_class, attribute_name) -> dict:
        attribute = cls._find_cache_attribute(instance_class, attribute_name)
        return attribute.attribute_cache

    @classmethod
    def reset(cls, *, instance_class, attribute_name, subclasses=True):
        """
        Clears the cache for the given attribute name on the given instance_class.

        Having done:

            import random
            class Foo:
                @classproperty_cached
                def something(cls):
                    return random.randint(100)

        one can clear the cache by doing:

            classproperty_cached.reset(instance_class=Foo, attribute_name='something')

        :param instance_class: a class with an attribute whose value is managed by '@classproperty_cached'.
        :param attribute_name: the name of the attribute that has a cached value.
        :param subclasses: a bool indicating whether the cache reset requests applies to
             all subclasses (subclasses=True) or only the exact class given (subclasses=False).
             Using subclasses=False is not allowed if CACHE_EACH_SUBCLASS is False.
        """

        if not cls._USES_PER_SUBCLASS_CACHES and not subclasses:
            raise ValueError(f"The subclasses= argument to {cls.__name__}.reset must not be False"
                             f" because {cls.__name__} does not use per-subclass caches.")
        if not isinstance(instance_class, type):
            raise ValueError(f"The instance_class= argument to {cls.__name__}.reset must be a class.")
        reference_class = cls._find_reference_class(instance_class, attribute_name)
        attribute_cache = cls._find_cache_map(reference_class, attribute_name)
        keys_to_remove = []
        predicate = issubclass if subclasses else equals
        for cache_class, cache_value in attribute_cache.items():
            if predicate(cache_class, reference_class):
                keys_to_remove.append(cache_class)
        for cache_class in keys_to_remove:
            del attribute_cache[cache_class]
        return bool(keys_to_remove)


class classproperty_cached_each_subclass(classproperty_cached):
    """
    This decorator is like 'classproperty_cached', but a separate cache is maintained per-subclass.

    Such a property returns the same value each time, just like any class property,
    but initialization is delayed until first use and determined by a call to the decorated function.

    Example:

        import time
        class Freeze:
            @classproperty_cached
            def sample():
                return time.time()

        Freeze.sample
        1665600374.4801269
        Freeze.sample
        1665600374.4801269

        class SubFreeze(Freeze):
            pass

        SubFreeze.sample
        1665600540.1467211
        SubFreeze.sample
        1665600540.1467211

        Freeze.sample
        1665600374.4801269

        SubFreeze.sample
        1665600540.1467211
    """

    _USES_PER_SUBCLASS_CACHES = True


def equals(x, y):
    """
    A functional form of the '==' equality predicate so that it can be handled as a functional value.
    """
    return x == y


class Singleton:
    """
    A class witn a cached class property 'singleton' that holds an instance of the class (created with no arguments).

    The .singleton instance is created on demand (so will not be created at all if .singleton is never accessed).

    Example:

        class Foo(Singleton):
            pass

        # Regular instantiation of the class works like normal, giving a new class each time.
        Foo()
        <__main__.Foo object at 0x10e8aed90>
        Foo()
        <__main__.Foo object at 0x10e8b0150>

        # The .singleton property gives the same instance every time.
        Foo.singleton
        <__main__.Foo object at 0x10e8aefd0>
        Foo.singleton
        <__main__.Foo object at 0x10e8aefd0>
    """

    @classproperty_cached_each_subclass
    def singleton(cls):  # noQA - PyCharm wrongly thinks the argname should be 'self'
        return cls()     # noQA - PyCharm flags a bogus warning for this


def key_value_dict(key, value):
    """
    Given a key k and a value v, returns the dictionary {"Key": k, "Value": v}, which is a format AWS is fond of.
    """
    return {'Key': key, 'Value': value}


def merge_key_value_dict_lists(x, y):
    """
    Merges two lists of the form [{"Key": k1, "Value": v1}, {"Key": k2, "Value": v2}].
    It is assumed that neither list has repeated keys, and that the resulting list also should not.
    Entries in the resulting list will be in the same order as the original two lists except when both
    lists contain entries that share a key. In that case, the two entries will be merged
    into a single entry with the position of the first such entry and value of the second.
    """
    merged = {}
    for pair in x:
        merged[pair['Key']] = pair['Value']
    for pair in y:
        merged[pair['Key']] = pair['Value']
    return [key_value_dict(k, v) for k, v in merged.items()]


def merge_objects(target: Union[dict, List[Any]], source: Union[dict, List[Any]], full: bool = False) -> dict:
    """
    Merges the given source dictionary or list into the target dictionary or list.
    This MAY well change the given target (dictionary or list) IN PLACE.
    The the full argument is True then any target lists longer than the
    source be will be filled out with the last element(s) of the source.
    """
    if target is None:
        return source
    if isinstance(target, dict) and isinstance(source, dict) and source:
        for key, value in source.items():
            target[key] = merge_objects(target[key], value, full) if key in target else value
    elif isinstance(target, list) and isinstance(source, list) and source:
        for i in range(max(len(source), len(target))):
            if i < len(target):
                if i < len(source):
                    target[i] = merge_objects(target[i], source[i], full)
                elif full:
                    target[i] = merge_objects(target[i], source[len(source) - 1], full)
            else:
                target.append(source[i])
    elif source:
        target = source
    return target


def load_json_from_file_expanding_environment_variables(file: str) -> Union[dict, list]:
    def expand_envronment_variables(data):  # noqa
        if isinstance(data, dict):
            return {key: expand_envronment_variables(value) for key, value in data.items()}
        if isinstance(data, list):
            return [expand_envronment_variables(element) for element in data]
        if isinstance(data, str):
            return re.sub(r"\$\{([^}]+)\}|\$([a-zA-Z_][a-zA-Z0-9_]*)",
                          lambda match: os.environ.get(match.group(1) or match.group(2), match.group(0)), data)
        return data
    with open(file, "r") as file:
        return expand_envronment_variables(json.load(file))


# Stealing topological sort classes below from python's graphlib module introduced
# in v3.9 with minor refactoring.
# Source: https://github.com/python/cpython/blob/3.11/Lib/graphlib.py
# Docs: https://docs.python.org/3.11/library/graphlib.html
# TODO: Remove once python version >= 3.9


class _NodeInfo:
    __slots__ = "node", "npredecessors", "successors"

    def __init__(self, node):
        # The node this class is augmenting.
        self.node = node

        # Number of predecessors, generally >= 0. When this value falls to 0,
        # and is returned by get_ready(), this is set to _NODE_OUT and when the
        # node is marked done by a call to done(), set to _NODE_DONE.
        self.npredecessors = 0

        # List of successor nodes. The list can contain duplicated elements as
        # long as they're all reflected in the successor's npredecessors attribute.
        self.successors = []


class CycleError(ValueError):
    """Subclass of ValueError raised by TopologicalSorter.prepare if cycles
    exist in the working graph.
    If multiple cycles exist, only one undefined choice among them will be reported
    and included in the exception. The detected cycle can be accessed via the second
    element in the *args* attribute of the exception instance and consists in a list
    of nodes, such that each node is, in the graph, an immediate predecessor of the
    next node in the list. In the reported list, the first and the last node will be
    the same, to make it clear that it is cyclic.
    """
    pass


class TopologicalSorter:
    """Provides functionality to topologically sort a graph of hashable nodes"""

    _NODE_OUT = -1
    _NODE_DONE = -2

    def __init__(self, graph=None):
        self._node2info = {}
        self._ready_nodes = None
        self._npassedout = 0
        self._nfinished = 0

        if graph is not None:
            for node, predecessors in graph.items():
                self.add(node, *predecessors)

    def _get_nodeinfo(self, node):
        result = self._node2info.get(node)
        if result is None:
            self._node2info[node] = result = _NodeInfo(node)
        return result

    def add(self, node, *predecessors):
        """Add a new node and its predecessors to the graph.
        Both the *node* and all elements in *predecessors* must be hashable.
        If called multiple times with the same node argument, the set of dependencies
        will be the union of all dependencies passed in.
        It is possible to add a node with no dependencies (*predecessors* is not provided)
        as well as provide a dependency twice. If a node that has not been provided before
        is included among *predecessors* it will be automatically added to the graph with
        no predecessors of its own.
        Raises ValueError if called after "prepare".
        """
        if self._ready_nodes is not None:
            raise ValueError("Nodes cannot be added after a call to prepare()")

        # Create the node -> predecessor edges
        nodeinfo = self._get_nodeinfo(node)
        nodeinfo.npredecessors += len(predecessors)

        # Create the predecessor -> node edges
        for pred in predecessors:
            pred_info = self._get_nodeinfo(pred)
            pred_info.successors.append(node)

    def prepare(self):
        """Mark the graph as finished and check for cycles in the graph.
        If any cycle is detected, "CycleError" will be raised, but "get_ready" can
        still be used to obtain as many nodes as possible until cycles block more
        progress. After a call to this function, the graph cannot be modified and
        therefore no more nodes can be added using "add".
        """
        if self._ready_nodes is not None:
            raise ValueError("cannot prepare() more than once")

        self._ready_nodes = [
            i.node for i in self._node2info.values() if i.npredecessors == 0
        ]
        # ready_nodes is set before we look for cycles on purpose:
        # if the user wants to catch the CycleError, that's fine,
        # they can continue using the instance to grab as many
        # nodes as possible before cycles block more progress
        cycle = self._find_cycle()
        if cycle:
            raise CycleError(f"nodes are in a cycle", cycle)

    def get_ready(self):
        """Return a tuple of all the nodes that are ready.
        Initially it returns all nodes with no predecessors; once those are marked
        as processed by calling "done", further calls will return all new nodes that
        have all their predecessors already processed. Once no more progress can be made,
        empty tuples are returned.
        Raises ValueError if called without calling "prepare" previously.
        """
        if self._ready_nodes is None:
            raise ValueError("prepare() must be called first")

        # Get the nodes that are ready and mark them
        result = tuple(self._ready_nodes)
        n2i = self._node2info
        for node in result:
            n2i[node].npredecessors = self._NODE_OUT

        # Clean the list of nodes that are ready and update
        # the counter of nodes that we have returned.
        self._ready_nodes.clear()
        self._npassedout += len(result)

        return result

    def is_active(self):
        """Return ``True`` if more progress can be made and ``False`` otherwise.
        Progress can be made if cycles do not block the resolution and either there
        are still nodes ready that haven't yet been returned by "get_ready" or the
        number of nodes marked "done" is less than the number that have been returned
        by "get_ready".
        Raises ValueError if called without calling "prepare" previously.
        """
        if self._ready_nodes is None:
            raise ValueError("prepare() must be called first")
        return self._nfinished < self._npassedout or bool(self._ready_nodes)

    def __bool__(self):
        return self.is_active()

    def done(self, *nodes):
        """Marks a set of nodes returned by "get_ready" as processed.
        This method unblocks any successor of each node in *nodes* for being returned
        in the future by a call to "get_ready".
        Raises :exec:`ValueError` if any node in *nodes* has already been marked as
        processed by a previous call to this method, if a node was not added to the
        graph by using "add" or if called without calling "prepare" previously or if
        node has not yet been returned by "get_ready".
        """

        if self._ready_nodes is None:
            raise ValueError("prepare() must be called first")

        n2i = self._node2info

        for node in nodes:

            # Check if we know about this node (it was added previously using add()
            nodeinfo = n2i.get(node)
            if nodeinfo is None:
                raise ValueError(f"node {node!r} was not added using add()")

            # If the node has not being returned (marked as ready) previously, inform the user.
            stat = nodeinfo.npredecessors
            if stat != self._NODE_OUT:
                if stat >= 0:
                    raise ValueError(
                        f"node {node!r} was not passed out (still not ready)"
                    )
                elif stat == self._NODE_DONE:
                    raise ValueError(f"node {node!r} was already marked done")
                else:
                    assert False, f"node {node!r}: unknown status {stat}"

            # Mark the node as processed
            nodeinfo.npredecessors = self._NODE_DONE

            # Go to all the successors and reduce the number of predecessors, collecting all the ones
            # that are ready to be returned in the next get_ready() call.
            for successor in nodeinfo.successors:
                successor_info = n2i[successor]
                successor_info.npredecessors -= 1
                if successor_info.npredecessors == 0:
                    self._ready_nodes.append(successor)
            self._nfinished += 1

    def _find_cycle(self):
        n2i = self._node2info
        stack = []
        itstack = []
        seen = set()
        node2stacki = {}

        for node in n2i:
            if node in seen:
                continue

            while True:
                if node in seen:
                    # If we have seen already the node and is in the
                    # current stack we have found a cycle.
                    if node in node2stacki:
                        return stack[node2stacki[node]:] + [node]
                    # else go on to get next successor
                else:
                    seen.add(node)
                    itstack.append(iter(n2i[node].successors).__next__)
                    node2stacki[node] = len(stack)
                    stack.append(node)

                # Backtrack to the topmost stack entry with
                # at least another successor.
                while stack:
                    try:
                        node = itstack[-1]()
                        break
                    except StopIteration:
                        del node2stacki[stack.pop()]
                        itstack.pop()
                else:
                    break
        return None

    def static_order(self):
        """Returns an iterable of nodes in a topological order.
        The particular order that is returned may depend on the specific
        order in which the items were inserted in the graph.
        Using this method does not require to call "prepare" or "done". If any
        cycle is detected, :exc:`CycleError` will be raised.
        """
        self.prepare()
        while self.is_active():
            node_group = self.get_ready()
            yield from node_group
            self.done(*node_group)


def deduplicate_list(lst):
    """ De-duplicates the given list by converting it to a set then back to a list.
    NOTES:
    * The list must contain 'hashable' type elements that can be used in sets.
    * The result list might not be ordered the same as the input list.
    * This will also take tuples as input, though the result will be a list.
    :param lst: list to de-duplicate
    :return: de-duplicated list
    """
    return list(set(lst))


def chunked(seq, *, chunk_size=1):
    if not isinstance(chunk_size, int) or chunk_size < 1:
        raise ValueError(f"The chunk_size, {chunk_size}, must be a positive integer.")
    chunk = []
    i = 0
    for item in seq:
        chunk.append(item)
        i = (i + 1) % chunk_size
        if i == 0:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def map_chunked(fn, seq, *, chunk_size=1, reduce=None):
    result = (fn(chunk) for chunk in chunked(seq, chunk_size=chunk_size))
    return reduce(result) if reduce is not None else result


_36_DIGITS = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"


def format_in_radix(n: int, *, radix: int):

    if not isinstance(n, int) or n < 0:
        raise ValueError(f"Expected n to be a non-negative integer {n}")
    if radix < 2 or radix > 36:
        raise ValueError(f"Expected radix to be an integer between 2 and 36, inclusive: {radix}")
    buffer = []
    while n > 0:
        quo = n // radix
        rem = n % radix
        buffer += _36_DIGITS[rem]
        n = quo
    buffer.reverse()
    return "".join(buffer) or "0"


def parse_in_radix(text: str, *, radix: int):
    if not (text and isinstance(text, str)):
        raise ValueError(f"Expected a string to parse: {text}")
    if radix < 2 or radix > 36:
        raise ValueError(f"Expected radix to be an integer between 2 and 36, inclusive: {radix}")
    res = 0
    try:
        for c in text:
            res = res * radix + _36_DIGITS.index(c.upper())
        return res
    except Exception:
        pass
    raise ValueError(f"Unable to parse: {text!r}")


def pad_to(target_size: int, data: list, *, padding=None):
    """
    This will pad to a given target size, a list of a potentially different actual size, using given padding.
    e.g., pad_to(3, [1, 2]) will return [1, 2, None]
    """
    actual_size = len(data)
    if actual_size < target_size:
        data = data + [padding] * (target_size - actual_size)
    return data


def normalize_spaces(value: str) -> str:
    """
    Returns the given string with multiple consecutive occurrences of whitespace
    converted to a single space, and left and right trimmed of spaces.
    """
    return re.sub(r"\s+", " ", value).strip()


def normalize_string(value: Optional[str]) -> Optional[str]:
    """
    Strips leading/trailing spaces, and converts multiple consecutive spaces to a single space
    in the given string value and returns the result. If the given value is None returns an
    empty string. If the given value is not actually even a string then return None.
    """
    if value is None:
        return ""
    elif isinstance(value, str):
        return re.sub(r"\s+", " ", value).strip()
    return None


def find_nth_from_end(string: str, substring: str, nth: int) -> int:
    """
    Returns the index of the nth occurrence of the given substring within
    the given string from the END of the given string; or -1 if not found.
    """
    index = -1
    string = string[::-1]
    for i in range(0, nth):
        index = string.find(substring, index + 1)
    return len(string) - index - 1 if index >= 0 else -1


def set_nth(string: str, nth: int, replacement: str) -> str:
    """
    Sets the nth character of the given string to the given replacement string.
    """
    if not isinstance(string, str) or not isinstance(nth, int) or not isinstance(replacement, str):
        return string
    if nth < 0:
        nth += len(string)
    return string[:nth] + replacement + string[nth + 1:] if 0 <= nth < len(string) else string


def format_size(nbytes: Union[int, float], precision: int = 2, nospace: bool = False, terse: bool = False) -> str:
    if isinstance(nbytes, str) and nbytes.isdigit():
        nbytes = int(nbytes)
    elif not isinstance(nbytes, (int, float)):
        return ""
    UNITS = ['bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB']
    UNITS_TERSE = ['b', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y']
    MAX_UNITS_INDEX = len(UNITS) - 1
    ONE_K = 1024
    index = 0
    if (precision := max(precision, 0)) and (nbytes <= ONE_K):
        precision -= 1
    while abs(nbytes) >= ONE_K and index < MAX_UNITS_INDEX:
        nbytes /= ONE_K
        index += 1
    if index == 0:
        nbytes = int(nbytes)
        return f"{nbytes} byte{'s' if nbytes != 1 else ''}"
    unit = (UNITS_TERSE if terse else UNITS)[index]
    size = f"{nbytes:.{precision}f}"
    if size.endswith(f".{'0' * precision}"):
        # Tidy up extraneous zeros.
        size = size[:-(precision - 1)]
    return f"{size}{'' if nospace else ' '}{unit}"


def format_duration(seconds: Union[int, float]) -> str:
    seconds_actual = seconds
    seconds = round(max(seconds, 0))
    durations = [("year", 31536000), ("day", 86400), ("hour", 3600), ("minute", 60), ("second", 1)]
    parts = []
    for name, duration in durations:
        if seconds >= duration:
            count = seconds // duration
            seconds %= duration
            if count != 1:
                name += "s"
            parts.append(f"{count} {name}")
    if len(parts) == 0:
        return f"{seconds_actual:.1f} seconds"
    elif len(parts) == 1:
        return f"{seconds_actual:.1f} seconds"
    else:
        return " ".join(parts[:-1]) + " " + parts[-1]


class JsonLinesReader:

    def __init__(self, fp, padded=False, padding=None):
        """
        Given an fp (the conventional name for a "file pointer", the thing a call to io.open returns,
        this creates an object that can be used to iterate across the lines in the JSON lines file
        that the fp is reading from.

        There are two possible formats that this will return.

        For files that contain a series of dictionaries, such as:
            {"something": 1, "else": "a"}
            {"something": 2, "else": "b"}
            ...etc
        this will just return thos those dictionaries one-by-one when iterated over.

        The same set of dictionaries will also be yielded by a file containing:
            ["something", "else"]
            [1, "a"]
            [2, "b"]
            ...etc
        this will just return thos those dictionaries one-by-one when iterated over.

        NOTES:

        * In the second case, shorter lists on subsequent lines return only partial dictionaries.
        * In the second case, longer lists on subsequent lines will quietly drop any extra elements.
        """

        self.fp = fp
        self.padded: bool = padded
        self.padding = padding
        self.headers = None  # Might change after we see first line

    def __iter__(self):
        first_line = True
        n_headers = 0
        for raw_line in self.fp:
            line = json.loads(raw_line)
            if first_line:
                first_line = False
                if isinstance(line, list):
                    self.headers = line
                    n_headers = len(line)
                    continue
            # If length of line is more than we expect, ignore it. Let user put comments beyond our table
            # But if length of line is less than we expect, extend the line with None
            if self.headers:
                if not isinstance(line, list):
                    raise Exception("If the first line is a list, all lines must be.")
                if self.padded and len(line) < n_headers:
                    line = pad_to(n_headers, line, padding=self.padding)
                yield dict(zip(self.headers, line))
            elif isinstance(line, dict):
                yield line
            else:
                raise Exception(f"If the first line is not a list, all lines must be dictionaries: {line!r}")


def get_app_specific_directory() -> str:
    """
    Returns the standard system application specific directory:
    - On MacOS this directory: is: ~/Library/Application Support
    - On Linux this directory is: ~/.local/share
    - On Windows this directory is: %USERPROFILE%\\AppData\\Local  # noqa
    N.B. This is has been tested on MacOS and Linux but not on Windows.
    """
    return appdirs.user_data_dir()


def get_os_name() -> str:
    if os_name := platform.system():
        if os_name == "Darwin": return "osx"  # noqa
        elif os_name == "Linux": return "linux"  # noqa
        elif os_name == "Windows": return "windows"  # noqa
    return ""


def get_cpu_architecture_name() -> str:
    if os_architecture_name := platform.machine():
        if os_architecture_name == "x86_64": return "amd64"  # noqa
        return os_architecture_name
    return ""


def create_uuid(nodash: bool = False, upper: bool = False) -> str:
    value = str(uuid.uuid4())
    if nodash is True:
        value = value.replace("-", "")
    if upper is True:
        value = value.upper()
    return value


def create_short_uuid(length: Optional[int] = None, upper: bool = False):
    # Not really techincally a uuid of course.
    if (length is None) or (not isinstance(length, int)) or (length < 1):
        length = 16
    value = shortuuid.ShortUUID().random(length=length)
    if upper is True:
        value = value.upper()
    return value


print(format_size(1024 * 1024))
