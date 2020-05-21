"""
This file contains functions that might be generally useful.
"""

import contextlib
import functools
import os
import logging
import time
import warnings
import webtest  # importing the library makes it easier to mock testing

from typing import Type


# Is this the right place for this? I feel like this should be done in an application, not a library.
# -kmp 27-Apr-2020
logging.basicConfig()


# Using PRINT(...) for debugging, rather than its more familiar lowercase form) for intended programmatic output,
# makes it easier to find stray print statements that were left behind in debugging. -kmp 30-Mar-2020

PRINT = print


class VirtualAppError(Exception):
    """ Special Exception to be raised by VirtualApp that contains some additional info """

    def __init__(self, msg, url, body, e):
        super(VirtualAppError, self).__init__(msg)
        self.msg = msg
        self.query_url = url
        self.query_body = body
        self.raw_exception = e

    def __repr__(self):
        return "Exception encountered on VirtualApp\n" \
               "URL: %s\n" \
               "BODY: %s\n" \
               "MSG: %s\n" \
               "Raw Exception: %s\n" % (self.query_url, self.query_body, self.msg, self.raw_exception)

    def __str__(self):
        return self.__repr__()


class _VirtualAppHelper(webtest.TestApp):  # effectively disguises 'TestApp'
    pass


class VirtualApp:
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
            raise VirtualAppError(msg='HTTP GET failed.', url=url, body='<empty>', e=str(e))

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
            raise VirtualAppError(msg='HTTP POST failed.', url=url, body=obj, e=str(e))

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
            raise VirtualAppError(msg='HTTP PATCH failed.', url=url, body=fields, e=str(e))


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
def filtered_warnings(action, message: str = "", category: Type[Warning] = Warning,
                      module: str = "", lineno: int = 0, append: bool = False):
    """
    Context manager temporarily filters deprecation messages for the duration of the body.
    Used otherwise the same as warnings.filterwarnings would be used.

    For example:

           with filtered_warnings('ignore', category=DeprecationWarning):
               ... use something that's deprecated without a lot of fuss ...

    Note: This is not threadsafe. It's OK while loading system and during testing,
          but not in worker threads.
    """
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

        def __init__(self, retries_allowed=None, wait_seconds=None):
            self.retries_allowed = retries_allowed
            self.wait_seconds = wait_seconds or wait_seconds

        @property
        def tries_allowed(self):
            return 1 + self.retries_allowed

    _RETRY_OPTIONS_CATALOG = {}

    DEFAULT_RETRIES_ALLOWED = 1
    DEFAULT_WAIT_SECONDS = 0

    @classmethod
    def _wait_adjustor(cls, wait_increment, wait_multiplier):

        if wait_increment and wait_multiplier:
            raise SyntaxError("You may not specify both wait_increment and wait_multiplier.")

        if wait_increment:
            return lambda x: x + wait_increment
        elif wait_multiplier:
            return lambda x: x * wait_multiplier
        else:
            return lambda x: x

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
        """

        def decorator(function):
            function_name = name_key or function.__name__
            function_profile = cls.RetryOptions(
                retries_allowed=cls.DEFAULT_RETRIES_ALLOWED if retries_allowed is None else retries_allowed,
                wait_seconds=cls.DEFAULT_WAIT_SECONDS if wait_seconds is None else wait_seconds
            )

            cls._RETRY_OPTIONS_CATALOG[function_name] = function_profile  # Only for debugging.
            function_profile.retries_allowed = retries_allowed
            function_profile.wait_seconds = wait_seconds or cls.DEFAULT_WAIT_SECONDS
            function_profile.wait_adjustor = cls._wait_adjustor(wait_increment=wait_increment,
                                                                wait_multiplier=wait_multiplier)

            @functools.wraps(function)
            def wrapped_function(*args, **kwargs):
                tries_allowed = function_profile.tries_allowed
                wait_seconds = function_profile.wait_seconds or 0
                for i in range(tries_allowed):
                    if i > 0:
                        if i > 1:
                            wait_seconds = function_profile.wait_adjustor(wait_seconds)
                        if wait_seconds > 0:
                            time.sleep(wait_seconds)
                    try:
                        success = function(*args, **kwargs)
                        return success
                    except Exception:
                        pass
                raise

            return wrapped_function

        return decorator
