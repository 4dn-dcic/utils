"""
This file contains functions that might be generally useful.
"""

import contextlib
import datetime
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
            raise VirtualAppError(msg='HTTP GET failed.', url=url, body='<empty>', raw_exception=e)

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

        def make_wait_adjustor(self, wait_increment=None, wait_multiplier=None):
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

        def decorator(function):
            function_name = name_key or function.__name__
            function_profile = cls.RetryOptions(
                retries_allowed=cls._defaulted(retries_allowed, cls.DEFAULT_RETRIES_ALLOWED),
                wait_seconds=cls._defaulted(wait_seconds, cls.DEFAULT_WAIT_SECONDS),
                wait_increment=cls._defaulted(wait_increment, cls.DEFAULT_WAIT_INCREMENT),
                wait_multiplier=cls._defaulted(wait_multiplier, cls.DEFAULT_WAIT_MULTIPLIER),
            )

            # See the 'retrying' method to understand what this is about. -kmp 8-Jul-2020
            if function_name != 'anonymous':
                cls._RETRY_OPTIONS_CATALOG[function_name] = function_profile  # Only for debugging.

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


def utc_today_str():
    """Returns a YYYY-mm-dd date string, relative to the UTC timezone."""
    return datetime.datetime.strftime(datetime.datetime.utcnow(), "%Y-%m-%d")


# I think we will not need LockoutManager. It's really just a special case of RateManager,
# though its operations are a little different. This commented-out code should be removed
# once we have successfully installed a system based on RateManager. -kmp 20-Jul-2020
#
# class LockoutManager:
#     """
#     This class is used as a guard of a critical operation that can only be called within a certain frequency.
#     e.g.,
#
#         class Foo:
#             def __init__(self):
#                 # 60 seconds required between calls, with a 1 second margin of error (overhead, clocks varying, etc)
#                 self.lockout_manager = LockoutManager(action="foo", lockout_seconds=1, safety_seconds=1)
#             def foo():
#                 self.lockout_manager.wait_if_needed()
#                 do_guarded_action()
#
#         f = Foo()     # make a Foo
#         v1 = f.foo()  # will immediately get a value
#         time.sleep(58)
#         v2 = f.foo()  # will wait about 2 seconds, then get a value
#         v3 = f.foo()  # will wait about 60 seconds, then get a value
#
#     Conceptually this is a special case of RateManager for n=1, though in practice it arose differently and
#     the supplementary methods (which we happen to use mostly for testing) differ because the n=1 case is simpler
#     and admits more questions. So, for now at least, this is not a subclass of RateManager but a separate
#     implementation.
#     """
#
#     EARLIEST_TIMESTAMP = datetime.datetime(datetime.MINYEAR, 1, 1)  # maybe useful for testing
#
#     def __init__(self, *, lockout_seconds, safety_seconds=0, action="metered action", enabled=True, log=None):
#         """
#         Creates a LockoutManager that cooperates in assuring a guarded operation is only happens at a certain rate.
#
#         The rate is once person lockout_seconds. This is a special case of RateManager and might get phased out
#         as redundant, but has slightly different operations available for testing.
#
#         Args:
#
#         lockout_seconds int: A theoretical number of seconds allowed between calls to the guarded operation.
#         safety_seconds int: An amount added to interval_seconds to accommodate real world coordination fuzziness.
#         action str: A noun or noun phrase describing the action being guarded.
#         enabled bool: A boolean controlling whether this facility is enabled. If False, waiting is disabled.
#         log object: A logger object (supporting operations like .debug, .info, .warning, and .error).
#         """
#
#         # This makes it easy to turn off the feature
#         self.lockout_enabled = enabled
#         self.lockout_seconds = lockout_seconds
#         self.safety_seconds = safety_seconds
#         self.action = action
#         self._timestamp = self.EARLIEST_TIMESTAMP
#         self.log = log or logging
#
#     @property
#     def timestamp(self):
#         """The timestamp is read-only. Use update_timestamp() to set it."""
#         return self._timestamp
#
#     @property
#     def effective_lockout_seconds(self):
#         """
#         The effective time between calls
#
#         Returns: the sum of the lockout and the safety seconds
#         """
#         return self.lockout_seconds + self.safety_seconds
#
#     def wait_if_needed(self):
#         """
#         This function is intended to be called immediately prior to each guarded operation.
#
#         This function will wait (using time.sleep) only if necessary, and for the amount necessary,
#         to comply with rate-limiting declared in the creation of this LockoutManager.
#
#         NOTE WELL: It is presumed that all calls are coming from this source. This doesn't have ESP that would
#         detect or otherwise accommodate externally generated calls, so violations of rate-limiting can still
#         happen that way. This should be sufficient for sequential testing, and better than nothing for
#         production operation.  This is not a substitute for responding to server-initiated throttling protocols.
#         """
#         now = datetime.datetime.now()
#         # Note that this quantity is always positive because now is always bigger than the timestamp.
#         seconds_since_last_purge = (now - self._timestamp).total_seconds()
#         # Note again that because seconds_since_last_attempt is positive, the wait seconds will
#         # never exceed self.effective_lockout_seconds, so
#         #   0 <= wait_seconds <= self.effective_lockout_seconds
#         wait_seconds = max(0.0, self.effective_lockout_seconds - seconds_since_last_purge)
#         if wait_seconds > 0.0:
#             shared_message = ("Last %s attempt was at %s (%s seconds ago)."
#                               % (self.action, self._timestamp, seconds_since_last_purge))
#             if self.lockout_enabled:
#                 action_message = "Waiting %s seconds before attempting another." % wait_seconds
#                 self.log.warning("%s %s" % (shared_message, action_message))
#                 time.sleep(wait_seconds)
#             else:
#                 action_message = "Continuing anyway because lockout is disabled."
#                 self.log.warning("%s %s" % (shared_message, action_message))
#         self.update_timestamp()
#
#     def update_timestamp(self):
#         """
#         Explicitly sets the reference time point for computation of our lockout.
#         This is called implicitly by .wait_if_needed(), and for some situations that may be sufficient.
#         """
#         self._timestamp = datetime.datetime.now()


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
