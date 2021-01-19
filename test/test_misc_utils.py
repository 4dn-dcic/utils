import botocore.exceptions
import datetime as datetime_module
import io
import json
import os
import pytest
import pytz
import random
import re
import time
import warnings
import webtest

from dcicutils.misc_utils import (
    PRINT, ignored, filtered_warnings, get_setting_from_context, TestApp, VirtualApp, VirtualAppError,
    _VirtualAppHelper,  # noqa - yes, this is a protected member, but we still want to test it
    Retry, apply_dict_overrides, utc_today_str, RateManager, environ_bool,
    LockoutManager, check_true, remove_prefix, remove_suffix, full_class_name, full_object_name, constantly,
    keyword_as_title, file_contents, CachedField, camel_case_to_snake_case, snake_case_to_camel_case, make_counter,
    CustomizableProperty, UncustomizedInstance, getattr_customized, copy_json, url_path_join,
    as_seconds, ref_now, in_datetime_interval, as_datetime, as_ref_datetime, as_utc_datetime, REF_TZ, hms_now, HMS_TZ,
    DatetimeCoercionFailure, remove_element,
)
from dcicutils.qa_utils import (
    Occasionally, ControlledTime, override_environ, MockFileSystem, printed_output, raises_regexp
)
from unittest import mock


def test_uppercase_print():
    # This is just a synonym, so the easiest thing is just to test that fact.
    assert PRINT._printer == print

    # But also a basic test that it does something
    s = io.StringIO()
    PRINT("something", file=s)
    assert s.getvalue() == "something\n"


def test_ignored():
    def foo(x, y):
        ignored(x, y)
    # Check that no error occurs for having used this.
    assert foo(3, 4) is None


def test_get_setting_from_context():

    sample_settings = {'pie.flavor': 'apple'}

    with mock.patch.object(os, "environ", {}):

        assert get_setting_from_context(sample_settings, ini_var='pie.flavor') == 'apple'
        assert get_setting_from_context(sample_settings, ini_var='pie.color') is None
        assert get_setting_from_context(sample_settings, ini_var='pie.color', default='brown') == 'brown'

        # Note that env_var=None means 'use default', not 'no env var'. You'd want env_var=False for 'no env var'.
        assert get_setting_from_context(sample_settings, ini_var='pie.flavor', env_var=None) == 'apple'
        assert get_setting_from_context(sample_settings, ini_var='pie.color', env_var=None) is None
        assert get_setting_from_context(sample_settings, ini_var='pie.color', env_var=None, default='brown') == 'brown'

        assert get_setting_from_context(sample_settings, ini_var='pie.flavor', env_var=False) == 'apple'
        assert get_setting_from_context(sample_settings, ini_var='pie.color', env_var=False) is None
        assert get_setting_from_context(sample_settings, ini_var='pie.color', env_var=False, default='brown') == 'brown'

    with mock.patch.object(os, "environ", {'PIE_FLAVOR': 'cherry', 'PIE_COLOR': 'red'}):

        assert get_setting_from_context(sample_settings, ini_var='pie.flavor') == 'cherry'
        assert get_setting_from_context(sample_settings, ini_var='pie.color') == 'red'

        # Note that env_var=None means 'use default', not 'no env var'. You'd want env_var=False for 'no env var'.
        assert get_setting_from_context(sample_settings, ini_var='pie.flavor', env_var=None) == 'cherry'
        assert get_setting_from_context(sample_settings, ini_var='pie.color', env_var=None) == 'red'

        assert get_setting_from_context(sample_settings, ini_var='pie.flavor', env_var=False) == 'apple'
        assert get_setting_from_context(sample_settings, ini_var='pie.color', env_var=False) is None

    with mock.patch.object(os, "environ", {'PIE_FLAVOR': '', 'PIE_COLOR': ''}):

        # Note that because there is an explicit value in the environment, even null, that gets used.

        assert get_setting_from_context(sample_settings, ini_var='pie.flavor') == ''
        assert get_setting_from_context(sample_settings, ini_var='pie.flavor', env_var=None) == ''
        assert get_setting_from_context(sample_settings, ini_var='pie.flavor', env_var=False) == 'apple'

        assert get_setting_from_context(sample_settings, ini_var='pie.flavor', env_var=None, default='lime') == ''
        assert get_setting_from_context(sample_settings, ini_var='pie.flavor', env_var=False, default='lime') == 'apple'

        assert get_setting_from_context(sample_settings, ini_var='pie.color') == ''
        assert get_setting_from_context(sample_settings, ini_var='pie.color', env_var=None) == ''
        assert get_setting_from_context(sample_settings, ini_var='pie.color', env_var=False) is None

        assert get_setting_from_context(sample_settings, ini_var='pie.color', env_var=None, default='green') == ''
        assert get_setting_from_context(sample_settings, ini_var='pie.color', env_var=False, default='green') == 'green'


class FakeResponse:

    def __init__(self, json):
        self._json = json

    def json(self):
        return self._json

    @property
    def content(self):
        return json.dumps(self.json, indent=2, default=str)


class FakeTestApp:
    def __init__(self, app, extra_environ=None):
        self.app = app
        self.extra_environ = extra_environ or {}
        self.calls = []

    def get(self, url, **kwargs):
        call_info = {'op': 'get', 'url': url, 'kwargs': kwargs}
        self.calls.append(call_info)
        return FakeResponse({'processed': call_info})

    def post_json(self, url, obj, **kwargs):
        call_info = {'op': 'post_json', 'url': url, 'obj': obj, 'kwargs': kwargs}
        self.calls.append(call_info)
        return FakeResponse({'processed': call_info})

    def put_json(self, url, obj, **kwargs):
        call_info = {'op': 'put_json', 'url': url, 'obj': obj, 'kwargs': kwargs}
        self.calls.append(call_info)
        return FakeResponse({'processed': call_info})

    def patch_json(self, url, obj, **kwargs):
        call_info = {'op': 'patch_json', 'url': url, 'obj': obj, 'kwargs': kwargs}
        self.calls.append(call_info)
        return FakeResponse({'processed': call_info})


class FakeApp:
    pass


def test_test_app():
    test_app = TestApp(FakeApp(), {})
    assert isinstance(test_app, webtest.TestApp)
    assert not test_app.__test__


def test_virtual_app_creation():
    with mock.patch.object(VirtualApp, "HELPER_CLASS", FakeTestApp):
        app = FakeApp()
        environ = {'some': 'stuff'}

        vapp = VirtualApp(app, environ)

        assert isinstance(vapp, VirtualApp)
        assert not isinstance(vapp, webtest.TestApp)
        assert not isinstance(vapp, _VirtualAppHelper)

        assert isinstance(vapp.wrapped_app, FakeTestApp)  # the mocked one, anyway.
        assert vapp.wrapped_app.app is app
        assert vapp.wrapped_app.extra_environ is environ

        assert vapp.app is vapp.wrapped_app.app

        return vapp


def test_virtual_app_get():

    with mock.patch.object(VirtualApp, "HELPER_CLASS", FakeTestApp):
        app = FakeApp()
        environ = {'some': 'stuff'}
        vapp = VirtualApp(app, environ)

    log_info = []

    with mock.patch("logging.info") as mock_info:
        mock_info.side_effect = lambda msg: log_info.append(msg)

        response1 = vapp.get("http://no.such.place/")
        assert response1.json() == {
            'processed': {
                'op': 'get',
                'url': 'http://no.such.place/',
                'kwargs': {},
            }
        }

        response2 = vapp.get("http://no.such.place/", params={'foo': 'bar'})
        assert response2.json() == {
            'processed': {
                'op': 'get',
                'url': 'http://no.such.place/',
                'kwargs': {'params': {'foo': 'bar'}},
            }
        }

        assert log_info == [
            'OUTGOING HTTP GET: http://no.such.place/',
            'OUTGOING HTTP GET: http://no.such.place/',
        ]
        assert vapp.wrapped_app.calls == [
            {
                'op': 'get',
                'url': 'http://no.such.place/',
                'kwargs': {},
            },
            {
                'op': 'get',
                'url': 'http://no.such.place/',
                'kwargs': {'params': {'foo': 'bar'}},
            },
        ]


def test_virtual_app_post_json():

    with mock.patch.object(VirtualApp, "HELPER_CLASS", FakeTestApp):
        app = FakeApp()
        environ = {'some': 'stuff'}
        vapp = VirtualApp(app, environ)

    log_info = []

    with mock.patch("logging.info") as mock_info:
        mock_info.side_effect = lambda msg: log_info.append(msg)

        response1 = vapp.post_json("http://no.such.place/", {'beta': 'gamma'})
        assert response1.json() == {
            'processed': {
                'op': 'post_json',
                'url': 'http://no.such.place/',
                'obj': {'beta': 'gamma'},
                'kwargs': {},
            }
        }

        response2 = vapp.post_json("http://no.such.place/", {'alpha': 'omega'}, params={'foo': 'bar'})
        assert response2.json() == {
            'processed': {
                'op': 'post_json',
                'url': 'http://no.such.place/',
                'obj': {'alpha': 'omega'},
                'kwargs': {'params': {'foo': 'bar'}},
            }
        }

        assert log_info == [
            ("OUTGOING HTTP POST on url: %s with object: %s"
             % ("http://no.such.place/", {'beta': 'gamma'})),
            ("OUTGOING HTTP POST on url: %s with object: %s"
             % ("http://no.such.place/", {'alpha': 'omega'})),
        ]
        assert vapp.wrapped_app.calls == [
            {
                'op': 'post_json',
                'url': 'http://no.such.place/',
                'obj': {'beta': 'gamma'},
                'kwargs': {},
            },
            {
                'op': 'post_json',
                'url': 'http://no.such.place/',
                'obj': {'alpha': 'omega'},
                'kwargs': {'params': {'foo': 'bar'}},
            },
        ]


def test_virtual_app_put_json():

    with mock.patch.object(VirtualApp, "HELPER_CLASS", FakeTestApp):
        app = FakeApp()
        environ = {'some': 'stuff'}
        vapp = VirtualApp(app, environ)

    log_info = []

    with mock.patch("logging.info") as mock_info:
        mock_info.side_effect = lambda msg: log_info.append(msg)

        response1 = vapp.put_json("http://no.such.place/", {'beta': 'gamma'})
        assert response1.json() == {
            'processed': {
                'op': 'put_json',
                'url': 'http://no.such.place/',
                'obj': {'beta': 'gamma'},
                'kwargs': {},
            }
        }

        response2 = vapp.put_json("http://no.such.place/", {'alpha': 'omega'}, params={'foo': 'bar'})
        assert response2.json() == {
            'processed': {
                'op': 'put_json',
                'url': 'http://no.such.place/',
                'obj': {'alpha': 'omega'},
                'kwargs': {'params': {'foo': 'bar'}},
            }
        }

        assert log_info == [
            ("OUTGOING HTTP PUT on url: %s with object: %s"
             % ("http://no.such.place/", {'beta': 'gamma'})),
            ("OUTGOING HTTP PUT on url: %s with object: %s"
             % ("http://no.such.place/", {'alpha': 'omega'})),
        ]
        assert vapp.wrapped_app.calls == [
            {
                'op': 'put_json',
                'url': 'http://no.such.place/',
                'obj': {'beta': 'gamma'},
                'kwargs': {},
            },
            {
                'op': 'put_json',
                'url': 'http://no.such.place/',
                'obj': {'alpha': 'omega'},
                'kwargs': {'params': {'foo': 'bar'}},
            },
        ]


def test_virtual_app_patch_json():

    with mock.patch.object(VirtualApp, "HELPER_CLASS", FakeTestApp):
        app = FakeApp()
        environ = {'some': 'stuff'}
        vapp = VirtualApp(app, environ)

    log_info = []

    with mock.patch("logging.info") as mock_info:
        mock_info.side_effect = lambda msg: log_info.append(msg)

        response1 = vapp.patch_json("http://no.such.place/", {'beta': 'gamma'})
        assert response1.json() == {
            'processed': {
                'op': 'patch_json',
                'url': 'http://no.such.place/',
                'obj': {'beta': 'gamma'},
                'kwargs': {},
            }
        }

        response2 = vapp.patch_json("http://no.such.place/", {'alpha': 'omega'}, params={'foo': 'bar'})
        assert response2.json() == {
            'processed': {
                'op': 'patch_json',
                'url': 'http://no.such.place/',
                'obj': {'alpha': 'omega'},
                'kwargs': {'params': {'foo': 'bar'}},
            }
        }

        assert log_info == [
            ("OUTGOING HTTP PATCH on url: %s with changes: %s"
             % ("http://no.such.place/", {'beta': 'gamma'})),
            ("OUTGOING HTTP PATCH on url: %s with changes: %s"
             % ("http://no.such.place/", {'alpha': 'omega'})),
        ]
        assert vapp.wrapped_app.calls == [
            {
                'op': 'patch_json',
                'url': 'http://no.such.place/',
                'obj': {'beta': 'gamma'},
                'kwargs': {},
            },
            {
                'op': 'patch_json',
                'url': 'http://no.such.place/',
                'obj': {'alpha': 'omega'},
                'kwargs': {'params': {'foo': 'bar'}},
            },
        ]


def test_virtual_app_error():

    error_message = "You did a bad thing."
    offending_url = "http://fixture.4dnucleome.org/offending/url"
    body_text = '{"alpha": "omega"}'
    body_json = {"alpha": "omega"}
    wrapped_error = Exception("Some other exception")

    e = VirtualAppError(error_message, offending_url, body_text, wrapped_error)
    m = str(e)

    assert error_message in m
    assert offending_url in m
    assert body_text in m
    assert str(wrapped_error) in m

    # And the repr is the same.
    assert repr(e) == str(e)

    # NOTE: Weirdly, I think we'd have had complete code coverage even without this next test, but that illustrates
    #  why code coverage counts aren't always the right metric. With different data, the same code paths sometimes
    #  does different things in ways that code coverage tools don't register. -kmp 21-May-2020

    e2 = VirtualAppError(error_message, offending_url, body_json, wrapped_error)
    m2 = str(e2)

    assert error_message in m2
    assert offending_url in m2
    assert str(body_json) in m2             # body_json will be rendered as a Python dict (e.g., with single quotes)
    assert not json.dumps(body_json) in m2  # So body_json will NOT be rendered as JSON via json.dumps
    assert str(wrapped_error) in m2

    # And the repr is the same.
    assert repr(e2) == str(e2)


def test_virtual_app_crud_failure():

    simulated_error_message = "simulated error"

    class FakeTestApp:

        def __init__(self, app, environ):
            ignored(app, environ)

        def get(self, url, **kwargs):
            raise webtest.AppError(simulated_error_message)

        def post_json(self, url, object, **kwargs):  # noqa - the name of this argument is not chosen by us here
            raise webtest.AppError(simulated_error_message)

        def put_json(self, url, object, **kwargs):  # noqa - the name of this argument is not chosen by us here
            raise webtest.AppError(simulated_error_message)

        def patch_json(self, url, fields, **kwargs):
            raise webtest.AppError(simulated_error_message)

    with mock.patch.object(VirtualApp, "HELPER_CLASS", FakeTestApp):

        app = FakeApp()
        environ = {'some': 'stuff'}

        vapp = VirtualApp(app, environ)

        some_url = "http://fixture.4dnucleome.org/some/url"

        operations = [
            lambda: vapp.get(some_url),
            lambda: vapp.post_json(some_url, {'a': 1, 'b': 2, 'c': 3}),
            lambda: vapp.put_json(some_url, {'a': 1, 'b': 2, 'c': 3}),
            lambda: vapp.patch_json(some_url, {'b': 5})
        ]

        for operation in operations:
            try:
                operation()
            except Exception as e:
                assert isinstance(e, VirtualAppError)  # NOTE: not webtest.AppError, which is what was raised
                assert str(e.raw_exception) == simulated_error_message
                assert isinstance(e.raw_exception, webtest.AppError)


def test_filtered_warnings():

    def expect_warnings(pairs):
        with warnings.catch_warnings(record=True) as w:
            # Trigger a warning.
            warnings.warn("oh, this is deprecated for sure", DeprecationWarning)  # noqa
            warnings.warn("tsk, tsk, tsk, what ugly code", SyntaxWarning)  # noqa
            # Verify some things
            for expected_count, expected_type in pairs:
                count = 0
                for warning in w:
                    if issubclass(warning.category, expected_type):
                        count += 1
                assert count == expected_count

    expect_warnings([(2, Warning), (1, DeprecationWarning), (1, SyntaxWarning)])

    with filtered_warnings("ignore"):
        expect_warnings([(0, Warning), (0, DeprecationWarning), (0, SyntaxWarning)])

    with filtered_warnings("ignore", category=Warning):
        expect_warnings([(0, Warning), (0, DeprecationWarning), (0, SyntaxWarning)])

    with filtered_warnings("ignore", category=DeprecationWarning):
        expect_warnings([(1, Warning), (0, DeprecationWarning), (1, SyntaxWarning)])

    with filtered_warnings("ignore", category=SyntaxWarning):
        expect_warnings([(1, Warning), (1, DeprecationWarning), (0, SyntaxWarning)])


def _adder(n):
    def addn(x):
        return x + n
    return addn


def test_retry():

    sometimes_add2 = Occasionally(_adder(2), success_frequency=2)

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

    rarely_add3 = Occasionally(_adder(3), success_frequency=5)

    with pytest.raises(Exception):
        assert rarely_add3(1) == 4
    with pytest.raises(Exception):
        assert rarely_add3(1) == 4
    with pytest.raises(Exception):
        assert rarely_add3(1) == 4
    with pytest.raises(Exception):
        assert rarely_add3(1) == 4
    assert rarely_add3(1) == 4  # 5th time's a charm


def test_retry_timeouts():

    rarely_add3 = Occasionally(_adder(3), success_frequency=5)

    ARGS = 1  # noqa - We have to access a random place out of a tuple structure for mock data on time.sleep's arg

    # NOTE WELL: For testing, we chose 1.25 to use factors of 2 so floating point can exactly compare

    @Retry.retry_allowed(retries_allowed=4, wait_seconds=2, wait_multiplier=1.25)
    def reliably_add3(x):
        return rarely_add3(x)

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

    rarely_add3.reset()

    @Retry.retry_allowed(retries_allowed=4, wait_seconds=2, wait_increment=3)
    def reliably_add_three(x):
        return rarely_add3(x)

    with mock.patch("time.sleep") as mock_sleep:

        assert reliably_add_three(1) == 4

        assert mock_sleep.call_count == 4

        assert mock_sleep.mock_calls[0][ARGS][0] == 2
        assert mock_sleep.mock_calls[1][ARGS][0] == 5   # 2 + 3
        assert mock_sleep.mock_calls[2][ARGS][0] == 8   # 2 + 3 * 2
        assert mock_sleep.mock_calls[3][ARGS][0] == 11  # 2 + 3 * 3

        assert reliably_add_three(2) == 5

        assert mock_sleep.call_count == 8

        assert mock_sleep.mock_calls[4][ARGS][0] == 2
        assert mock_sleep.mock_calls[5][ARGS][0] == 5   # 2 + 3
        assert mock_sleep.mock_calls[6][ARGS][0] == 8   # 2 + 3 * 2
        assert mock_sleep.mock_calls[7][ARGS][0] == 11  # 2 + 3 * 3


def test_retry_error_handling():

    rarely_add3 = Occasionally(_adder(3), success_frequency=5)

    with pytest.raises(SyntaxError):

        @Retry.retry_allowed(retries_allowed=4, wait_seconds=2, wait_increment=3, wait_multiplier=1.25)
        def reliably_add_three(x):
            return rarely_add3(x)


def test_retrying_timeouts():

    rarely_add3 = Occasionally(_adder(3), success_frequency=5)

    ARGS = 1  # noqa - We have to access a random place out of a tuple structure for mock data on time.sleep's arg

    # NOTE WELL: For testing, we chose 1.25 to use factors of 2 so floating point can exactly compare

    reliably_add3 = Retry.retrying(rarely_add3, retries_allowed=4, wait_seconds=2, wait_multiplier=1.25)

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

    rarely_add3.reset()

    reliably_add_three = Retry.retrying(rarely_add3, retries_allowed=4, wait_seconds=2, wait_increment=3)

    with mock.patch("time.sleep") as mock_sleep:

        assert reliably_add_three(1) == 4

        assert mock_sleep.call_count == 4

        assert mock_sleep.mock_calls[0][ARGS][0] == 2
        assert mock_sleep.mock_calls[1][ARGS][0] == 5   # 2 + 3
        assert mock_sleep.mock_calls[2][ARGS][0] == 8   # 2 + 3 * 2
        assert mock_sleep.mock_calls[3][ARGS][0] == 11  # 2 + 3 * 3

        assert reliably_add_three(2) == 5

        assert mock_sleep.call_count == 8

        assert mock_sleep.mock_calls[4][ARGS][0] == 2
        assert mock_sleep.mock_calls[5][ARGS][0] == 5   # 2 + 3
        assert mock_sleep.mock_calls[6][ARGS][0] == 8   # 2 + 3 * 2
        assert mock_sleep.mock_calls[7][ARGS][0] == 11  # 2 + 3 * 3


def test_retrying_error_handling():

    rarely_add3 = Occasionally(_adder(3), success_frequency=5)

    with pytest.raises(SyntaxError):

        Retry.retrying(rarely_add3, retries_allowed=4, wait_seconds=2,
                       wait_increment=3, wait_multiplier=1.25)


def test_apply_dict_overrides():

    x = {'a': 1, 'b': 2}

    actual = apply_dict_overrides(x, a=11, c=33)
    expected = {'a': 11, 'b': 2, 'c': 33}
    assert isinstance(actual, dict)
    assert actual == expected
    assert x == expected

    actual = apply_dict_overrides(x, b=22, c=None)
    expected = {'a': 11, 'b': 22, 'c': 33}
    assert actual == expected
    assert x == expected


def test_utc_today_str():
    pattern = "[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]"
    actual = utc_today_str()
    assert re.match(pattern, actual), "utc_today_str() result %s did not match format: %s" % (actual, pattern)


def test_as_seconds():
    assert as_seconds(seconds=1, minutes=1) == 61
    assert as_seconds(seconds=1, minutes=1, as_type=str) == '61'
    assert as_seconds(minutes=0.5, seconds=1) == 31
    assert as_seconds(minutes=0.5, seconds=1, as_type=str) == '31'
    assert as_seconds(minutes=0.025) == 1.5
    assert as_seconds() == 0
    assert as_seconds(weeks=1, days=1, hours=1, minutes=1, seconds=1) == 694861
    assert as_seconds(weeks=1, days=1, hours=1, minutes=1, seconds=1, milliseconds=500) == 694861.5
    assert as_seconds(milliseconds=1000) == 1
    assert as_seconds(milliseconds=2000) == 2
    assert as_seconds(milliseconds=250) == 0.25


def test_hms_tz():

    assert HMS_TZ == REF_TZ, "HMS_TZ was deprectead, but is still expected to be a synonym for REF_TZ."


@pytest.mark.parametrize('now', [hms_now, ref_now])  # hms_now is a deprecated name for ref_now
def test_hms_now_and_ref_now(now):

    t0 = datetime_module.datetime(2015, 7, 4, 12, 0, 0)
    dt = ControlledTime(t0)

    with mock.patch("dcicutils.misc_utils.datetime", dt):

        t1 = now()
        t1_utc = t1.replace(tzinfo=pytz.UTC)
        assert t1.replace(tzinfo=None) == t0 + datetime_module.timedelta(seconds=1)
        delta = (t1 - t1_utc)
        delta_seconds = delta.total_seconds()
        delta_hours = delta_seconds / 3600
        assert delta_hours in {4.0, 5.0}  # depending on daylight savings time, HMS is either 4 or 5 hours off from UTC


def test_as_datetime():

    t0 = datetime_module.datetime(2015, 7, 4, 12, 0, 0)
    t0_utc = pytz.UTC.localize(datetime_module.datetime(2015, 7, 4, 12, 0, 0))

    assert as_datetime(t0) == t0
    assert as_datetime(t0, tz=pytz.UTC) == t0_utc
    assert not as_datetime('2015-07-04T12:00:00').tzinfo
    assert as_datetime('2015-07-04T12:00:00') == t0
    assert as_datetime('2015-07-04 12:00:00') == t0
    assert as_datetime('2015-07-04 12:00:00', tz=pytz.UTC) == t0_utc
    assert as_datetime('2015-07-04 12:00:00Z').tzinfo
    assert as_datetime('2015-07-04 12:00:00Z') != t0
    assert as_datetime('2015-07-04 12:00:00Z') == t0_utc
    assert as_datetime('2015-07-04 12:00:00-0000') == t0_utc

    with raises_regexp(DatetimeCoercionFailure,
                       re.escape("Cannot coerce to datetime: 2018-01-02 25:00:00")):
        as_datetime("2018-01-02 25:00:00")  # There is no 25 o'clock

    assert as_datetime("2018-01-02 25:00:00", raise_error=False) is None

    with raises_regexp(DatetimeCoercionFailure,
                       re.escape("Cannot coerce to datetime: 2018-01-02 25:00:00 (for timezone UTC)")):
        as_datetime("2018-01-02 25:00:00", pytz.UTC)  # There is no 25 o'clock

    assert as_datetime("2018-01-02 25:00:00", pytz.UTC, raise_error=False) is None


def test_as_ref_datetime():

    t0 = datetime_module.datetime(2015, 7, 4, 12, 0, 0)
    t0_utc = pytz.UTC.localize(datetime_module.datetime(2015, 7, 4, 12, 0, 0))
    t0_hms = REF_TZ.localize(datetime_module.datetime(2015, 7, 4, 12, 0, 0))

    # Things that parse as a date equivalent to noon Jul 4, 2014 in HMS time
    for t in (t0, t0_hms,
              '2015-07-04T12:00:00',
              '2015-07-04 12:00:00',
              '2015-07-04T12:00:00-0400',
              '2015-07-04 12:00:00-0400'):
        result = as_ref_datetime(t)
        assert result.tzinfo
        assert result != t0
        assert result != t0_utc
        assert result == t0_hms
        assert str(result) == '2015-07-04 12:00:00-04:00'

    # Note that if this were in winter instead of summer, a different timezone marker would come back
    for t in (datetime_module.datetime(2015, 1, 4, 12), '2015-01-04T12:00:00-05:00', '2015-01-04 12:00:00-05:00'):
        assert str(as_ref_datetime(t)) == '2015-01-04 12:00:00-05:00'

    # Things that parse as a date equivalent to noon Jul 4, 2014 in UTC time because that is given explicitly,
    # but the result is still expressed in HMS time notation.
    for t in (t0_utc,
              '2015-07-04T12:00:00Z',
              '2015-07-04 12:00:00Z',
              '2015-07-04T12:00:00-0000',
              '2015-07-04 12:00:00-0000',
              '2015-07-04T12:00:00+0000',
              '2015-07-04 12:00:00+0000',
              '2015-07-04T12:00:00-00:00',
              '2015-07-04 12:00:00-00:00',
              '2015-07-04T12:00:00+00:00',
              '2015-07-04 12:00:00+00:00'):
        result = as_ref_datetime(t)
        assert result.tzinfo
        assert result != t0
        assert result == t0_utc  # The times are equivalent even if the notation is different
        assert result != t0_hms
        assert str(result) == '2015-07-04 08:00:00-04:00'  # The result notation is different than the UTC input

    with raises_regexp(DatetimeCoercionFailure,
                       re.escape("Cannot coerce to datetime: 2018-01-02 25:00:00 (for timezone US/Eastern)")):
        as_ref_datetime("2018-01-02 25:00:00")  # There is no 25 o'clock

    with raises_regexp(DatetimeCoercionFailure,
                       re.escape("Cannot coerce to datetime: 2018-01-02 25:00:00Z (for timezone US/Eastern)")):
        # This is parsed against US/Eastern time (or whatever REF_TIME is), so the message will mention
        # that time even though 'Z' tries to override it. The 'Z' part can be recovered in the error message
        # by looking at the string it's trying to parse.
        as_ref_datetime("2018-01-02 25:00:00Z")  # There is no 25 o'clock


def test_as_utc_datetime():

    t0 = datetime_module.datetime(2015, 7, 4, 12)
    t0_utc = pytz.UTC.localize(datetime_module.datetime(2015, 7, 4, 12, 0, 0))
    t0_hms = REF_TZ.localize(datetime_module.datetime(2015, 7, 4, 12, 0, 0))

    t4_utc = datetime_module.datetime(2015, 7, 4, 16, 0, 0, tzinfo=pytz.UTC)  # same as t0_hms, but UTC is 4 hour offset
    t5_utc = datetime_module.datetime(2015, 1, 4, 17, 0, 0, tzinfo=pytz.UTC)  # 5 hour offset from default ref time

    # Things that parse as a date equivalent to noon Jul 4, 2014 in UTC time
    for t in (t0, t0_hms, t4_utc,
              '2015-07-04T12:00:00',  # HMS time implied
              '2015-07-04 12:00:00',  # HMS time implied
              '2015-07-04T16:00:00Z',
              '2015-07-04 16:00:00Z'):
        result = as_utc_datetime(t)
        assert result.tzinfo
        assert result != t0
        assert result != t0_utc
        assert result == t0_hms
        assert result == t4_utc
        assert str(result) == '2015-07-04 16:00:00+00:00'

    # Note that if this were in winter instead of summer, a different timezone marker would come back
    for t in (datetime_module.datetime(2015, 1, 4, 12),
              '2015-01-04T12:00:00-0500',
              '2015-01-04 12:00:00-0500',
              '2015-01-04T12:00:00-05:00',
              '2015-01-04 12:00:00-05:00',
              '2015-01-04T17:00:00Z',
              '2015-01-04 17:00:00Z'):
        result = as_utc_datetime(t)
        assert result == t5_utc
        assert str(result) == '2015-01-04 17:00:00+00:00'

    # Things that parse as a date equivalent to noon Jul 4, 2014 in UTC time because that is given explicitly,
    # but the result is still expressed in HMS time notation.
    for t in (t0_utc,
              '2015-07-04T12:00:00Z',
              '2015-07-04 12:00:00Z',
              '2015-07-04T12:00:00-0000',
              '2015-07-04 12:00:00-0000',
              '2015-07-04T12:00:00+0000',
              '2015-07-04 12:00:00+0000'):
        result = as_utc_datetime(t)
        assert result.tzinfo
        assert result != t0
        assert result == t0_utc
        assert result != t0_hms
        assert str(result) == '2015-07-04 12:00:00+00:00'

    with raises_regexp(DatetimeCoercionFailure,
                       re.escape("Cannot coerce to datetime: 2018-01-02 25:00:00 (for timezone UTC)")):
        as_utc_datetime("2018-01-02 25:00:00")  # There is no 25 o'clock


def test_in_datetime_interval():

    t0 = datetime_module.datetime(2015, 7, 4, 12, 0, 0)
    t1 = datetime_module.datetime(2015, 7, 4, 13, 0, 0)
    t2 = datetime_module.datetime(2015, 7, 4, 14, 0, 0)
    t3 = datetime_module.datetime(2015, 7, 4, 15, 0, 0)
    t4 = datetime_module.datetime(2015, 7, 4, 16, 0, 0)

    in_t1_to_t3_range_scenarios = [
        (t0, False),
        (t1, True),
        (t2, True),
        (t3, True),
        (t4, False),
    ]

    for t, expected in in_t1_to_t3_range_scenarios:
        assert in_datetime_interval(t, start=t1, end=t3) is expected
        assert in_datetime_interval(t, start=str(t1), end=str(t3)) is expected

    in_t1_and_afterwards_range_scenarios = [
        (t0, False),
        (t1, True),
        (t2, True),
        (t3, True),
        (t4, True),
    ]

    for t, expected in in_t1_and_afterwards_range_scenarios:
        assert in_datetime_interval(t, start=t1) is expected

    in_t1_and_beforehand_range_scenarios = [
        (t0, True),
        (t1, True),
        (t2, True),
        (t3, True),
        (t4, False),
    ]

    for t, expected in in_t1_and_beforehand_range_scenarios:
        assert in_datetime_interval(t, end=t3) is expected

    with pytest.raises(DatetimeCoercionFailure):
        # This will raise an error because the 'end=' argument has bad syntax.
        in_datetime_interval("2015-01-01 23:59:00", start="2015-01-01 22:00:00", end="2015-01-01 25:00:00")


def test_lockout_manager_timestamp():

    tick = 0.1
    dt = ControlledTime(tick_seconds=tick)
    timedelta = datetime_module.timedelta

    with mock.patch.object(datetime_module, "datetime", dt):
        with mock.patch.object(time, "sleep", dt.sleep):

            manager = LockoutManager(lockout_seconds=60, safety_seconds=1, action="simulated action")

            assert manager.timestamp == manager.EARLIEST_TIMESTAMP

            manager.wait_if_needed()

            assert manager.timestamp == dt.just_now()

            manager = LockoutManager(lockout_seconds=60, safety_seconds=1, action="simulated action", enabled=False)

            assert manager.timestamp == manager.EARLIEST_TIMESTAMP

            manager.wait_if_needed()

            t0 = dt.just_now()

            assert manager.timestamp == t0

            # This will check time twice but not sleep when disabled.
            # The internal timestamp will be set to the second of those time checks.
            manager.wait_if_needed()

            assert manager.timestamp == t0 + timedelta(seconds=2 * tick)

            time.sleep(30)

            # The passage of time doesn't change the timestamp value, only waiting or update_timestamp does.
            assert manager.timestamp == t0 + timedelta(seconds=2 * tick)

            t1 = dt.just_now()

            manager.update_timestamp()  # Reads time once to set it.

            assert manager.timestamp == t1 + timedelta(seconds=1 * tick)


def test_lockout_manager():

    protected_action = "simulated action"

    # The function now() will get us the time. This assure us that binding datetime.datetime
    # will not be affecting us.
    now = datetime_module.datetime.now

    # real_t0 is the actual wallclock time at the start of this test. We use it only to make sure
    # that all these other tests are really going through our mock. In spite of longer mocked
    # timescales, this test should run quickly.
    real_t0 = now()
    print("Starting test at", real_t0)

    # dt will be our substitute for datetime.datetime.
    # (it also has a sleep method that we can substitute for time.sleep)
    dt = ControlledTime(tick_seconds=1)

    class MockLogger:

        def __init__(self):
            self.log = []

        def warning(self, msg):
            self.log.append(msg)

    with mock.patch("datetime.datetime", dt):
        with mock.patch("time.sleep", dt.sleep):
            my_log = MockLogger()

            assert isinstance(datetime_module.datetime, ControlledTime)

            lockout_manager = LockoutManager(action=protected_action,
                                             lockout_seconds=60,
                                             safety_seconds=1,
                                             log=my_log)
            assert not hasattr(lockout_manager, 'client')  # Just for safety, we don't need a client for this test

            t0 = dt.just_now()

            lockout_manager.wait_if_needed()

            t1 = dt.just_now()

            print("t0=", t0)
            print("t1=", t1)

            # We've set the clock to increment 1 second on every call to datetime.datetime.now(),
            # and we expect exactly two calls to be made in the called function:
            #  - Once on entry to get the current time prior to the protected action
            #  - Once on exit to set the timestamp after the protected action.
            # We expect no sleeps, so that doesn't play in.
            assert (t1 - t0).total_seconds() == 2

            assert my_log.log == []

            lockout_manager.wait_if_needed()

            t2 = dt.just_now()

            print("t2=", t2)

            # We've set the clock to increment 1 second on every call to datetime.datetime.now(),
            # and we expect exactly two calls to be made in the called function, plus we also
            # expect to sleep for 60 seconds of the 61 seconds it wants to reserve (one second having
            # passed since the last protected action).

            assert (t2 - t1).total_seconds() == 62

            assert my_log.log == ['Last %s attempt was at 2010-01-01 12:00:02 (1.0 seconds ago).'
                                  ' Waiting 60.0 seconds before attempting another.' % protected_action]

            my_log.log = []  # Reset the log

            dt.sleep(30)  # Simulate 30 seconds of time passing

            t3 = dt.just_now()
            print("t3=", t3)

            lockout_manager.wait_if_needed()

            t4 = dt.just_now()
            print("t4=", t4)

            # We've set the clock to increment 1 second on every call to datetime.datetime.now(),
            # and we expect exactly two calls to be made in the called function, plus we also
            # expect to sleep for 30 seconds of the 61 seconds it wants to reserve (31 seconds having
            # passed since the last protected action).

            assert (t4 - t3).total_seconds() == 32

            assert my_log.log == ['Last %s attempt was at 2010-01-01 12:01:04 (31.0 seconds ago).'
                                  ' Waiting 30.0 seconds before attempting another.' % protected_action]

    real_t1 = now()
    print("Done testing at", real_t1)
    # Whole test should happen much faster, less than a half second
    assert (real_t1 - real_t0).total_seconds() < 0.5


def test_rate_manager():

    metered_action = "simulated action"

    # PyCharm thinks this is not used. -kmp 26-Jul-2020
    # r = RateManager(interval_seconds=60, safety_seconds=1, allowed_attempts=4)

    # The function now() will get us the time. This assure us that binding datetime.datetime
    # will not be affecting us.
    now = datetime_module.datetime.now

    # real_t0 is the actual wallclock time at the start of this test. We use it only to make sure
    # that all these other tests are really going through our mock. In spite of longer mocked
    # timescales, this test should run quickly.
    real_t0 = now()
    print("Starting test at", real_t0)

    # dt will be our substitute for datetime.datetime.
    # (it also has a sleep method that we can substitute for time.sleep)
    tick = 1/128  # 0.0078125 seconds (a little less than a hundredth, but precisely representable in base 2 AND base 10
    dt = ControlledTime(tick_seconds=tick)  #

    class MockLogger:

        def __init__(self):
            self.log = []

        def reset(self):
            self.log = []

        def warning(self, msg):
            self.log.append(msg)

    with mock.patch("datetime.datetime", dt):
        with mock.patch("time.sleep", dt.sleep):
            my_log = MockLogger()

            try:
                RateManager(allowed_attempts=-7, interval_seconds=60)
            except TypeError as e:
                assert str(e) == "The allowed_attempts must be a positive integer: -7"
            else:
                raise AssertionError("Error not raised.")

            class WaitTester:

                def __init__(self, rate_manager):
                    self.count = 0
                    self.expected_wait_max_seconds = 0
                    self.rate_manager = rate_manager
                    rate_manager.set_wait_hook(self.noticer)

                def expect_to_wait(self, max_seconds):
                    self.expected_wait_max_seconds = max_seconds

                def noticer(self, wait_seconds, next_expiration):
                    self.count += 1
                    print("-----")
                    for i, item in enumerate(self.rate_manager.timestamps):
                        print("* " if item == next_expiration else "  ", "Slot", i, "is", item, )
                    print("Expected wait is", self.expected_wait_max_seconds)
                    print("Actual wait is", wait_seconds)
                    assert wait_seconds < self.expected_wait_max_seconds
                    print("-----")

            rate_manager = RateManager(interval_seconds=60, safety_seconds=1, action=metered_action,
                                       allowed_attempts=4, log=my_log)

            wait_tester = WaitTester(rate_manager)

            print("Part A")

            t0 = dt.just_now()
            rate_manager.wait_if_needed()  #
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            t1 = dt.now()  # Consumes 1 clock tick
            print("t0=", t0)
            print("t1=", t1)
            assert (t1 - t0).total_seconds() <= 1  # 4 ticks is MUCH less than one second, even with roundoff error
            assert wait_tester.count == 0

            print(json.dumps(my_log.log, indent=2))
            assert my_log.log == []

            my_log.reset()

            print("Part B")

            t0 = dt.just_now()
            wait_tester.expect_to_wait(62)
            rate_manager.wait_if_needed()  # This will have to wait approximately 61 seconds.
            wait_tester.expect_to_wait(0)
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            t1 = dt.now()  # Consumes 1 clock tick
            print("t0=", t0)
            print("t1=", t1)
            wait_seconds = (t1 - t0).total_seconds()
            assert wait_seconds > 55, "Wait time (%s seconds) was shorter than expected." % wait_seconds
            assert wait_seconds <= 65, "Wait time (%s seconds) was longer than expected." % wait_seconds
            assert wait_tester.count == 1

            print(json.dumps(my_log.log, indent=2))
            [log_msg_1] = my_log.log
            assert re.match("Waiting 6[0-9][.][0-9]* seconds before attempting simulated action[.]", log_msg_1)

            my_log.reset()

            print("Part C")

            dt.sleep(120)  # This will clear all previous uses
            t0 = dt.just_now()
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            dt.sleep(30)
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            wait_tester.expect_to_wait(32)
            rate_manager.wait_if_needed()  # This should have to wait 32 seconds
            wait_tester.expect_to_wait(0)
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            dt.sleep(35)
            rate_manager.wait_if_needed()  # Consumes 1 clock tick, consuming an expired item
            rate_manager.wait_if_needed()  # Consumes 1 clock tick, because we just waited
            t1 = dt.now()  # Consumes 1 clock tick
            print("t0=", t0)
            print("t1=", t1)
            expected_wait = 30 + 32 + 35
            print("expected_wait=", expected_wait)
            wait_seconds = (t1 - t0).total_seconds()
            print("actual_wait=", wait_seconds)
            assert wait_seconds > expected_wait - 5, "Wait time (%s seconds) was shorter than expected." % wait_seconds
            assert wait_seconds <= expected_wait + 5, "Wait time (%s seconds) was longer than expected." % wait_seconds
            assert wait_tester.count == 2

            print(json.dumps(my_log.log, indent=2))
            [log_msg_1] = my_log.log
            assert re.match("Waiting 3[0-9][.][0-9]* seconds before attempting simulated action[.]", log_msg_1)

            my_log.reset()

            print("Part D")

            dt.sleep(120)  # This will clear all previous uses
            t0 = dt.just_now()
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            dt.sleep(25)
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            wait_tester.expect_to_wait(37)
            rate_manager.wait_if_needed()  # This should have to wait 37 seconds
            wait_tester.expect_to_wait(0)
            rate_manager.wait_if_needed()  # Consumes 1 clock tick. This would've had to wait, but we already did.
            dt.sleep(25)
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            dt.sleep(25)
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            wait_tester.expect_to_wait(12)
            rate_manager.wait_if_needed()  # This should have to wait 12 seconds.
            rate_manager.wait_if_needed()  # Consumes 1 clock tick
            t1 = dt.now()  # Consumes 1 clock tick
            print("t0=", t0)
            print("t1=", t1)
            expected_wait = 25 + 37 + 25 + 25 + 12
            print("expected_wait=", expected_wait)
            wait_seconds = (t1 - t0).total_seconds()
            print("actual_wait=", wait_seconds)
            assert wait_seconds > expected_wait - 5, "Wait time (%s seconds) was shorter than expected." % wait_seconds
            assert wait_seconds <= expected_wait + 5, "Wait time (%s seconds) was longer than expected." % wait_seconds
            assert wait_tester.count == 4

            print(json.dumps(my_log.log, indent=2))
            [log_msg_1, log_msg_2] = my_log.log
            assert re.match("Waiting 3[0-9][.][0-9]* seconds before attempting simulated action[.]", log_msg_1)
            assert re.match("Waiting 1[0-9][.][0-9]* seconds before attempting simulated action[.]", log_msg_2)

            my_log.reset()

            print("End of Parts")

    real_t1 = now()
    print("Done testing at", real_t1)
    # Whole test should happen much faster, less than a half second
    assert (real_t1 - real_t0).total_seconds() < 0.5


def test_environ_bool():

    with override_environ(FOO=None):
        assert environ_bool("FOO") is False
        assert environ_bool("FOO", default=None) is None
        assert environ_bool("FOO", None) is None

    with override_environ(FOO="TRUE"):
        assert environ_bool("FOO") is True
        assert environ_bool("FOO", default=None) is True
        assert environ_bool("FOO", None) is True

    with override_environ(FOO="TrUe"):  # Actually, any case should work
        assert environ_bool("FOO") is True
        assert environ_bool("FOO", default=None) is True
        assert environ_bool("FOO", None) is True

    with override_environ(FOO="FALSE"):
        assert environ_bool("FOO") is False
        assert environ_bool("FOO", default=None) is False
        assert environ_bool("FOO", None) is False

    with override_environ(FOO="anything"):
        assert environ_bool("FOO") is False
        assert environ_bool("FOO", default=None) is False
        assert environ_bool("FOO", None) is False

    with override_environ(FOO=""):
        assert environ_bool("FOO") is False
        assert environ_bool("FOO", default=None) is False
        assert environ_bool("FOO", None) is False


def test_check_true():

    x = [1, 2, 3]
    check_true(x == [1, 2, 3], "x is not a list of one, two, and three.")

    msg = "x is not a list of four, five, and six."
    with pytest.raises(RuntimeError) as e:
        check_true(x == [4, 5, 6], msg)
    assert msg in str(e)


def test_remove_element():

    old = ['a', 'b', 'c', 'a', 'b', 'c']
    new = remove_element('b', old)
    assert old is not new
    assert old == ['a', 'b', 'c', 'a', 'b', 'c']
    assert new == ['a', 'c', 'a', 'b', 'c']

    new = remove_element('z', old, raise_error=False)
    assert old is not new
    assert new == ['a', 'b', 'c', 'a', 'b', 'c']
    assert old == ['a', 'b', 'c', 'a', 'b', 'c']

    with pytest.raises(ValueError):
        remove_element('z', old)
    assert old == ['a', 'b', 'c', 'a', 'b', 'c']


def test_remove_prefix():
    assert remove_prefix("foo:", "foo:bar") == "bar"
    assert remove_prefix("foo:", "foo:bar", required=False) == "bar"
    assert remove_prefix("foo:", "foo:bar", required=True) == "bar"

    assert remove_prefix("foo:", "baz:bar") == "baz:bar"
    assert remove_prefix("foo:", "baz:bar", required=False) == "baz:bar"
    with pytest.raises(ValueError):
        assert remove_prefix("foo:", "baz:bar", required=True)

    assert remove_prefix("foo:", "foo:foo:bar") == "foo:bar"
    assert remove_prefix("foo:", "baz:foo:bar") == "baz:foo:bar"

    assert remove_prefix("", "foo") == "foo"
    assert remove_prefix("", "foo", required=False) == "foo"
    assert remove_prefix("", "foo", required=True) == "foo"


def test_remove_suffix():
    assert remove_suffix(":bar", "foo:bar") == "foo"
    assert remove_suffix(":bar", "foo:bar", required=False) == "foo"
    assert remove_suffix(":bar", "foo:bar", required=True) == "foo"

    assert remove_suffix(":baz", "foo:bar") == "foo:bar"
    assert remove_suffix(":baz", "foo:bar", required=False) == "foo:bar"
    with pytest.raises(ValueError):
        assert remove_suffix(":baz", "foo:bar", required=True)

    assert remove_suffix(":bar", "foo:bar:bar") == "foo:bar"
    assert remove_suffix(":bar", "foo:bar:baz") == "foo:bar:baz"

    assert remove_suffix("", "foo") == "foo"
    assert remove_suffix("", "foo", required=False) == "foo"
    assert remove_suffix("", "foo", required=True) == "foo"


def test_full_class_name():

    assert full_class_name(3) == 'int'
    assert full_class_name(botocore.exceptions.BotoCoreError()) == "botocore.exceptions.BotoCoreError"


def test_full_object_name():

    assert full_object_name(type(3)) == 'int'
    assert full_object_name(botocore.exceptions.BotoCoreError) == "botocore.exceptions.BotoCoreError"
    assert full_object_name(3) is None
    assert full_object_name('foo') is None
    assert full_object_name(full_object_name) == 'dcicutils.misc_utils.full_object_name'


def test_constantly():

    five = constantly(5)

    assert five() == 5
    assert five(13) == 5
    assert five(nobody='cares') == 5
    assert five(0, 1, 2, fourth=3, fifth=4) == 5

    assert five() + five() == 10

    arbitrariness = 1000000
    randomness = constantly(random.randint(1, arbitrariness))
    assert randomness() < arbitrariness + 1
    assert randomness() > 0
    assert randomness() - randomness() == 0
    assert randomness() == randomness()


def test_keyword_as_title():

    assert keyword_as_title('foo') == 'Foo'
    assert keyword_as_title('some_text') == 'Some Text'
    assert keyword_as_title('some text') == 'Some Text'
    assert keyword_as_title('SOME_TEXT') == 'Some Text'

    # Hyphens are unchanged.
    assert keyword_as_title('SOME-TEXT') == 'Some-Text'
    assert keyword_as_title('mary_smith-jones') == 'Mary Smith-Jones'


def test_file_contents():

    mfs = MockFileSystem()

    with mock.patch("io.open", mfs.open):

        with io.open("foo.txt", 'w') as fp:
            print("foo", file=fp)
            print("bar", file=fp)

        assert file_contents("foo.txt") == "foo\nbar\n"
        assert file_contents("foo.txt", binary=True) == 'foo\nbar\n'.encode('utf-8')
        assert file_contents("foo.txt", binary=True) == b'\x66\x6f\x6f\x0a\x62\x61\x72\x0a'

        with io.open("foo.bin", 'wb') as fp:
            fp.write(bytes([72, 101]))
            fp.write(bytes([108, 108, 111, 33, 10]))

        assert file_contents("foo.bin", binary=True) == b'\x48\x65\x6c\x6c\x6f\x21\x0a'
        assert file_contents("foo.bin", binary=False) == b'\x48\x65\x6c\x6c\x6f\x21\x0a'.decode('utf-8')
        assert file_contents("foo.bin", binary=False) == 'Hello!\n'


def test_make_counter():

    counter = make_counter()
    assert counter() == 0
    assert counter() == 1
    assert counter() == 2

    array_counter = make_counter([], step=[0])
    assert array_counter() == [0]
    assert array_counter() == [0, 0]
    assert array_counter() == [0, 0, 0]

    string_counter = make_counter('', step='.')
    assert string_counter() == ''
    assert string_counter() == '.'
    assert string_counter() == '..'


class TestCachedField:

    DEFAULT_TIMEOUT = 600

    def test_cached_field_basic(self):
        def simple_update_function():
            return random.choice(range(10000))

        field = CachedField('simple1', update_function=simple_update_function)
        assert field.value is not None
        current = field.get()
        assert current == field.value
        assert field.get_updated() != current
        assert field.timeout == self.DEFAULT_TIMEOUT
        field.set_timeout(30)
        assert field.timeout == 30

    def test_cached_field_mocked(self):
        dt = ControlledTime()

        with mock.patch.object(datetime_module, "datetime", dt):
            field = CachedField('simple1', update_function=make_counter())

            assert field.value is not None

            # Get a value, which should not be changing over short periods of time.
            val1 = field.get()

            assert field.value == val1
            assert field.get() == val1

            assert field.value == val1
            assert field.get() == val1

            # Forcing an update even though not much time has passed. Field value should be updated.
            val2 = field.get_updated()

            assert val2 != val1
            assert field.value != val1
            assert field.value == val2

            # Immediately recheck value, but accepting cache value. Field value should be unchanged.
            val3 = field.get()

            assert val3 == val2
            assert field.value == val2
            assert field.value == val3

            # Wait a while, but not enough to trigger cache update. Field value should be unchanged.
            dt.sleep(self.DEFAULT_TIMEOUT / 2)
            val4 = field.get()

            assert val2 == val4
            assert val2 == field.value
            assert val4 == field.value

            # Wait a bit longer, enough to trigger cache update. Field value should be updated.

            dt.sleep(self.DEFAULT_TIMEOUT / 2)  # This should push us into the cache refill time
            val5 = field.get()

            assert val2 != val5
            assert field.value != val2
            assert field.value == val5

            assert field.get() == val5  # This is the new stable value until next cache timeout
            assert field.get() == val5
            assert field.get() == val5
            assert field.get() == val5

            dt.sleep(self.DEFAULT_TIMEOUT)  # Fast forward to where we're going to refill again
            assert field.get() != val5

    def test_cached_field_timeout(self):
        field = CachedField('simple1', update_function=make_counter())

        assert field.timeout == self.DEFAULT_TIMEOUT
        field.set_timeout(30)
        assert field.timeout == 30


@pytest.mark.parametrize('token, expected', [
    ('VariantSample', 'variant_sample'),
    ('Variant', 'variant'),
    ('HiglassViewConfig', 'higlass_view_config'),
    ('ABCD', 'a_b_c_d'),
    ('', ''),
    ('Oneverylongthing1234567895D', 'oneverylongthing1234567895_d'),
    ('XMLContainer', 'x_m_l_container'),
])
def test_camel_case_to_snake_case(token, expected):
    assert camel_case_to_snake_case(token) == expected


@pytest.mark.parametrize('token, expected', [
    ('variant_sample', 'VariantSample'),
    ('variant', 'Variant'),
    ('higlass_view_config', 'HiglassViewConfig'),
    ('a_b_c_d', 'ABCD'),
    ('', ''),
    ('oneverylongthing1234567895_d', 'Oneverylongthing1234567895D'),
    ('x_m_l_container', 'XMLContainer'),
    ('X_M_L_Container', 'XMLContainer'),
])
def test_snake_case_to_camel_case(token, expected):
    assert snake_case_to_camel_case(token) == expected


@pytest.mark.parametrize('obj', [
    {},
    {'hello': 'world'},
    {'foo': 5},
    {'list': ['a', 'b', 'c']},
    {'list2': [1, 2, 3]},
    {'list_of_objects': [
        {'hello': 'world', 'foo': 'bar'},
        {'hello': 'dog', 'foo': 'cat'}
    ]},
    {'object': {'of objects': {'of more objects': {'even more': 'and more'}}}}
])
def test_copy_json(obj):
    """ Tests some basic cases for copy_json """
    assert copy_json(obj) == obj


def test_copy_json_side_effects():
    obj = {'foo': [1, 2, 3], 'bar': [{'x': 4, 'y': 5}, {'x': 2, 'y': 7}]}
    obj_copy = copy_json(obj)
    obj['foo'][1] = 20
    obj['bar'][0]['y'] = 500  # NoQA - PyCharm wrongly fears there are type errors in this line, that it will fail.
    obj['bar'][1] = 17
    assert obj == {'foo': [1, 20, 3], 'bar': [{'x': 4, 'y': 500}, 17]}
    assert obj_copy == {'foo': [1, 2, 3], 'bar': [{'x': 4, 'y': 5}, {'x': 2, 'y': 7}]}


class SampleClass:

    def __init__(self, favorite_fruit):
        self.favorite_fruit = favorite_fruit

    FAVORITE_COLOR = CustomizableProperty('FAVORITE_COLOR', description="the string name of a color")


class SampleClass2(SampleClass):

    FAVORITE_SONG = CustomizableProperty('FAVORITE_SONG', description="the string name of a song")


class SampleClass3(SampleClass2):

    FAVORITE_COLOR = 'blue'
    FAVORITE_SONG = 'Jingle Bells'


def test_find_field_declared_class():

    thing = SampleClass2(favorite_fruit='orange')

    [kind, value] = UncustomizedInstance._find_field_declared_class(thing, 'favorite_fruit')
    assert kind == 'instance'
    assert value == 'orange'

    [kind, value] = UncustomizedInstance._find_field_declared_class(thing, 'FAVORITE_SONG')
    assert kind == SampleClass2
    ignored(value)

    [kind, value] = UncustomizedInstance._find_field_declared_class(thing, 'FAVORITE_COLOR')
    assert kind == SampleClass
    ignored(value)

    with pytest.raises(RuntimeError):
        # If we were to search for something that simply wasn't a property, customizable or not,
        # the search would fail and a RuntimeError would be raised.
        UncustomizedInstance._find_field_declared_class(thing, 'FAVORITE_SHOW')


def test_uncustomized_instance():

    test_class_prefix = __name__

    thing = SampleClass2(favorite_fruit=CustomizableProperty('favorite_fruit',
                                                             description="the string name of a fruit"))

    assert str(UncustomizedInstance(thing, field='FAVORITE_SONG')) == (
        "Attempt to access field FAVORITE_SONG from class %s.SampleClass2."
        " It was expected to be given a custom value in a subclass: the string name of a song."
        % test_class_prefix
    )

    assert str(UncustomizedInstance(thing, field='FAVORITE_COLOR')) == (
        "Attempt to access field FAVORITE_COLOR from class %s.SampleClass."
        " It was expected to be given a custom value in a subclass: the string name of a color."
        % test_class_prefix
    )

    assert str(UncustomizedInstance(thing, field='favorite_fruit')) == (
        "Attempt to access field favorite_fruit from instance."
        " It was expected to be given a custom value in a subclass: the string name of a fruit."
    )


def test_customized_instance():

    test_class_prefix = __name__

    uncustomized_thing = SampleClass2(favorite_fruit=CustomizableProperty('favorite_fruit',
                                                                          description="the string name of a fruit"))

    # It doesn't work to store these things directly in the slot
    assert isinstance(uncustomized_thing.favorite_fruit, CustomizableProperty)
    assert isinstance(getattr(uncustomized_thing, "favorite_fruit"), CustomizableProperty)
    # But this will spot it...
    with printed_output() as printed:
        with raises_regexp(UncustomizedInstance, "Attempt to access field favorite_fruit from instance."
                                                 " It was expected to be given a custom value in a subclass:"
                                                 " the string name of a fruit."):
            getattr_customized(uncustomized_thing, "favorite_fruit")
        assert printed.last is None

    with printed_output() as printed:
        with raises_regexp(UncustomizedInstance, "Attempt to access field FAVORITE_COLOR"
                                                 " from class %s.SampleClass."
                                                 " It was expected to be given a custom value in a subclass:"
                                                 " the string name of a color."
                                                 % test_class_prefix):
            PRINT(uncustomized_thing.FAVORITE_COLOR)
        assert printed.last is None
    with printed_output() as printed:
        with raises_regexp(UncustomizedInstance, "Attempt to access field FAVORITE_COLOR"
                                                 " from class %s.SampleClass."
                                                 " It was expected to be given a custom value in a subclass:"
                                                 " the string name of a color."
                                                 % test_class_prefix):
            PRINT(getattr(uncustomized_thing, "FAVORITE_COLOR"))
        assert printed.last is None
    with printed_output() as printed:
        with raises_regexp(UncustomizedInstance, "Attempt to access field FAVORITE_COLOR"
                                                 " from class %s.SampleClass."
                                                 " It was expected to be given a custom value in a subclass:"
                                                 " the string name of a color."
                                                 % test_class_prefix):
            PRINT(getattr_customized(uncustomized_thing, "FAVORITE_COLOR"))
        assert printed.last is None

    with printed_output() as printed:
        with raises_regexp(UncustomizedInstance, "Attempt to access field FAVORITE_SONG"
                                                 " from class %s.SampleClass2."
                                                 " It was expected to be given a custom value in a subclass:"
                                                 " the string name of a song."
                                                 % test_class_prefix):
            PRINT(uncustomized_thing.FAVORITE_SONG)
        assert printed.last is None
    with printed_output() as printed:
        with raises_regexp(UncustomizedInstance, "Attempt to access field FAVORITE_SONG"
                                                 " from class %s.SampleClass2."
                                                 " It was expected to be given a custom value in a subclass:"
                                                 " the string name of a song."
                                                 % test_class_prefix):
            PRINT(getattr(uncustomized_thing, "FAVORITE_SONG"))
        assert printed.last is None
    with printed_output() as printed:
        with raises_regexp(UncustomizedInstance, "Attempt to access field FAVORITE_SONG"
                                                 " from class %s.SampleClass2."
                                                 " It was expected to be given a custom value in a subclass:"
                                                 " the string name of a song."
                                                 % test_class_prefix):
            PRINT(getattr_customized(uncustomized_thing, "FAVORITE_SONG"))
        assert printed.last is None

    customized_thing = SampleClass3(favorite_fruit='orange')

    assert customized_thing.favorite_fruit == 'orange'
    assert customized_thing.FAVORITE_COLOR == 'blue'
    assert customized_thing.FAVORITE_SONG == 'Jingle Bells'


def test_customized_class():

    test_class_prefix = __name__

    with printed_output() as printed:
        PRINT(SampleClass.FAVORITE_COLOR)
        assert printed.last == "<CustomizableProperty FAVORITE_COLOR>"
    with printed_output() as printed:
        PRINT(getattr(SampleClass, 'FAVORITE_COLOR'))
        assert printed.last == "<CustomizableProperty FAVORITE_COLOR>"
    with printed_output() as printed:
        with raises_regexp(UncustomizedInstance, "Attempt to access field FAVORITE_COLOR"
                                                 " from class %s.SampleClass."
                                                 " It was expected to be given a custom value in a subclass:"
                                                 " the string name of a color."
                                                 % test_class_prefix):
            PRINT(getattr_customized(SampleClass, 'FAVORITE_COLOR'))
        assert printed.last is None

    with printed_output() as printed:
        PRINT(SampleClass2.FAVORITE_COLOR)
        assert printed.last == "<CustomizableProperty FAVORITE_COLOR>"
    with printed_output() as printed:
        PRINT(getattr(SampleClass2, 'FAVORITE_COLOR'))
        assert printed.last == "<CustomizableProperty FAVORITE_COLOR>"
    with printed_output() as printed:
        with raises_regexp(UncustomizedInstance, "Attempt to access field FAVORITE_COLOR"
                                                 " from class %s.SampleClass."
                                                 " It was expected to be given a custom value in a subclass:"
                                                 " the string name of a color."
                                                 % test_class_prefix):
            PRINT(getattr_customized(SampleClass2, 'FAVORITE_COLOR'))
        assert printed.last is None

    with printed_output() as printed:
        PRINT(SampleClass2.FAVORITE_SONG)
        assert printed.last == "<CustomizableProperty FAVORITE_SONG>"
    with printed_output() as printed:
        PRINT(getattr(SampleClass2, 'FAVORITE_SONG'))
        assert printed.last == "<CustomizableProperty FAVORITE_SONG>"
    with printed_output() as printed:
        with raises_regexp(UncustomizedInstance, "Attempt to access field FAVORITE_SONG"
                                                 " from class %s.SampleClass2."
                                                 " It was expected to be given a custom value in a subclass:"
                                                 " the string name of a song."
                                                 % test_class_prefix):
            PRINT(getattr_customized(SampleClass2, 'FAVORITE_SONG'))
        assert printed.last is None


def test_url_path_join():

    assert url_path_join('foo', 'bar') == 'foo/bar'
    assert url_path_join('foo/', 'bar') == 'foo/bar'
    assert url_path_join('foo', '/bar') == 'foo/bar'
    assert url_path_join('foo/', '/bar') == 'foo/bar'
    assert url_path_join('//foo//', '///bar//') == '//foo/bar//'
