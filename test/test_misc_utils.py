import botocore.exceptions
import datetime as datetime_module
import functools
import io
import json
import os
import pytest
import pytz
import random
import re
import time
import uuid
import warnings
import webtest

from dcicutils import misc_utils as misc_utils_module
from dcicutils.misc_utils import (
    PRINT, ignored, ignorable, filtered_warnings, get_setting_from_context, TestApp, VirtualApp, VirtualAppError,
    _VirtualAppHelper,  # noqa - yes, this is a protected member, but we still want to test it
    Retry, apply_dict_overrides, utc_today_str, RateManager, environ_bool, str_to_bool, is_uuid,
    LockoutManager, check_true, remove_prefix, remove_suffix, full_class_name, full_object_name, constantly,
    keyword_as_title, file_contents, CachedField, camel_case_to_snake_case, snake_case_to_camel_case, make_counter,
    CustomizableProperty, UncustomizedInstance, getattr_customized, copy_json, url_path_join,
    as_seconds, ref_now, in_datetime_interval, as_datetime, as_ref_datetime, as_utc_datetime, REF_TZ,
    DatetimeCoercionFailure, remove_element, identity, count, count_if, find_association, find_associations,
    ancestor_classes, is_proper_subclass, decorator, is_valid_absolute_uri, override_environ, override_dict,
    capitalize1, local_attrs, dict_zip, json_leaf_subst, print_error_message, get_error_message, utc_now_str,
    _is_function_of_exactly_one_required_arg, _apply_decorator,  _36_DIGITS, _mockable_input, # noQA
    string_list, string_md5, SingletonManager, key_value_dict, merge_key_value_dict_lists, lines_printed_to,
    classproperty, classproperty_cached, classproperty_cached_each_subclass, Singleton, NamedObject, obsolete,
    ObsoleteError, CycleError, TopologicalSorter, keys_and_values_to_dict, dict_to_keys_and_values, is_c4_arn,
    deduplicate_list, chunked, parse_in_radix, format_in_radix, managed_property, future_datetime,
    MIN_DATETIME, MIN_DATETIME_UTC, INPUT, builtin_print, map_chunked, to_camel_case, json_file_contents,
    pad_to, JsonLinesReader, split_string, merge_objects, to_float, to_integer,
    load_json_from_file_expanding_environment_variables, create_readonly_object
)
from dcicutils.qa_utils import (
    Occasionally, ControlledTime, override_environ as qa_override_environ, MockFileSystem, printed_output,
    raises_regexp, MockId, MockLog, input_series,
)
from dcicutils.tmpfile_utils import temporary_file
from typing import Any, Dict, List
from unittest import mock


def test_uppercase_print():
    # This is just a synonym, so the easiest thing is just to test that fact.
    assert PRINT.wrapped_action == print

    # But also a basic test that it does something
    s = io.StringIO()
    PRINT("something", file=s)
    assert s.getvalue() == "something\n"


def test_uppercase_input():
    print()  # start on a fresh line

    # This is just a synonym, so the easiest thing is just to test that fact.
    assert INPUT.wrapped_action == _mockable_input

    # But also a basic test that it does something
    some_response = "some response"
    some_prompt = "foo?"
    with printed_output() as printed:
        with input_series(some_response):
            assert INPUT(some_prompt) == some_response
    assert printed.lines == [some_prompt]


def test_builtin_print():

    with printed_output() as print:
        s = io.StringIO()
        builtin_print("foo", file=s)
        assert s.getvalue() == "foo\n"
    assert print.lines == []  # builtin_print does not cooperate in this stuff


def test_ignored():
    def foo(x, y):
        ignored(x, y)
    # Check that no error occurs for having used this.
    assert foo(3, 4) is None


def test_ignorable():
    def foo(x, y):
        ignorable(x, y)
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

    def post(self, url, obj, **kwargs):
        call_info = {'op': 'post', 'url': url, 'obj': obj, 'kwargs': kwargs}
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
    assert not test_app.__test__  # noQA - testing that we've successfully "monkey patched" this very low-level prop


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


def test_virtual_app_post():

    with mock.patch.object(VirtualApp, "HELPER_CLASS", FakeTestApp):
        app = FakeApp()
        environ = {'some': 'stuff'}
        vapp = VirtualApp(app, environ)

    log_info = []

    with mock.patch("logging.info") as mock_info:
        mock_info.side_effect = lambda msg: log_info.append(msg)

        response1 = vapp.post("http://no.such.place/", {'beta': 'gamma'})
        assert response1.json() == {
            'processed': {
                'op': 'post',
                'url': 'http://no.such.place/',
                'obj': {'beta': 'gamma'},
                'kwargs': {},
            }
        }

        response2 = vapp.post("http://no.such.place/", {'alpha': 'omega'}, params={'foo': 'bar'})
        assert response2.json() == {
            'processed': {
                'op': 'post',
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
                'op': 'post',
                'url': 'http://no.such.place/',
                'obj': {'beta': 'gamma'},
                'kwargs': {},
            },
            {
                'op': 'post',
                'url': 'http://no.such.place/',
                'obj': {'alpha': 'omega'},
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

        def post(self, url, object, **kwargs):  # noqa - the name of this argument is not chosen by us here
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
            lambda: vapp.post(some_url, "stuff"),
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
            warnings.warn("oh, what a bad user you are", RuntimeWarning)  # noqa
            warnings.warn("tsk, tsk, tsk, what ugly code", SyntaxWarning)  # noqa
            # Verify some things
            for expected_count, expected_type in pairs:
                count = 0
                for warning in w:
                    if issubclass(warning.category, expected_type):
                        count += 1
                assert count == expected_count, (
                    f"Warnings: {[e.message for e in w]} (count={count} expected{expected_count})"
                )

    expect_warnings([(2, Warning), (1, RuntimeWarning), (1, SyntaxWarning)])

    with filtered_warnings("ignore"):
        expect_warnings([(0, Warning), (0, RuntimeWarning), (0, SyntaxWarning)])

    with filtered_warnings("ignore", category=Warning):
        expect_warnings([(0, Warning), (0, RuntimeWarning), (0, SyntaxWarning)])

    with filtered_warnings("ignore", category=RuntimeWarning):
        expect_warnings([(1, Warning), (0, RuntimeWarning), (1, SyntaxWarning)])

    with filtered_warnings("ignore", category=SyntaxWarning):
        expect_warnings([(1, Warning), (1, RuntimeWarning), (0, SyntaxWarning)])


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


def test_ref_now():

    t0 = datetime_module.datetime(2015, 7, 4, 12, 0, 0)
    dt = ControlledTime(t0)

    with mock.patch("dcicutils.misc_utils.datetime", dt):

        t1 = ref_now()
        t1_utc = t1.replace(tzinfo=pytz.UTC)
        assert t1.replace(tzinfo=None) == t0 + datetime_module.timedelta(seconds=1)
        delta = (t1 - t1_utc)
        delta_seconds = delta.total_seconds()
        delta_hours = delta_seconds / 3600
        assert delta_hours in {4.0, 5.0}  # depending on daylight savings time, HMS is either 4 or 5 hours off from UTC


@pytest.fixture
def three_things():
    one = {'value': 1, 'english': 'one', 'spanish': 'uno'}
    two = {'value': 2, 'english': 'two', 'spanish': 'dos'}
    three = {'value': 3, 'english': 'three', 'spanish': 'tres'}
    return [one, two, three]


def test_find_association(three_things):
    one, two, three = three_things
    assert find_association(three_things, value=1) == one
    assert find_association(three_things, spanish='dos') == two
    assert find_association(three_things, spanish='dos', value=2) == two
    assert find_association(three_things, spanish='dos', value=3) is None  # find_associations would return []


def test_find_association_functional(three_things):
    one, two, three = three_things
    assert find_association(three_things, value=lambda x: x % 2 == 0) == two
    with pytest.raises(Exception):
        find_association(three_things, value=lambda x: x % 2 == 1)   # find_associations would return [one, three]


def test_find_associations(three_things):
    one, two, three = three_things
    assert find_associations(three_things, value=1) == [one]
    assert find_associations(three_things, spanish='dos') == [two]
    assert find_associations(three_things, spanish='dos', value=2) == [two]
    assert find_associations(three_things, spanish='dos', value=3) == []


def test_find_associations_functional(three_things):
    one, two, three = three_things
    assert find_associations(three_things, value=lambda x: x % 2 == 1) == [one, three]


def test_as_datetime():

    t0 = datetime_module.datetime(2015, 7, 4, 12, 0, 0)
    t0_utc = pytz.UTC.localize(datetime_module.datetime(2015, 7, 4, 12, 0, 0))  # noQA - PyCharm wrongly fusses at this

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
    t0_utc = pytz.UTC.localize(datetime_module.datetime(2015, 7, 4, 12, 0, 0))  # noQA - PyCharm wrongly fusses at this
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
    t0_utc = pytz.UTC.localize(datetime_module.datetime(2015, 7, 4, 12, 0, 0))  # noQA - PyCharm wrongly fusses at this
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

    # The function now() will get us the time. This assures us that binding datetime.datetime
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

    # The function now() will get us the time. This assures us that binding datetime.datetime
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

    with qa_override_environ(FOO=None):
        assert environ_bool("FOO") is False
        assert environ_bool("FOO", default=None) is None
        assert environ_bool("FOO", None) is None

    with qa_override_environ(FOO="TRUE"):
        assert environ_bool("FOO") is True
        assert environ_bool("FOO", default=None) is True
        assert environ_bool("FOO", None) is True

    with qa_override_environ(FOO="TrUe"):  # Actually, any case should work
        assert environ_bool("FOO") is True
        assert environ_bool("FOO", default=None) is True
        assert environ_bool("FOO", None) is True

    with qa_override_environ(FOO="FALSE"):
        assert environ_bool("FOO") is False
        assert environ_bool("FOO", default=None) is False
        assert environ_bool("FOO", None) is False

    with qa_override_environ(FOO="anything"):
        assert environ_bool("FOO") is False
        assert environ_bool("FOO", default=None) is False
        assert environ_bool("FOO", None) is False

    with qa_override_environ(FOO=""):
        assert environ_bool("FOO") is False
        assert environ_bool("FOO", default=None) is False
        assert environ_bool("FOO", None) is False


def test_str_to_bool():

    assert str_to_bool(None) is None

    assert str_to_bool("TRUE") is True
    assert str_to_bool("True") is True
    assert str_to_bool("trUe") is True
    assert str_to_bool("TrUe") is True

    assert str_to_bool("FALSE") is False
    assert str_to_bool("False") is False
    assert str_to_bool("false") is False
    assert str_to_bool("FaLsE") is False

    assert str_to_bool("") is False
    assert str_to_bool("anything") is False

    for x in [0, 1, -1, 17, True, False, [], [17], {}, {"some": "thing"}]:
        with pytest.raises(ValueError):
            str_to_bool(x)  # NoQA - We're testing that this will err, so no need for PyCharm to do syntax warning


def test_check_true():

    x = [1, 2, 3]
    check_true(x == [1, 2, 3], "x is not a list of one, two, and three.")

    msg = "x is not a list of four, five, and six."
    with pytest.raises(RuntimeError) as e:
        check_true(x == [4, 5, 6], msg)
    assert msg in str(e.value)


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


def test_ancestor_classes():
    assert ancestor_classes(int) == [object]

    class Foo:
        pass

    assert ancestor_classes(Foo) == [object]

    class FooBar(Foo):
        pass

    assert ancestor_classes(FooBar) == [Foo, object]
    assert ancestor_classes(FooBar, reverse=True) == [object, Foo]


def test_is_function_of_exactly_one_required_arg():

    def f0():
        return ['n_args', 0]

    def f1(x):
        ignored(x)
        return ['n_args', 1]

    def f2(x, y):
        ignored(x, y)
        return ['n_args', 2]

    def f0_k1(*, k):
        ignored(k)
        return ['n_args', 0, 'n_kwargs', 1]

    def f1_k1(*, k):
        ignored(k)
        return ['n_args', 1, 'n_kwargs', 1]

    def f2_k1(*, k):
        ignored(k)
        return ['n_args', 2, 'n_kwargs', 1]

    def f0_1(x=0):
        ignored(x)
        return ['n_args_min', 0, 'n_args_max', 1]

    def f0_2(x=0, y=0):
        ignored(x, y)
        return ['n_args_min', 0, 'n_args_max', 1]

    def f0_k0_1(*, k=0):
        ignored(k)
        return ['n_args', 0, 'n_kwargs_min', 0, 'n_kwargs_max', 1]

    assert _is_function_of_exactly_one_required_arg(f0) is False
    assert _is_function_of_exactly_one_required_arg(f1) is True
    assert _is_function_of_exactly_one_required_arg(f2) is False

    assert _is_function_of_exactly_one_required_arg(f0_k1) is False
    assert _is_function_of_exactly_one_required_arg(f1_k1) is False
    assert _is_function_of_exactly_one_required_arg(f2_k1) is False

    assert _is_function_of_exactly_one_required_arg(f0_1) is False
    assert _is_function_of_exactly_one_required_arg(f0_2) is False
    assert _is_function_of_exactly_one_required_arg(f0_k0_1) is False

    assert _is_function_of_exactly_one_required_arg(17) is False


def test_apply_decorator():

    print()

    def foo(outer1=None, outer2=None, inner1=None, inner2=None):
        return {'outer1': outer1, 'outer2': outer2, 'inner1': inner1, 'inner2': inner2}

    def foo_positional(inner1):
        return {'inner1': inner1}

    def curryize(outer1=None, outer2=None):
        def _curried(fn):
            @functools.wraps(fn)
            def _foo(*args, **kwargs):
                if outer1 is not None:
                    kwargs['outer1'] = outer1
                if outer2 is not None:
                    kwargs['outer2'] = outer2
                return {'wrapped': fn(*args, **kwargs)}
            return _foo
        return _curried

    def never_called(*args, **kwargs):
        ignored(args, kwargs)
        raise NotImplementedError("this should never be called")

    print("Testing case 1")

    # Case 1: Mixing positionals and keys is not allowed.
    with pytest.raises(SyntaxError):
        _apply_decorator(never_called, 17, somekey=1)

    # Case1: More than one positional is not allowed
    # Two or more arguments is ambiguous and not allowed.
    with pytest.raises(SyntaxError):
        _apply_decorator(never_called, 17, 18)

    print("Testing case 2A")

    assert _apply_decorator(curryize(outer1=1, outer2=2), foo)(inner1=3, inner2=4) == {
        'wrapped': {
            'outer1': 1,
            'outer2': 2,
            'inner1': 3,
            'inner2': 4
        }
    }

    print("Testing case 2B")

    assert _apply_decorator(curryize, foo_positional)(3) == {
        'wrapped': {
            'inner1': 3,
        }
    }

    print("Testing case 3")

    # Zero arguments means give back a wrapped function
    # Corresponds to
    #   @my_decorator
    #   def foo_curried(y, z): ...
    #  so there is no opportunity to supply w and x early.

    assert _apply_decorator(curryize)(foo)(inner1=3, inner2=4) == {
        'wrapped': {
            'outer1': None,
            'outer2': None,
            'inner1': 3,
            'inner2': 4
        }
    }


def test_is_proper_subclass():

    assert is_proper_subclass(object, object) is False
    assert is_proper_subclass(float, object) is True
    assert is_proper_subclass(object, float) is False
    assert is_proper_subclass(float, float) is False

    class Foo:
        pass

    class Bar(Foo):
        pass

    class Baz(Bar):
        pass

    assert is_proper_subclass(Foo, object) is True
    assert is_proper_subclass(Bar, object) is True
    assert is_proper_subclass(Baz, object) is True

    assert is_proper_subclass(Foo, Foo) is False
    assert is_proper_subclass(Bar, Foo) is True
    assert is_proper_subclass(Baz, Foo) is True


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


def test_identity():
    for x in [1, 'foo', ['x', 17]]:
        assert x == identity(x)


def test_count_if():

    list_of_numbers = [0, 10, 20, 30]
    list_of_things = [None, 1, '', 'foo', 'False', [1, 2, 3]]

    list_of_non_zero_numbers = list_of_numbers.copy()
    list_of_non_zero_numbers.remove(0)

    assert count_if(constantly(True), list_of_numbers) == len(list_of_numbers)
    assert count_if(constantly(True), list_of_things) == len(list_of_things)
    assert count_if(constantly(True), []) == 0

    assert count_if(constantly(False), list_of_numbers) == 0
    assert count_if(constantly(False), list_of_things) == 0
    assert count_if(constantly(False), []) == 0

    assert count_if(identity, list_of_numbers) == len([x for x in list_of_numbers if x])
    assert count_if(identity, list_of_things) == len([x for x in list_of_things if x])
    assert count_if(identity, []) == 0

    with pytest.raises(Exception):
        count_if(identity, None)


def test_count():

    list_of_numbers = [0, 10, 20, 30]
    list_of_things = [None, 1, '', 'foo', 'False', [1, 2, 3]]

    list_of_non_zero_numbers = list_of_numbers.copy()
    list_of_non_zero_numbers.remove(0)

    assert count(list_of_numbers, filter=constantly(True)) == len(list_of_numbers)
    assert count(list_of_things, filter=constantly(True)) == len(list_of_things)
    assert count([], filter=constantly(True)) == 0

    assert count(list_of_numbers, filter=constantly(False)) == 0
    assert count(list_of_things, filter=constantly(False)) == 0
    assert count([], filter=constantly(False)) == 0

    assert count(list_of_numbers) == len([x for x in list_of_numbers if x])
    assert count(list_of_things) == len([x for x in list_of_things if x])
    assert count([]) == 0

    with pytest.raises(Exception):
        count(None)


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


def test_json_file_contents():

    mfs = MockFileSystem()
    sample_data = {"foo": 1, "bar": [2, True]}
    with mock.patch("io.open", mfs.open):
        with io.open("foo.txt", 'w') as fp:
            json.dump(sample_data, fp)
        assert json_file_contents("foo.txt") == sample_data


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

            last_known_time_of_next_update = field.time_of_next_update

            assert field.value is not None

            # Get a value, which should not be changing over short periods of time.
            val1 = field.get()

            assert field.value == val1
            assert field.get() == val1

            assert field.value == val1
            assert field.get() == val1

            # Forcing an update even though not much time has passed. Field value should be updated.
            val2 = field.get_updated()

            assert field.time_of_next_update == last_known_time_of_next_update

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

            assert field.time_of_next_update == last_known_time_of_next_update

            assert val2 == val4
            assert val2 == field.value
            assert val4 == field.value

            # Wait a bit longer, enough to trigger cache update. Field value should be updated.

            dt.sleep(self.DEFAULT_TIMEOUT / 2)  # This should push us into the cache refill time
            val5 = field.get()

            assert field.time_of_next_update != last_known_time_of_next_update
            last_known_time_of_next_update = field.time_of_next_update

            assert val2 != val5
            assert field.value != val2
            assert field.value == val5

            assert field.get() == val5  # This is the new stable value until next cache timeout
            assert field.get() == val5
            assert field.get() == val5
            assert field.get() == val5

            dt.sleep(self.DEFAULT_TIMEOUT)  # Fast-forward to where we're going to refill again
            val6 = field.get()
            assert val6 != val5

            assert field.time_of_next_update != last_known_time_of_next_update
            last_known_time_of_next_update = field.time_of_next_update

            val7 = field.get_updated()
            assert val6 != val7

            assert field.time_of_next_update == last_known_time_of_next_update

            val8 = field.get_updated(push_ttl=True)
            assert val8 == val7 + 1
            assert field.time_of_next_update != last_known_time_of_next_update
            last_known_time_of_next_update = field.time_of_next_update

            val9 = field.get_updated(push_ttl=False)
            assert val9 == val8 + 1
            assert field.time_of_next_update == last_known_time_of_next_update

    def test_cached_field_timeout(self):
        field = CachedField('simple1', update_function=make_counter())

        assert field.timeout == self.DEFAULT_TIMEOUT
        field.set_timeout(30)
        assert field.timeout == 30

    def test_cached_field_str_and_str(self):
        def constant_17():
            return 17
        f = CachedField(name='foo', update_function=constant_17, timeout=1234)
        assert str(f) == repr(f) == "CachedField(name='foo',update_function=constant_17,timeout=1234)"


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
    ('VariantSample', 'variant-sample'),
    ('Variant', 'variant'),
    ('HiglassViewConfig', 'higlass-view-config'),
    ('ABCD', 'a-b-c-d'),
    ('', ''),
    ('Oneverylongthing1234567895D', 'oneverylongthing1234567895-d'),
    ('XMLContainer', 'x-m-l-container'),
])
def test_camel_case_to_snake_case_hyphenated(token, expected):
    assert camel_case_to_snake_case(token, separator='-') == expected


@pytest.mark.parametrize('token, expected', [
    ('variant_sample', 'VariantSample'),
    ('variant', 'Variant'),
    ('_variant_', 'Variant'),
    ('__variant', 'Variant'),
    ('higlass_view_config', 'HiglassViewConfig'),
    ('a_b_c_d', 'ABCD'),
    ('', ''),
    ('oneverylongthing1234567895_d', 'Oneverylongthing1234567895D'),
    ('x_m_l_container', 'XMLContainer'),
    ('X_M_L_Container', 'XMLContainer'),
])
def test_snake_case_to_camel_case(token, expected):
    assert snake_case_to_camel_case(token) == expected


@pytest.mark.parametrize('token, expected', [
    ('variant-sample', 'VariantSample'),
    ('variant', 'Variant'),
    ('-variant-', 'Variant'),
    ('--variant', 'Variant'),
    ('higlass-view-config', 'HiglassViewConfig'),
    ('a-b-c-d', 'ABCD'),
    ('', ''),
    ('oneverylongthing1234567895-d', 'Oneverylongthing1234567895D'),
    ('x-m-l-container', 'XMLContainer'),
    ('X-M-L-Container', 'XMLContainer'),
])
def test_snake_case_to_camel_case_hyphenated(token, expected):
    assert snake_case_to_camel_case(token, separator='-') == expected


@pytest.mark.parametrize('token, expected', [
    ('variant_sample', 'VariantSample'),
    ('variant', 'Variant'),
    ('_variant_', 'Variant'),
    ('__variant', 'Variant'),
    ('higlass_view_config', 'HiglassViewConfig'),
    ('a_b_c_d', 'ABCD'),
    ('', ''),
    ('oneverylongthing1234567895_d', 'Oneverylongthing1234567895D'),
    ('x_m_l_container', 'XMLContainer'),
    ('X_M_L_Container', 'XMLContainer'),
])
def test_to_camel_case(token, expected):
    assert to_camel_case(token) == expected
    assert to_camel_case(token.replace('_', '-')) == expected
    assert to_camel_case(expected) == expected  # make sure it's stable


@pytest.mark.parametrize('token, expected', [
    ('', ''),
    ('x', 'X'),
    ('foo', 'Foo'),
    ('FOO', 'FOO'),
    ('Foo', 'Foo'),
    ('fooBar', 'FooBar'),
])
def test_capitalize1(token, expected):
    assert capitalize1(token) == expected


def test_is_uuid():

    good_uuid = str(uuid.uuid4())
    bad_uuid = '123-456-789'

    assert not is_uuid("12345678abcd678123456781234")  # wrong length. expecting 32 digits
    assert not is_uuid("12-3456781234abcd1234567812345678")  # hyphens only allowed at multiple of four boundaries
    assert not is_uuid("12-3456781234abcd1234567-812345678")  # ditto

    assert is_uuid("123456781234abcd1234567812345678")
    assert is_uuid("12345678abcd56781234ABCD12345678")
    assert is_uuid("1234-5678abcd56781234ABCD12345678")
    assert is_uuid("12345678abcd-56781234ABCD1234-5678")
    assert is_uuid("1234-5678-abcd56781234ABCD-12345678")
    assert is_uuid("1234-5678-abcd-56781234ABCD12345678")
    assert is_uuid("1234-5678-abcd-5678-1234-ABCD-1234-5678")
    assert is_uuid("1234-5678-abcd-5678-1234-ABCD-1234-5678-")  # we don't really want this, but we tolerate it

    assert is_uuid("{12345678abcd56781234ABCD12345678}")  # braces are optionally allowed
    assert is_uuid("{1234-5678-abcd5678-1234-ABCD-1234-5678}")  # ditto
    assert is_uuid("1234-5678-abcd5678-1234-ABCD-1234-5678}")  # ditto
    assert is_uuid("{1234-5678-abcd5678-1234-ABCD-1234-5678-}")  # balanced braces trailing hyphen tolerated

    assert is_uuid(good_uuid) is True
    assert is_uuid(bad_uuid) is False


def test_string_list():

    assert string_list('') == []
    assert string_list('foo') == ['foo']
    assert string_list('foo,bar') == ['foo', 'bar']
    assert string_list('foo,bar,baz') == ['foo', 'bar', 'baz']

    # Edge cases
    assert string_list(' ') == []
    assert string_list('  foo   ') == ['foo']
    assert string_list('  foo   ,,bar ,  ') == ['foo', 'bar']

    with pytest.raises(ValueError) as exc:
        string_list(17)
    assert str(exc.value) == "Not a string: 17"


def test_string_md5():

    # Result of "echo -n 'xyzzy' | md5" or "md5 -s 'xyzzy'"
    assert string_md5("xyzzy") == "1271ed5ef305aadabc605b1609e24c52"
    # Result of "echo 'xyzzy' | md5"
    assert string_md5("xyzzy\n") == "89d447eb9afaeb94e463615e8ded6479"


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
    obj: Dict[str, Any] = {'foo': [1, 2, 3], 'bar': [{'x': 4, 'y': 5}, {'x': 2, 'y': 7}]}
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


class SampleException(Exception):  # Used for testing print_error_message
    pass


def test_get_error_message_and_print_error_message():

    print()  # Start test output on a fresh line

    def test_error(error_descriptor, expected, expected_full):

        try:
            # Allow either of two different ways to describe an error: as an exception or a function that raises one.
            if isinstance(error_descriptor, Exception):
                raise error_descriptor
            else:
                error_descriptor()

        except Exception as exception:

            # Test the ability to get or print the error message.

            with printed_output() as printed:

                assert get_error_message(exception, full=False) == expected
                print_error_message(exception, full=False)
                assert printed.last == expected

                assert get_error_message(exception, full=True) == expected_full
                print_error_message(exception, full=True)
                assert printed.last == expected_full

            return

        raise AssertionError("The exception_amker did not raise an error.")

    empty_dict = {}

    test_error(  # The empty_dict['foo'] will unconditionally err, so we don't need the value to get used
               error_descriptor=lambda: empty_dict['foo'],
               # There is no difference between short and long forms for primitive error classes that have no module id.
               expected="KeyError: 'foo'",
               expected_full="KeyError: 'foo'")

    test_error(error_descriptor=SampleException("Something happened."),
               expected="SampleException: Something happened.",
               expected_full="test.test_misc_utils.SampleException: Something happened.")


def test_url_path_join():

    assert url_path_join('foo', 'bar') == 'foo/bar'
    assert url_path_join('foo/', 'bar') == 'foo/bar'
    assert url_path_join('foo', '/bar') == 'foo/bar'
    assert url_path_join('foo/', '/bar') == 'foo/bar'
    assert url_path_join('//foo//', '///bar//') == '//foo/bar//'


def test_decorator_for_class():

    # This test is kind of a 'test first' thing, where we first illustrate that the native decorator facility
    # has a problem that is pretty irritating. Then we check our solution doesn't have that problem.

    def check_class_decoration(the_decorator):

        @the_decorator(foo=False)
        class Decorated1:
            pass

        @the_decorator(foo=True)
        class Decorated2(Decorated1):
            pass

        @the_decorator
        class Decorated3(Decorated2):  # noQA - There's a deliberate bug on this line for testing
            pass

        assert isinstance(Decorated1, type)
        assert isinstance(Decorated2, type)
        assert isinstance(Decorated3, type)

        assert Decorated1.foo is False
        assert Decorated2.foo is True
        assert Decorated3.foo is False

    def native_decorator(foo=False):
        def _wrap(cls):
            cls.foo = foo
            return cls
        return _wrap

    with pytest.raises(Exception):
        #
        # If you were to remove the pytest.raises here, you'd see:
        #
        # >       assert isinstance(Decorated3, type)
        # E       assert False
        # E        +  where False =
        #               isinstance(<function test_decorator.<locals>.native_decorator.<locals>._wrap at 0x109ec50d0>,
        #                          type)
        #
        check_class_decoration(native_decorator)

    # It works to 'promote' a native decorator to a better decorator via function call.

    check_class_decoration(decorator(native_decorator))

    # It works the normal decorator way with @better_decorator

    @decorator  # noQA - PyCharm will incorrectly infer that this requires an argument list
    def better_decorator(foo=False):
        def _wrap(cls):
            cls.foo = foo
            return cls
        return _wrap

    check_class_decoration(better_decorator)

    # It works the normal decorator way with an arglist, as in: @another_better_decorator(...)

    @decorator()
    def another_better_decorator(foo=False):
        def _wrap(cls):
            cls.foo = foo
            return cls
        return _wrap

    check_class_decoration(another_better_decorator)


def test_decorator_for_function():

    def check_function_decoration(the_decorator):

        # Remember that the function promises not to take positional arguments.
        @the_decorator(label='eff')
        def f(x):
            return x

        assert f(4) == {'eff': 4}

        @the_decorator()
        def f(x):
            return x

        assert f(4) == {'value': 4}

        @the_decorator
        def f(x):
            return x

        assert f(4) == {'value': 4}

    def native_decorator(label='value'):
        def _wrap(fn):
            @functools.wraps(fn)
            def _wrapped(*args, **kwargs):
                return {label: fn(*args, **kwargs)}
            return _wrapped
        return _wrap

    with pytest.raises(Exception):
        check_function_decoration(native_decorator)

    # It works to 'promote' a native decorator to a better decorator via function call.

    check_function_decoration(decorator(native_decorator))

    # It works the normal decorator way with @better_decorator

    @decorator  # noQA - PyCharm will incorrectly infer that this requires an argument list
    def better_decorator(label='value'):
        def _wrap(fn):
            @functools.wraps(fn)
            def _wrapped(*args, **kwargs):
                return {label: fn(*args, **kwargs)}
            return _wrapped
        return _wrap

    check_function_decoration(better_decorator)

    # It works the normal decorator way with an arglist, as in: @another_better_decorator(...)

    @decorator()
    def another_better_decorator(label='value'):
        def _wrap(fn):
            @functools.wraps(fn)
            def _wrapped(*args, **kwargs):
                return {label: fn(*args, **kwargs)}
            return _wrapped
        return _wrap

    check_function_decoration(another_better_decorator)


def test_is_valid_absolute_uri():

    # Scheme and host are required. We want absolute URIs.
    #
    # In the rfc3987 implementation, there are a couple of cases marked that used to return true because
    # most of these were only screened out for lack of a colon (:) and those occasional cases were let
    # through improperly. Really the colon check was supposed to be checking for a scheme, but colons can
    # happen in other places and that was a lousy check. -kmp 20-Apr-2021

    assert is_valid_absolute_uri(3) is False

    assert is_valid_absolute_uri("foo") is False

    assert is_valid_absolute_uri("/foo") is False
    assert is_valid_absolute_uri("/foo.html") is False

    assert is_valid_absolute_uri("/foo?alpha") is False
    assert is_valid_absolute_uri("/foo?alpha=1") is False
    assert is_valid_absolute_uri("/foo?alpha=1&beta=2") is False

    assert is_valid_absolute_uri("//somehost") is False
    assert is_valid_absolute_uri("//somehost.example.com") is False

    assert is_valid_absolute_uri("//user@somehost") is False
    assert is_valid_absolute_uri("//user:pass@somehost") is False  # Used to wrongly yield True due to ':' in URI

    assert is_valid_absolute_uri("//somehost/") is False
    assert is_valid_absolute_uri("//somehost.example.com/") is False

    assert is_valid_absolute_uri("//somehost?alpha") is False
    assert is_valid_absolute_uri("//somehost?alpha=1") is False
    assert is_valid_absolute_uri("//somehost?alpha=1&beta=2") is False

    assert is_valid_absolute_uri("//somehost/?alpha=1&beta=2") is False

    # Used to wrongly yield True due to ':' in URI
    assert is_valid_absolute_uri("//somehost/?alpha=1&beta=2&colon=:") is False

    # Scheme provided, but no host. We want absolute URIs.
    #
    # In the rfc3987 implementation we used to use, each of the next three examples used to return False.
    # -kmp 20-Apr-2021

    assert is_valid_absolute_uri("http:/foo?alpha") is False  # Used to be True
    assert is_valid_absolute_uri("http:/foo?alpha=1") is False  # Used to be True
    assert is_valid_absolute_uri("http:/foo?alpha=1&beta=2") is False  # Used to be True

    # Scheme provided, but not the one we want.
    #
    # In the rfc3987 implementation, we allowed these through because we really just looked for a colon.
    # But we're not prepared to manage these other protocols, so we're now more careful. -kmp 20-Apr-2021

    assert is_valid_absolute_uri("ftps://somehost") is False  # Used to be True
    assert is_valid_absolute_uri("ftp://somehost") is False  # Used to be True

    # Finally we have a scheme and a host

    assert is_valid_absolute_uri("https://somehost") is True

    assert is_valid_absolute_uri("http://somehost") is True
    assert is_valid_absolute_uri("http://somehost.example.com") is True

    assert is_valid_absolute_uri("http://user@somehost") is True
    assert is_valid_absolute_uri("http://user:pass@somehost") is True

    assert is_valid_absolute_uri("http://somehost/") is True
    assert is_valid_absolute_uri("http://somehost.example.com/") is True

    assert is_valid_absolute_uri("http://somehost?alpha") is True
    assert is_valid_absolute_uri("http://somehost?alpha=1") is True
    assert is_valid_absolute_uri("http://somehost?alpha=1&beta=2") is True

    assert is_valid_absolute_uri("http://somehost/?alpha=1&beta=2") is True

    assert is_valid_absolute_uri("http://somehost/?alpha=1&beta=2&colon=:") is True

    # The given string still has to be syntactically valid, though.
    #
    # In the rfc3987 implementation we used to use, each of the next three examples used to raise ValueError
    # when parsing uri_reference to be validated. Now we trap that and quietly return False. -kmp 20-Apr-2021

    assert is_valid_absolute_uri("http://what:is:this/foo") is False  # Used to raise ValueError
    assert is_valid_absolute_uri("http://host:notanumber/foo") is False  # Used to raise ValueError
    assert is_valid_absolute_uri("http://what@is@this/foo") is False  # Used to raise ValueError

    # TODO: In the rfc3987 implementation, we allowed tags, but do we really want that? -kmp 20-Apr-2021

    # Used to be False, but only due to lack of ':' in URI, not due to tag
    assert is_valid_absolute_uri("foo#alpha") is False
    assert is_valid_absolute_uri("foo#alpha:beta") is False  # Used to wrongly be True due to ':' in URI

    assert is_valid_absolute_uri("http://abc.def/foo#alpha") is True  # TODO: Reconsider tags
    assert is_valid_absolute_uri("http://abc.def/foo#alpha:beta") is True  # TODO: Reconsider tags


def test_override_dict():

    d = {'foo': 'bar'}
    d_copy = d.copy()

    unique_prop1 = str(uuid.uuid4())
    unique_prop2 = str(uuid.uuid4())
    unique_prop3 = str(uuid.uuid4())

    assert unique_prop1 not in d
    assert unique_prop2 not in d
    assert unique_prop3 not in d

    with override_dict(d, **{unique_prop1: "something", unique_prop2: "anything"}):

        assert unique_prop1 in d  # added
        value1a = d.get(unique_prop1)
        assert value1a == "something"

        assert unique_prop2 in d  # added
        value2a = d.get(unique_prop2)
        assert value2a == "anything"

        assert unique_prop3 not in d

        with override_dict(d, **{unique_prop1: "something_else", unique_prop3: "stuff"}):

            assert unique_prop1 in d  # updated
            value1b = d.get(unique_prop1)
            assert value1b == "something_else"

            assert unique_prop2 in d  # unchanged
            assert d.get(unique_prop2) == value2a

            assert unique_prop3 in d  # added
            assert d.get(unique_prop3) == "stuff"

            with override_dict(d, **{unique_prop1: None}):

                assert unique_prop1 not in d  # removed

                with override_dict(d, **{unique_prop1: None}):

                    assert unique_prop1 not in d  # re-removed

                assert unique_prop1 not in d  # un-re-removed, but still removed

            assert unique_prop1 in d  # restored after double removal
            assert d.get(unique_prop1) == value1b

        assert unique_prop1 in d
        assert d.get(unique_prop1) == value1a

        assert unique_prop2 in d
        assert d.get(unique_prop2) == value2a

        assert unique_prop3 not in d

    assert unique_prop1 not in d
    assert unique_prop2 not in d
    assert unique_prop3 not in d

    assert d == d_copy


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


def test_dict_zip():

    assert dict_zip({}, {}) == []
    assert dict_zip({'a': 'one'}, {'a': 1}) == [('one', 1)]
    assert dict_zip({'a': 'one', 'b': 'two'}, {'a': 1, 'b': 2}) == [('one', 1), ('two', 2)]

    with pytest.raises(ValueError):
        dict_zip({'a': 'one'}, {'a': 1, 'extra': 2})

    with pytest.raises(ValueError):
        dict_zip({'a': 'one', 'extra': 2}, {'a': 1})


def test_json_leaf_subst():

    subs = {"x": "ex"}
    assert json_leaf_subst(3, subs) == 3
    assert json_leaf_subst("x", subs) == "ex"
    assert json_leaf_subst("y", subs) == "y"
    assert json_leaf_subst(["x", "y", "z"], subs) == ["ex", "y", "z"]
    assert json_leaf_subst(["x", "y", "z", "x", "y", "z"], subs) == ["ex", "y", "z", "ex", "y", "z"]

    subs = {3: "three"}
    assert json_leaf_subst(3, subs) == "three"
    assert json_leaf_subst(3.0, subs) == "three"  # weird but acceptable

    subs = {"x": "ex", "y": "why", "ex": "y", "why": "y"}
    assert json_leaf_subst("x", subs) == "ex"
    assert json_leaf_subst(["x", "y"], subs) == ["ex", "why"]
    assert json_leaf_subst({"x": "y", "y": "x"}, subs) == {"ex": "why", "why": "ex"}


def test_singleton_manager():

    class Foo:
        def __init__(self, token2=None):
            self.secret_token = uuid.uuid4()
            self.secret_token2 = token2

    foo1_manager = SingletonManager(Foo)

    assert foo1_manager.singleton_class is Foo
    assert isinstance(foo1_manager.singleton, Foo)
    assert foo1_manager.singleton is foo1_manager.singleton
    assert foo1_manager.singleton.secret_token == foo1_manager.singleton.secret_token
    assert foo1_manager.singleton.secret_token2 is None

    foo2_manager = SingletonManager(Foo, str(uuid.uuid4()))

    assert foo2_manager.singleton_class is Foo
    assert isinstance(foo2_manager.singleton, Foo)
    assert foo2_manager.singleton is foo2_manager.singleton
    assert foo2_manager.singleton.secret_token == foo2_manager.singleton.secret_token
    assert foo2_manager.singleton.secret_token2 == foo2_manager.singleton.secret_token2

    foo3_manager = SingletonManager(Foo, token2=str(uuid.uuid4()))

    assert foo3_manager.singleton_class is Foo
    assert isinstance(foo3_manager.singleton, Foo)
    assert foo3_manager.singleton is foo3_manager.singleton
    assert foo3_manager.singleton.secret_token == foo3_manager.singleton.secret_token
    assert foo3_manager.singleton.secret_token2 is not None
    assert foo3_manager.singleton.secret_token2 == foo3_manager.singleton.secret_token2


def test_classproperty():

    t = ControlledTime()

    class Clock:
        @classproperty
        def sample(cls):  # noQA - PyCharm wrongly thinks the argname should be 'self'
            return t.now()

    class SubClock(Clock):
        pass

    t0 = Clock.sample
    t1 = Clock.sample
    t2 = Clock.sample

    assert t2 > t1 > t0

    t3 = SubClock.sample
    t4 = SubClock.sample

    assert t4 > t3 > t2

    t5 = Clock.sample

    assert t5 > t4


def test_classproperty_cached():

    print()

    t = ControlledTime()

    class Clock:
        @classproperty_cached
        def sample(cls):  # noQA - PyCharm wrongly thinks the argname should be 'self'
            return t.now()

    class SubClock(Clock):
        pass

    c_t1 = Clock.sample
    c_t2 = Clock.sample
    c_t3 = Clock.sample

    assert c_t3 == c_t2 == c_t1  # Cached value

    s_t1 = SubClock.sample
    s_t2 = SubClock.sample

    assert s_t2 == s_t1 == c_t3 == c_t2 == c_t1  # Cached value and cache is shared with the parent class

    with pytest.raises(ValueError) as exc:
        classproperty_cached.reset(instance_class=SubClock, attribute_name='sample', subclasses=False)
    assert str(exc.value) == ("The subclasses= argument to classproperty_cached.reset must not be False"
                              " because classproperty_cached does not use per-subclass caches.")

    # This will clear SubClock cache, but that's shared with the Clock cache, so both will clear.
    assert classproperty_cached.reset(instance_class=SubClock, attribute_name='sample') is True

    c_t5 = Clock.sample     # This should recompute Clock.sample cache, which is shared by SubCLock
    assert c_t5 > c_t1

    s_t3 = SubClock.sample  # This will access the Clock.sample cache indirectly
    assert s_t3 > s_t1

    assert s_t3 == c_t5     # This shows they are the same cache

    # Again, this will clear Clock and SubClock caches
    classproperty_cached.reset(instance_class=Clock, attribute_name='sample')

    c_t6 = Clock.sample     # This should recompute Clock.sample cached value
    s_t4 = SubClock.sample  # This should recompute SubClock.sample cached value
    assert c_t6 > c_t5
    assert s_t4 > c_t3
    assert s_t4 == c_t6  # They share a cache

    # Again, both are cleared. This isn't needed but is useful to retain parallel with test
    # for classproperty_cached_each_subclass
    classproperty_cached.reset(instance_class=SubClock, attribute_name='sample')

    c_t7 = Clock.sample
    assert c_t7 > c_t6

    s_t5 = SubClock.sample
    assert s_t5 > s_t4

    assert s_t5 == c_t7  # They share a cache

    # Finally, this is just an error. Since this cache cleare won't happen, the remaining cache value accesses
    # will show no change.
    with pytest.raises(ValueError) as exc:
        classproperty_cached.reset(instance_class=Clock, attribute_name='sample', subclasses=False)
    assert str(exc.value) == ("The subclasses= argument to classproperty_cached.reset must not be False"
                              " because classproperty_cached does not use per-subclass caches.")

    c_t8 = Clock.sample
    assert c_t8 == c_t7

    s_t6 = SubClock.sample
    assert s_t6 == s_t5

    with pytest.raises(ValueError) as exc:
        classproperty_cached.reset(instance_class='not-a-class', attribute_name='sample')
    assert str(exc.value) == 'The instance_class= argument to classproperty_cached.reset must be a class.'

    with pytest.raises(ValueError) as exc:
        classproperty_cached.reset(instance_class=Clock, attribute_name='not_sample')
    assert str(exc.value) == "The slot Clock.not_sample is not defined."

    class NotClock(Clock):
        sample = 17  # noQA - This overrides a previous slot and we expect it will break things.

    with pytest.raises(ValueError) as exc:
        classproperty_cached.reset(instance_class=NotClock, attribute_name='sample')
    assert str(exc.value) == "The slot NotClock.sample does not contain a cached value."


def test_classproperty_cached_each_subclass():

    print()

    t = ControlledTime()

    class Clock:
        @classproperty_cached_each_subclass
        def sample(cls):  # noQA - PyCharm wrongly thinks the argname should be 'self'
            return t.now()

    class SubClock(Clock):
        pass

    c_t1 = Clock.sample
    c_t2 = Clock.sample
    c_t3 = Clock.sample

    assert c_t3 == c_t2 == c_t1

    s_t1 = SubClock.sample
    s_t2 = SubClock.sample

    assert s_t2 == s_t1
    assert s_t1 > c_t3

    c_t4 = Clock.sample

    assert c_t4 == c_t1

    # This will clear SubClock cache only
    classproperty_cached_each_subclass.reset(instance_class=SubClock, attribute_name='sample')

    c_t5 = Clock.sample
    assert c_t5 == c_t1

    s_t3 = SubClock.sample  # This should recompute SubClock.sample cached value
    assert s_t3 > s_t1

    # This will clear Clock and SubClock caches
    classproperty_cached_each_subclass.reset(instance_class=Clock, attribute_name='sample')

    c_t6 = Clock.sample     # This should recompute Clock.sample cached value
    s_t4 = SubClock.sample  # This should recompute SubClock.sample cached value
    assert c_t6 > c_t5
    assert s_t4 > c_t6
    assert c_t6 > s_t3
    assert s_t3 > c_t5

    # This will clear the SubClock cache only
    classproperty_cached_each_subclass.reset(instance_class=SubClock, attribute_name='sample')

    c_t7 = Clock.sample
    assert c_t7 == c_t6

    s_t5 = SubClock.sample
    assert s_t5 > s_t4

    # This will clear Clock cache ONLY, not disturbing SubClock
    classproperty_cached_each_subclass.reset(instance_class=Clock, attribute_name='sample', subclasses=False)

    c_t8 = Clock.sample
    assert c_t8 > c_t7

    s_t6 = SubClock.sample
    assert s_t6 == s_t5

    with pytest.raises(ValueError) as exc:
        classproperty_cached_each_subclass.reset(instance_class='not-a-class', attribute_name='sample')
    assert str(exc.value) == 'The instance_class= argument to classproperty_cached_each_subclass.reset must be a class.'

    with pytest.raises(ValueError) as exc:
        classproperty_cached_each_subclass.reset(instance_class=Clock, attribute_name='not_sample')
    assert str(exc.value) == "The slot Clock.not_sample is not defined."

    class NotClock(Clock):
        sample = 17  # noQA - This overrides a previous slot and we expect it will break things.

    with pytest.raises(ValueError) as exc:
        classproperty_cached_each_subclass.reset(instance_class=NotClock, attribute_name='sample')
    assert str(exc.value) == "The slot NotClock.sample does not contain a cached value."


def test_singleton():

    assert Singleton() != Singleton()
    assert Singleton.singleton != Singleton()
    assert Singleton.singleton == Singleton.singleton
    assert Singleton.singleton is Singleton.singleton

    assert type(Singleton.singleton) is Singleton

    assert isinstance(Singleton.singleton, Singleton)

    class Foo(Singleton):
        pass

    assert Foo() != Foo()
    assert Foo.singleton != Foo()
    assert Foo.singleton == Foo.singleton
    assert Foo.singleton is Foo.singleton

    assert Foo.singleton != Singleton.singleton

    assert type(Foo.singleton) is Foo

    assert isinstance(Foo.singleton, Singleton)
    assert isinstance(Foo.singleton, Foo)


def test_named_object():

    with mock.patch.object(misc_utils_module, 'id', MockId(counter_base=0x10001)):

        eof = NamedObject('eof')

        assert str(eof) == '<eof>'
        assert repr(eof) == '<eof@10001>'

        missing = NamedObject('missing')

        assert str(missing) == '<missing>'
        assert repr(missing) == '<missing@10002>'


def test_key_value_dict():

    assert key_value_dict('a', 'b') == {'Key': 'a', 'Value': 'b'}


def test_merge_key_value_dict_lists():

    old = [{'Key': 'a', 'Value': '1'}, {'Key': 'b', 'Value': '2'}]
    new = [{'Key': 'c', 'Value': '3'}, {'Key': 'd', 'Value': '4'}]
    actual = merge_key_value_dict_lists(old, new)
    expected = [
        {'Key': 'a', 'Value': '1'},
        {'Key': 'b', 'Value': '2'},
        {'Key': 'c', 'Value': '3'},
        {'Key': 'd', 'Value': '4'}
    ]
    assert actual == expected

    old = [{'Key': 'a', 'Value': '1'}, {'Key': 'b', 'Value': '2'}]
    new = [{'Key': 'b', 'Value': '3'}, {'Key': 'd', 'Value': '4'}]
    actual = merge_key_value_dict_lists(old, new)
    expected = [{'Key': 'a', 'Value': '1'}, {'Key': 'b', 'Value': '3'}, {'Key': 'd', 'Value': '4'}]
    assert actual == expected

    old = [{'Key': 'a', 'Value': '1'}, {'Key': 'b', 'Value': '2'}]
    new = [{'Key': 'b', 'Value': '3'}, {'Key': 'd', 'Value': '4'}, {'Key': 'a', 'Value': '7'}]
    actual = merge_key_value_dict_lists(old, new)
    expected = [{'Key': 'a', 'Value': '7'}, {'Key': 'b', 'Value': '3'}, {'Key': 'd', 'Value': '4'}]
    assert actual == expected


def test_lines_printed_to():
    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove():
        with lines_printed_to("foo.text") as out:

            out("This is line 1.")
            out("This is line 2.")

        assert file_contents("foo.text") == "This is line 1.\nThis is line 2.\n"


def test_obsolete():

    def foo(x):
        return x + 17

    assert foo(10) == 27

    foo = obsolete(foo)
    with pytest.raises(ObsoleteError):
        foo(10)

    def bar(x):
        return x + 17
    bar_name = bar.__name__

    assert bar(10) == 27

    mock_logging = MockLog()
    with mock.patch.object(misc_utils_module, "logging", mock_logging):
        bar = obsolete(bar, fail=False)

        assert bar(10) == 27

    assert mock_logging.all_log_messages == [
        f"ERROR: Called obsolete function {bar_name}"
    ]


def test_keys_and_values_to_dict():
    kv = [{"Key": "abc", "Value": "def"}, {"Key": "ghi", "Value": "jkl"}]
    d = keys_and_values_to_dict(kv)
    assert d == {"abc": "def", "ghi": "jkl"}

    kv = [{"name": "abc", "value": "def"}, {"name": "ghi", "value": "jkl"}]
    d = keys_and_values_to_dict(kv, key_name="name", value_name="value")
    assert d == {"abc": "def", "ghi": "jkl"}

    assert keys_and_values_to_dict([]) == {}

    with pytest.raises(ValueError):
        # Missing key (Foo).
        kv = [{"Key": "abc", "Value": "def"}, {"Foo": "ghi", "Value": "jkl"}]
        keys_and_values_to_dict(kv)

    with pytest.raises(ValueError):
        # Missing value (Foo).
        kv = [{"Key": "abc", "Value": "def"}, {"Key": "ghi", "Foo": "jkl"}]
        keys_and_values_to_dict(kv)

    with pytest.raises(ValueError):
        # Duplicate key (abc).
        kv = [{"Key": "abc", "Value": "def"}, {"Key": "abc", "Value": "jkl"}]
        keys_and_values_to_dict(kv)


def test_dict_to_keys_and_values():
    d = {"abc": "def", "ghi": "jkl"}
    kv = dict_to_keys_and_values(d)
    assert kv == [{"Key": "abc", "Value": "def"}, {"Key": "ghi", "Value": "jkl"}]

    d = {"abc": "def", "ghi": "jkl"}
    kv = dict_to_keys_and_values(d, key_name="name", value_name="value")
    assert kv == [{"name": "abc", "value": "def"}, {"name": "ghi", "value": "jkl"}]

    assert dict_to_keys_and_values({}) == []


def test_is_c4_arn():
    assert is_c4_arn('arn:aws:ecs:us-east-1:643:cluster/c4-ecs-cgap-production-stack-Productionblue-fzOm82DGZKGZ')
    assert is_c4_arn('arn:aws:ecs:us-east-1:643:cluster/c4-ecs-cgap-production-stack-CgapProductionblue-fzOm82DGZKGZ')
    assert is_c4_arn('arn:aws:ecs:us-east-1:643:cluster/c4-ecs-fourfront-hotseat-stack-Hotseat-ZnIAAAz8Apxf')
    assert is_c4_arn('arn:aws:ecs:us-east-1:643:cluster/c4-ecs-fourfront-production-stack-FourfrontProd-hU0JiwOeWfdm')
    assert is_c4_arn('arn:aws:ecs:us-east-1:643:cluster/c4-ecs-abc-webdev-stack-CgapWebdev-rSLwZBbdVTtx')
    assert is_c4_arn('arn:aws:ecs:us-east-1:643:cluster:c4-ecs-def-mastertest-stack-FourfrontMaster-O25WVcRMpExr')
    assert not is_c4_arn('arn:aws:ecs:us-east-1:643:cluster-c4-ecs-def-mastertest-stack-FourfrontMaster-O25WVcRMpExr')
    assert not is_c4_arn('arn:aws:ecs:us-east-1:643:cluster/default')
    assert not is_c4_arn('arn:aws:ecs:us-east-1:643:cluster/xyzzy-c4-default')
    assert not is_c4_arn('arn:aws:ecs:us-east-1:643:cluster/xyzzyasdfc4-default')
    assert not is_c4_arn('arn:aws:ecs:us-east-1:643:cluster/xyzzy:-c4-default')


class TestTopologicalSorter:

    EMPTY_GRAPH = {}
    ACYCLIC_GRAPH = {"A": {"B"}, "B": {"C"}}
    CYCLIC_GRAPH = {"A": {"B"}, "B": {"C", "A"}}

    @pytest.mark.parametrize(
        "graph,exception,expected",
        [
            (EMPTY_GRAPH, False, []),
            (ACYCLIC_GRAPH, False, ["C", "B", "A"]),
            (CYCLIC_GRAPH, True, None),
        ]
    )
    def test_topological_sort(self, graph, exception, expected):
        sorter = TopologicalSorter(graph=graph)
        if exception:
            with pytest.raises(CycleError):
                sorted_nodes = sorter.static_order()
                # The expected error occurs during this coercion:
                list(sorted_nodes)
        else:
            sorted_nodes = sorter.static_order()
            result = list(sorted_nodes)
            assert result == expected


def test_utc_now_str():

    dt = ControlledTime()
    with mock.patch("dcicutils.misc_utils.datetime", dt):
        # One cannot patch datetime.datetime directly because it's a primitive type,
        # but patching a ControlledTime once it's installed in the same place works OK.
        with mock.patch.object(dt.datetime, "utcnow") as mock_utcnow:
            mock_utcnow.return_value = datetime_module.datetime(2013, 1, 1, 12, 34, 56)
            assert utc_now_str() == "2013-01-01T12:34:56+00:00"


def test_deduplicate_list():

    x = ['a', 'b', 'c', 'a', 'b', 'c']
    xlen = len(x)

    assert sorted(deduplicate_list(x)) == ['a', 'b', 'c']
    assert len(x) == xlen  # make sure there was no side effect to the original list

    y = ['a']
    y0 = deduplicate_list(y)
    assert y0 == y
    assert y0 is not y

    assert deduplicate_list([]) == []

    # As we've implemented it, tuples can be deduplicated, too, but the result will be a list.
    assert sorted(deduplicate_list(tuple(x))) == ['a', 'b', 'c']

    with pytest.raises(Exception):
        # Lists with elements that are mutable objects that cannot be hashed as sets can't be dedupliated.
        # For example, a list of lists.  This is because set([['a'], ['b'], ['c']]) will fail.
        deduplicate_list([['a'], ['b'], ['c']])


def test_chunked():

    foo = []
    for x in chunked([1, 2, 3, 4, 5, 6], chunk_size=2):
        foo.append(x)
    assert foo == [[1, 2], [3, 4], [5, 6]]

    foo = []
    for x in chunked([1, 2, 3, 4, 5, 6, 7], chunk_size=2):
        foo.append(x)
    assert foo == [[1, 2], [3, 4], [5, 6], [7]]

    foo = []
    for x in chunked([], chunk_size=2):
        foo.append(x)
    assert foo == []

    foo = []
    for x in chunked([1, 2, 3, 4, 5, 6], chunk_size=3):
        for y in chunked(x, chunk_size=2):
            foo.append(y)
    assert foo == [[1, 2], [3], [4, 5], [6]]

    foo = []
    for x in chunked([1, 2, 3, 4, 5, 6, 7], chunk_size=3):
        for y in chunked(x, chunk_size=2):
            foo.append(y)
    assert foo == [[1, 2], [3], [4, 5], [6], [7]]

    foo = []
    for x in chunked([1, 2, 3, 4, 5, 6], chunk_size=2):
        for y in chunked(x, chunk_size=3):
            foo.append(y)
    assert foo == [[1, 2], [3, 4], [5, 6]]

    foo = []
    for x in chunked([1, 2, 3, 4, 5, 6, 7], chunk_size=2):
        for y in chunked(x, chunk_size=3):
            foo.append(y)
    assert foo == [[1, 2], [3, 4], [5, 6], [7]]


def test_parse_in_radix():

    # Just some random examples...
    assert parse_in_radix("0", radix=17) == 0
    assert parse_in_radix("1101", radix=2) == 13
    assert parse_in_radix("123", radix=10) == 123
    assert parse_in_radix("325", radix=8) == 3 * (8 * 8) + 2 * 8 + 5
    assert parse_in_radix("dead", radix=16) == 13 * (16 ** 3) + 14 * (16 ** 2) + 10 * (16 ** 1) + 13 * (16 ** 0)
    assert parse_in_radix("DEAD", radix=16) == 13 * (16 ** 3) + 14 * (16 ** 2) + 10 * (16 ** 1) + 13 * (16 ** 0)
    assert parse_in_radix("10Z", radix=36) == 36 * 36 + 35
    assert parse_in_radix("10z", radix=36) == 36 * 36 + 35

    def char_range(lo, hi):
        return ''.join(chr(c) for c in range(ord(lo), ord(hi) + 1))

    assert char_range('b', 'e') == "bcde"  # just testing

    all_digits = char_range('0', '9') + char_range('A', 'Z')
    assert all_digits == _36_DIGITS

    for r in range(2, 37):

        assert parse_in_radix("0", radix=r) == 0

        for i in range(1, r):
            # All single-digits should parse to their string position.
            assert parse_in_radix(_36_DIGITS[i], radix=r) == i
            # Adding leading 0's shouldn't matter.
            assert parse_in_radix("000" + _36_DIGITS[i], radix=r) == i

        assert parse_in_radix("10", radix=r) == r

        for i in range(2, 6):
            # print(r"===== i={i}, radix={r} =====")
            ri = r ** i
            # 100=r^2, 1000=r^3, ...
            n_str = "1" + ("0" * i)
            # print(f"parsing n={n_str!r} in radix {r}, expecting {r}^{i}={ri}")
            assert parse_in_radix(n_str, radix=r) == ri
            # 11[2] + 1 = 100[2] = 2^2, 22[3] + 1 == 100[3] = 3^2,
            # 111[2] + 1 = 1000[2] = 2^3, 222[3] + 1 = 1000[3] = 3^3
            n_str = _36_DIGITS[r - 1] * i
            # print(f"parsing n={n_str!r} in radix {r}, expecting {r}^{i}-1={ri - 1}")
            assert parse_in_radix(n_str, radix=r) == ri - 1

    with pytest.raises(ValueError):
        parse_in_radix(" ", radix=10)  # bad character

    with pytest.raises(ValueError):
        parse_in_radix("", radix=10)  # empty string

    with pytest.raises(ValueError):
        parse_in_radix("0", radix=1)  # radix too low

    with pytest.raises(ValueError):
        parse_in_radix("0", radix=40)  # radix too high


def test_format_in_radix():

    # Just some random examples...
    assert format_in_radix(0, radix=10) == "0"
    assert format_in_radix(13, radix=2) == "1101"
    assert format_in_radix(123, radix=10) == "123"
    assert format_in_radix(3 * (8 * 8) + 2 * 8 + 5, radix=8) == "325"
    assert format_in_radix(13 * (16 ** 3) + 14 * (16 ** 2) + 10 * (16 ** 1) + 13 * (16 ** 0), radix=16) == "DEAD"
    assert format_in_radix(36 * 36 + 35, radix=36) == "10Z"

    for r in range(2, 37):
        for i in range(0, 6):
            for n in [i, r ** i, r ** i - 1]:
                assert parse_in_radix(format_in_radix(n, radix=r), radix=r) == n

    with pytest.raises(ValueError):
        assert format_in_radix(-27, radix=10)  # won't format negative number

    with pytest.raises(ValueError):
        format_in_radix(27, radix=1)  # radix too low

    with pytest.raises(ValueError):
        format_in_radix(27, radix=40)  # radix too high


def test_managed_property():

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


def test_min_datetime():

    assert MIN_DATETIME == datetime_module.datetime(1, 1, 1, 0, 0, 0)

    assert MIN_DATETIME.replace(tzinfo=pytz.UTC) == MIN_DATETIME_UTC


def test_future_datetime():

    dt = ControlledTime(tick_seconds=0.1)

    now = dt.just_now()

    with mock.patch.object(misc_utils_module, "datetime", dt):

        an_hour_from_now = future_datetime(minutes=60)  # computing now() inside this will consume one tick = .1 second

        assert an_hour_from_now == now + datetime_module.timedelta(hours=1, seconds=0.1)

        two_hours_from_now = future_datetime(hours=2, now=now)  # this won't check the time, so no seconds consumed

        assert two_hours_from_now == now + datetime_module.timedelta(hours=2)


def test_map_chunked():

    res = map_chunked(lambda x: ''.join(x), "abcdefghij", chunk_size=4)
    assert next(res) == 'abcd'
    assert next(res) == 'efgh'
    assert next(res) == 'ij'
    with pytest.raises(StopIteration):
        next(res)

    res = map_chunked(lambda x: ''.join(x), "abcdefghij", chunk_size=4, reduce=list)
    assert res == ['abcd', 'efgh', 'ij']

    res = map_chunked(lambda x: ''.join(x), "abcdefghij", chunk_size=4, reduce=lambda x: '.'.join(x))
    assert res == 'abcd.efgh.ij'


def test_pad_to():

    assert pad_to(5, []) == [None, None, None, None, None]
    assert pad_to(5, [], padding='foo') == ['foo', 'foo', 'foo', 'foo', 'foo']

    assert pad_to(5, ['x']) == ['x', None, None, None, None]
    assert pad_to(5, ['x'], padding='foo') == ['x', 'foo', 'foo', 'foo', 'foo']

    six_elements = ['a', 'b', 'c', 'd', 'e', 'f']

    assert pad_to(5, six_elements) == six_elements
    assert pad_to(5, six_elements, padding='foo')


def test_json_lines_reader_dicts():

    print()  # start on a fresh line

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove():

        item1 = {"foo": 1, "bar": 2}
        item2 = {"foo": 3, "bar": 4}

        item1_str = json.dumps(item1)
        item2_str = json.dumps(item2)

        sample_lines = [item1_str, item2_str]

        sample_filename = "somefile.jsonl"

        with io.open(sample_filename, 'w') as fp:
            for line in sample_lines:
                print(line, file=fp)

        for file, content in mfs.files.items():
            print("=" * 20, file, "=" * 20)
            print(content.decode('utf-8'))
            print("=" * 80)

        with io.open(sample_filename) as fp:
            assert [line for line in JsonLinesReader(fp)] == [item1, item2]


def test_json_lines_reader_lists():

    print()  # start on a fresh line

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove():

        item1 = {"foo": 1, "bar": 2}
        item2 = {"foo": 3, "bar": 4}

        headers: List[str] = list(item1.keys())

        item1_str = json.dumps([item1[header] for header in headers])
        item2_str = json.dumps([item2[header] for header in headers])

        sample_lines = [item1_str, item2_str]

        sample_filename = "somefile.jsonl"

        with io.open(sample_filename, 'w') as fp:

            print(json.dumps(headers), file=fp)
            for line in sample_lines:
                print(line, file=fp)

        for file, content in mfs.files.items():
            print("=" * 20, file, "=" * 20)
            print(content.decode('utf-8'))
            print("=" * 80)

        with io.open(sample_filename) as fp:
            parsed = [line for line in JsonLinesReader(fp)]
            expected = [item1, item2]
            assert parsed == expected


def test_split_array_string():
    def split_array_string(value: str, unique: bool = False) -> List[str]:
        return split_string(value, "|", "\\", unique=unique)
    assert split_array_string(r"abc|def|ghi") == ["abc", "def", "ghi"]
    assert split_array_string(r"abc\|def|ghi") == ["abc|def", "ghi"]
    assert split_array_string(r"abc\\|def|ghi") == ["abc\\", "def", "ghi"]
    assert split_array_string(r"abc\\\|def|ghi") == ["abc\\|def", "ghi"]
    assert split_array_string(r"abc\\\|def\|ghi") == ["abc\\|def|ghi"]
    assert split_array_string(r"abc\\|\\def\|ghi") == ["abc\\", "\\def|ghi"]
    assert split_array_string(r"abc\\|\\def\|ghi||") == ["abc\\", "\\def|ghi"]
    assert split_array_string(r"|abcdefghi|") == ["abcdefghi"]
    assert split_array_string(r"|abcdefghi||xyzzy") == ["abcdefghi", "xyzzy"]
    assert split_array_string(r"") == []
    assert split_array_string(None) == []
    assert split_array_string(r"|") == []
    assert split_array_string(r"\|") == ["|"]
    assert split_array_string(r"\\|") == ["\\"]
    assert split_array_string(r"abc|def|abc|ghi", unique=False) == ["abc", "def", "abc", "ghi"]
    assert split_array_string(r"abc|def|abc|ghi", unique=True) == ["abc", "def", "ghi"]
    assert split_array_string(r"abc\\\|def\|ghi|jkl|mno|jkl", unique=False) == ["abc\\|def|ghi", "jkl", "mno", "jkl"]
    assert split_array_string(r"abc\\\|def\|ghi|jkl|mno|jkl", unique=True) == ["abc\\|def|ghi", "jkl", "mno"]
    assert split_string(r"abc|def|ghi|def", delimiter="|", unique=False) == ["abc", "def", "ghi", "def"]
    assert split_string(r"abc|def|ghi|def", delimiter="|", unique=True) == ["abc", "def", "ghi"]


def test_merge_objects_1():
    target = {"abc": {"def": {"ghi": None}}, "xyzzy": [None, None, None]}
    source = {"xyzzy": [{"foo": None}, {"goo": None}]}
    expected = {"abc": {"def": {"ghi": None}}, "xyzzy": [{"foo": None}, {"goo": None}, None]}
    merge_objects(target, source, False)
    assert target == expected


def test_merge_objects_2():
    target = {"abc": {"def": {"ghi": None}}, "xyzzy": [None, None, None]}
    source = {"xyzzy": [{"foo": None}, {"goo": None}]}
    expected = {"abc": {"def": {"ghi": None}}, "xyzzy": [{"foo": None}, {"goo": None}, {"goo": None}]}
    merge_objects(target, source, True)
    assert target == expected


def test_merge_objects_3():
    target = {"abc": {"def": {"ghi": None}}, "xyzzy": [None, None]}
    source = {"xyzzy": [{"foo": None}, {"goo": None}]}
    expected = {"abc": {"def": {"ghi": None}}, "xyzzy": [{"foo": None}, {"goo": None}]}
    merge_objects(target, source, False)
    assert target == expected


def test_merge_objects_4():
    target = {"abc": {"def": {"ghi": None}}, "xyzzy": [None, None]}
    source = {"xyzzy": [{"foo": None}, {"goo": None}]}
    expected = {"abc": {"def": {"ghi": None}}, "xyzzy": [{"foo": None}, {"goo": None}]}
    merge_objects(target, source, True)
    assert target == expected


def test_merge_objects_5():
    target = {"abc": {"def": {"ghi": None}}, "xyzzy": ["mno"]}
    source = {"xyzzy": [{"foo": None}, {"goo": None}]}
    expected = {"abc": {"def": {"ghi": None}}, "xyzzy": [{"foo": None}, {"goo": None}]}
    merge_objects(target, source, False)
    assert target == expected


def test_merge_objects_6():
    target = {"abc": {"def": {"ghi": None}}, "xyzzy": ["mno"]}
    source = {"xyzzy": [{"foo": None}, {"goo": None}]}
    expected = {"abc": {"def": {"ghi": None}}, "xyzzy": [{"foo": None}, {"goo": None}]}
    merge_objects(target, source, True)
    assert target == expected


def test_merge_objects_7():
    target = {"abc": {"def": {"ghi": None}}, "xyzzy": [None, None, "abc", "def", 123]}
    source = {"xyzzy": [{"foo": None}, {"goo": None}, {"hoo": None}]}
    expected = {"abc": {"def": {"ghi": None}}, "xyzzy": [{"foo": None}, {"goo": None}, {"hoo": None}, "def", 123]}
    merge_objects(target, source, False)
    assert target == expected


def test_merge_objects_8():
    target = {"abc": {"def": {"ghi": None}}, "xyzzy": [None, None, "abc", "def", 123]}
    source = {"xyzzy": [{"foo": None}, {"goo": None}, {"hoo": None}]}
    expected = {"abc": {"def": {"ghi": None}},
                "xyzzy": [{"foo": None}, {"goo": None}, {"hoo": None}, {"hoo": None}, {"hoo": None}]}
    merge_objects(target, source, True)
    assert target == expected


def test_merge_objects_9():
    target = {"abc": [1, 2, 3]}
    source = {"abc": [4, 5]}
    expected = {"abc": [4, 5]}
    result = merge_objects(target, source, primitive_lists=True, copy=True)
    assert result == expected
    assert id(target) != id(result)

    target = {"abc": [1, 2, 3]}
    source = {"abc": [4, 5]}
    expected = {"abc": [4, 5, 3]}
    result = merge_objects(target, source, primitive_lists=False, copy=False)
    assert result == expected
    assert id(target) == id(result)


def test_to_integer():

    assert to_integer("0") == 0
    assert to_integer("00") == 0
    assert to_integer("1") == 1
    assert to_integer("01") == 1
    assert to_integer("123") == 123
    assert to_integer("123.0") == 123
    assert to_integer("123.00000") == 123
    assert to_integer(" 1234567890 ") == 1234567890

    assert to_integer("+0") == 0
    assert to_integer("+1") == 1
    assert to_integer("+123") == 123
    assert to_integer("+123.0") == 123
    assert to_integer("+123.00000") == 123
    assert to_integer("+ 1234567890 ") == 1234567890

    assert to_integer("-0") == 0
    assert to_integer("-1") == -1
    assert to_integer("-123") == -123
    assert to_integer("-123.0") == -123
    assert to_integer("-123.00000") == -123
    assert to_integer("- 1234567890 ") == -1234567890

    assert to_integer("1K", allow_multiplier_suffix=True) == 1000
    assert to_integer("2KB", allow_multiplier_suffix=True) == 2000
    assert to_integer("3Kb", allow_multiplier_suffix=True) == 3000
    assert to_integer("4kb", allow_multiplier_suffix=True) == 4000
    assert to_integer("5 kb", allow_multiplier_suffix=True) == 5000
    assert to_integer("6.00kB", allow_multiplier_suffix=True) == 6000
    assert to_integer("9KB", allow_multiplier_suffix=True) == 9000
    assert to_integer("12KB", allow_multiplier_suffix=True) == 12000
    assert to_integer("15K", allow_multiplier_suffix=True) == 15000
    assert to_integer("1.5K", allow_multiplier_suffix=True) == 1500
    assert to_integer("15.K", allow_multiplier_suffix=True) == 15000
    assert to_integer("15.0K", allow_multiplier_suffix=True) == 15000
    assert to_integer("15.01K", allow_multiplier_suffix=True) == 15010
    assert to_integer(" 15.01 K ", allow_multiplier_suffix=True) == 15010
    assert to_integer("15.012  KB", allow_multiplier_suffix=True) == 15012
    assert to_integer("25k", allow_multiplier_suffix=True) == 25000
    assert to_integer("35Kb", allow_multiplier_suffix=True) == 35000
    assert to_integer("-35Kb", allow_multiplier_suffix=True) == -35000
    assert to_integer("0K", allow_multiplier_suffix=True) == 0
    assert to_integer("1M", allow_multiplier_suffix=True) == 1000000
    assert to_integer("2G", allow_multiplier_suffix=True) == 2000000000
    assert to_integer("2.123456G", allow_multiplier_suffix=True) == 2123456000
    assert to_integer("345T", allow_multiplier_suffix=True) == 345000000000000
    assert to_integer("4.000000001230TB", allow_multiplier_suffix=True) == 4000000001230
    assert to_integer("0M", allow_multiplier_suffix=True) == 0

    assert type(to_integer("4.000000001230TB", allow_multiplier_suffix=True)) == int

    assert to_integer("1,234", allow_commas=True) == 1234
    assert to_integer("12,345", allow_commas=True) == 12345
    assert to_integer("123,456", allow_commas=True) == 123456
    assert to_integer("1,234,567", allow_commas=True) == 1234567

    assert to_integer("1,234K", allow_multiplier_suffix=True, allow_commas=True) == 1234000
    assert to_integer("1,234M", allow_multiplier_suffix=True, allow_commas=True) == 1234000000
    assert to_integer("1,234G", allow_multiplier_suffix=True, allow_commas=True) == 1234000000000
    assert to_integer("-1,234G", allow_multiplier_suffix=True, allow_commas=True) == -1234000000000
    assert to_integer("1,234,567T", allow_multiplier_suffix=True, allow_commas=True) == 1234567000000000000
    assert to_integer("-1,234,567T", allow_multiplier_suffix=True, allow_commas=True) == -1234567000000000000

    assert to_integer(4321) == 4321
    assert to_integer(4321 + 9) == 4330


def test_to_integer_errors():

    assert to_integer("") is None
    assert to_integer("  ") is None
    assert to_integer(None) is None
    assert to_integer([]) is None
    assert to_integer({}) is None
    assert to_integer(datetime_module.datetime.now()) is None

    assert to_integer("abc") is None
    assert to_integer("123 456") is None
    assert to_integer("1.2") is None
    assert to_integer("123K") is None
    assert to_integer("123 K") is None
    assert to_integer("123K B") is None
    assert to_integer("++123") is None
    assert to_integer("--123") is None
    assert to_integer("+-123") is None
    assert to_integer("123,456") is None

    assert to_integer("K2", allow_multiplier_suffix=True) is None
    assert to_integer("2KK", allow_multiplier_suffix=True) is None
    assert to_integer(".", allow_multiplier_suffix=True) is None
    assert to_integer(".K", allow_multiplier_suffix=True) is None
    assert to_integer("1.5001K", allow_multiplier_suffix=True) is None
    assert to_integer(".5001K", allow_multiplier_suffix=True) is None
    assert to_integer("123P", allow_multiplier_suffix=True) is None
    assert to_integer("1,234M", allow_multiplier_suffix=True) is None
    assert to_integer("4.0000000012309TB", allow_multiplier_suffix=True) is None  # 4000000001230.9

    assert to_integer("12,345") is None
    assert to_integer(",", allow_commas=True) is None
    assert to_integer("1,", allow_commas=True) is None
    assert to_integer("123,", allow_commas=True) is None
    assert to_integer(",123", allow_commas=True) is None
    assert to_integer("123,45", allow_commas=True) is None
    assert to_integer("12,4356", allow_commas=True) is None
    assert to_integer("12,,456", allow_commas=True) is None
    assert to_integer("1,234,5678", allow_commas=True) is None


def test_to_float():

    assert to_float("789") == 789.0
    assert to_float("1234.0567") == 1234.0567
    assert to_float("1234.0") == 1234.0
    assert to_float(".1234") == 0.1234
    assert to_float("1234.") == 1234.0
    assert to_float("1.5K", allow_multiplier_suffix=True) == 1500
    assert to_float("1.5678K", allow_multiplier_suffix=True) == 1567.8
    assert to_float("1.56789K", allow_multiplier_suffix=True) == 1567.89
    assert to_float("1.567898K", allow_multiplier_suffix=True) == 1567.898
    assert to_float("1.0K", allow_multiplier_suffix=True) == 1000
    assert to_float("1.K", allow_multiplier_suffix=True) == 1000
    assert to_float(".2K", allow_multiplier_suffix=True) == 200
    assert to_float(".2M", allow_multiplier_suffix=True) == 200000
    assert to_float("3.M", allow_multiplier_suffix=True) == 3000000
    assert to_float("0.9G", allow_multiplier_suffix=True) == 900000000
    assert to_float("1.9G", allow_multiplier_suffix=True) == 1900000000
    assert to_float("4.0000000012309TB", allow_multiplier_suffix=True) == 4000000001230.9
    assert to_float(4321.1234) == 4321.1234

    assert to_float("1,234.K", allow_commas=True, allow_multiplier_suffix=True) == 1234000
    assert to_float("12,345.K", allow_commas=True, allow_multiplier_suffix=True) == 12345000
    assert to_float("12,345.0K", allow_commas=True, allow_multiplier_suffix=True) == 12345000
    assert to_float("12,345.06K", allow_commas=True, allow_multiplier_suffix=True) == 12345060
    assert to_float("12,345.06789K", allow_commas=True, allow_multiplier_suffix=True) == 12345067.89
    assert to_float("123,456.9876G", allow_commas=True, allow_multiplier_suffix=True) == 123456987600000
    assert to_float("123,456.9876G", allow_commas=True, allow_multiplier_suffix=True) == 123456987600000.0
    assert to_float("123,456.123498765G", allow_commas=True, allow_multiplier_suffix=True) == 123456123498765.0
    assert to_float("123,456.1234987656G", allow_commas=True, allow_multiplier_suffix=True) == 123456123498765.6
    assert to_float("123,456.1234987656Tb", allow_commas=True, allow_multiplier_suffix=True) == 123456123498765600
    assert to_float("123,456.g", allow_commas=True, allow_multiplier_suffix=True) == 123456 * 1000 * 1000 * 1000
    assert to_float("0.0m", allow_commas=True, allow_multiplier_suffix=True) == 0

    assert type(to_float("789")) == float
    assert type(to_float("1.5K", allow_multiplier_suffix=True)) == float
    assert type(to_float("123,456.9876G", allow_commas=True, allow_multiplier_suffix=True)) == float


def test_to_float_errors():

    assert to_float("") is None
    assert to_float("  ") is None
    assert to_float(None) is None
    assert to_float([]) is None
    assert to_float({}) is None
    assert to_float(datetime_module.datetime.now()) is None
    assert to_float({}, fallback="123") == "123"

    assert to_float("abc") is None
    assert to_float("123 456") is None
    assert to_float("123.456K") is None
    assert to_float("123,456") is None
    assert to_float(".") is None
    assert to_float(".M", allow_multiplier_suffix=True) is None
    assert to_float("M", allow_multiplier_suffix=True) is None


def test_load_json_from_file_expanding_environment_variables():
    with mock.patch.object(os, "environ", {"Auth0Secret": "dgakjhdgretqobv", "SomeEnvVar": "xyzzy"}):
        some_json = {"Auth0Secret": "${Auth0Secret}", "abc": "def", "someproperty": "$SomeEnvVar"}
        with temporary_file(content=json.dumps(some_json), suffix=".json") as tmpfile:
            expanded_json = load_json_from_file_expanding_environment_variables(tmpfile)
            assert expanded_json == {"Auth0Secret": "dgakjhdgretqobv", "abc": "def", "someproperty": "xyzzy"}


def test_create_readonly_object():
    a = create_readonly_object(foo="bar")
    assert a.foo == "bar"
    a = create_readonly_object(abcdef=123, ghi=456, jk="xyzzy", lmnop={"greeting": "Hello, world!"})
    assert a.abcdef == 123
    assert a.ghi == 456
    assert a.jk == "xyzzy"
    assert a.lmnop == {"greeting": "Hello, world!"}
