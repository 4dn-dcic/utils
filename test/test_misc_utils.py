import io
import json
import os
import pytest
import warnings
import webtest
from dcicutils.misc_utils import (
    PRINT, ignored, filtered_warnings, get_setting_from_context, VirtualApp,
    _VirtualAppHelper,  # noqa - yes, this is a protected member, but we still want to test it
    Retry,
)
from dcicutils.qa_utils import Occasionally
from unittest import mock


def test_uppercase_print():
    # This is just a synonym, so the easiest thing is just to test that fact.
    assert PRINT == print

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

    def patch_json(self, url, obj, **kwargs):
        call_info = {'op': 'patch_json', 'url': url, 'obj': obj, 'kwargs': kwargs}
        self.calls.append(call_info)
        return FakeResponse({'processed': call_info})


class FakeApp:
    pass


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


def test_retry():

    def adder(n):
        def addn(x):
            return x + n
        return addn

    sometimes_add2 = Occasionally(adder(2), success_frequency=2)

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

    rarely_add3 = Occasionally(adder(3), success_frequency=5)

    with pytest.raises(Exception):
        assert rarely_add3(1) == 4
    with pytest.raises(Exception):
        assert rarely_add3(1) == 4
    with pytest.raises(Exception):
        assert rarely_add3(1) == 4
    with pytest.raises(Exception):
        assert rarely_add3(1) == 4
    assert rarely_add3(1) == 4  # 5th time's a charm

    rarely_add3.reset()

    ARGS = 1  # We have to access a random place out of a tuple structure for mock data on time.sleep's arg

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

    rarely_add3.reset()

    with pytest.raises(SyntaxError):

        @Retry.retry_allowed(retries_allowed=4, wait_seconds=2, wait_increment=3, wait_multiplier=1.25)
        def reliably_add_three(x):
            return rarely_add3(x)
