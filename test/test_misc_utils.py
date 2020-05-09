import io
import json
import os
import webtest
from dcicutils.misc_utils import (
    PRINT, ignored, get_setting_from_context, VirtualApp,
    _VirtualAppHelper,  # noqa - yes, this is a protected member, but we still want to test it
)
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
