import contextlib
import functools
import json
import os

from dcicutils.env_utils import EnvUtils
from dcicutils.misc_utils import file_contents, decorator
from .conftest_settings import TEST_DIR


@contextlib.contextmanager
def fresh_legacy_state():
    with EnvUtils.fresh_state_from(data={'is_legacy': True}):
        yield


@contextlib.contextmanager
def fresh_cgap_state():
    cgap_declaration = json.loads(file_contents(os.path.join(TEST_DIR,
                                                             'data_files/foursight-cgap-envs/main.ecosystem')))
    with EnvUtils.fresh_state_from(data=cgap_declaration):
        yield


@contextlib.contextmanager
def fresh_ff_state():
    with EnvUtils.fresh_ff_state():
        yield


@decorator()
def using_fresh_legacy_state():
    def wrap(function):
        @functools.wraps(function)
        def _wrapped(*args, **kwargs):
            with fresh_legacy_state():
                return function(*args, **kwargs)
        return _wrapped
    return wrap


@decorator()
def using_fresh_cgap_state():
    def wrap(function):
        @functools.wraps(function)
        def _wrapped(*args, **kwargs):
            with fresh_cgap_state():
                return function(*args, **kwargs)
        return _wrapped
    return wrap


@decorator()
def using_fresh_ff_state():
    def wrap(function):
        @functools.wraps(function)
        def _wrapped(*args, **kwargs):
            with fresh_ff_state():
                return function(*args, **kwargs)
        return _wrapped
    return wrap
