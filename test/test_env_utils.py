# import pytest
import os

from dcicutils.env_utils import (
    is_stg_or_prd_env, is_cgap_env, is_fourfront_env, blue_green_mirror_env, BEANSTALK_PROD_MIRRORS,
    FF_ENV_PRODUCTION_BLUE, FF_ENV_PRODUCTION_GREEN, FF_ENV_WEBPROD, FF_ENV_WEBPROD2, FF_ENV_MASTERTEST,
    FF_ENV_HOTSEAT, FF_ENV_STAGING, FF_ENV_WEBDEV, FF_ENV_WOLF,
    CGAP_ENV_PRODUCTION_BLUE, CGAP_ENV_PRODUCTION_GREEN, CGAP_ENV_WEBPROD, CGAP_ENV_MASTERTEST,
    CGAP_ENV_HOTSEAT, CGAP_ENV_STAGING, CGAP_ENV_WEBDEV, CGAP_ENV_WOLF,
    CGAP_ENV_PRODUCTION_BLUE_NEW, CGAP_ENV_PRODUCTION_GREEN_NEW, CGAP_ENV_WEBPROD_NEW, CGAP_ENV_MASTERTEST_NEW,
    CGAP_ENV_HOTSEAT_NEW, CGAP_ENV_STAGING_NEW, CGAP_ENV_WEBDEV_NEW, CGAP_ENV_WOLF_NEW,
    get_mirror_env_from_context, is_test_env, is_hotseat_env, guess_mirror_env,
    prod_bucket_env, public_url_mappings, CGAP_PUBLIC_URLS, FF_PUBLIC_URLS, FF_PROD_BUCKET_ENV, CGAP_PROD_BUCKET_ENV,
)
from unittest import mock


def test_prod_bucket_env():

    # Fourfront tests

    assert prod_bucket_env('fourfront-webprod') == FF_PROD_BUCKET_ENV
    assert prod_bucket_env('fourfront-webprod2') == FF_PROD_BUCKET_ENV

    assert prod_bucket_env('fourfront-mastertest') is None
    assert prod_bucket_env('fourfront-webdev') is None

    assert prod_bucket_env('fourfront-blue') == FF_PROD_BUCKET_ENV
    assert prod_bucket_env('fourfront-green') == FF_PROD_BUCKET_ENV

    # CGAP tests

    assert prod_bucket_env('fourfront-cgap') == CGAP_PROD_BUCKET_ENV

    assert prod_bucket_env('fourfront-cgap-blue') == CGAP_PROD_BUCKET_ENV
    assert prod_bucket_env('fourfront-cgap-green') == CGAP_PROD_BUCKET_ENV

    assert prod_bucket_env('fourfront-cgapdev') is None
    assert prod_bucket_env('fourfront-cgapwolf') is None


def test_public_url_mappings():

    assert public_url_mappings('fourfront-webprod') == FF_PUBLIC_URLS
    assert public_url_mappings('fourfront-webprod2') == FF_PUBLIC_URLS
    assert public_url_mappings('fourfront-blue') == FF_PUBLIC_URLS
    assert public_url_mappings('fourfront-green') == FF_PUBLIC_URLS

    assert public_url_mappings('fourfront-cgap') == CGAP_PUBLIC_URLS
    assert public_url_mappings('fourfront-cgap-blue') == CGAP_PUBLIC_URLS
    assert public_url_mappings('fourfront-cgap-green') == CGAP_PUBLIC_URLS
    assert public_url_mappings('cgap-blue') == CGAP_PUBLIC_URLS
    assert public_url_mappings('cgap-green') == CGAP_PUBLIC_URLS


def test_blue_green_mirror_env():

    # Should work for basic fourfront
    assert blue_green_mirror_env('fourfront-blue') == 'fourfront-green'
    assert blue_green_mirror_env('fourfront-green') == 'fourfront-blue'

    # Should work for basic cgap
    assert blue_green_mirror_env('cgap-blue') == 'cgap-green'
    assert blue_green_mirror_env('cgap-green') == 'cgap-blue'

    # Anticipated future cases
    assert blue_green_mirror_env('cgap-test-blue') == 'cgap-test-green'
    assert blue_green_mirror_env('cgap-test-green') == 'cgap-test-blue'

    # Things with no mirror have no blue/green in them
    assert blue_green_mirror_env('fourfront-cgap') is None
    assert blue_green_mirror_env('fourfront-mastertest') is None
    assert blue_green_mirror_env('fourfront-yellow') is None

    # Edge cases
    assert blue_green_mirror_env('xyz-green-1') == 'xyz-blue-1'
    assert blue_green_mirror_env('xyz-blue-1') == 'xyz-green-1'
    assert blue_green_mirror_env('xyz-blueish') == 'xyz-greenish'
    assert blue_green_mirror_env('xyz-greenish') == 'xyz-blueish'


def test_is_cgap_env():

    assert is_cgap_env('fourfront-cgap') is True
    assert is_cgap_env('cgap-prod') is True
    assert is_cgap_env('fourfront-blue') is False


def test_is_fourfront_env():

    assert is_fourfront_env('fourfront-cgap') is False
    assert is_fourfront_env('cgap-prod') is False
    assert is_fourfront_env('fourfront-blue') is True


def test_is_stg_or_prd_env():

    assert is_stg_or_prd_env("fourfront-green") is True
    assert is_stg_or_prd_env("fourfront-blue") is True
    assert is_stg_or_prd_env("fourfront-blue-1") is True
    assert is_stg_or_prd_env("fourfront-webprod") is True
    assert is_stg_or_prd_env("fourfront-webprod2") is True

    assert is_stg_or_prd_env("fourfront-yellow") is False
    assert is_stg_or_prd_env("fourfront-mastertest") is False
    assert is_stg_or_prd_env("fourfront-mastertest-1") is False
    assert is_stg_or_prd_env("fourfront-wolf") is False

    assert is_stg_or_prd_env("fourfront-cgap") is True
    assert is_stg_or_prd_env("fourfront-cgap-blue") is True
    assert is_stg_or_prd_env("fourfront-cgap-green") is True

    assert is_stg_or_prd_env("fourfront-cgap-yellow") is False
    assert is_stg_or_prd_env("fourfront-cgapwolf") is False
    assert is_stg_or_prd_env("fourfront-cgaptest") is False

    assert is_stg_or_prd_env("cgap-green") is True
    assert is_stg_or_prd_env("cgap-blue") is True
    assert is_stg_or_prd_env("cgap-dev") is False
    assert is_stg_or_prd_env("cgap-wolf") is False
    assert is_stg_or_prd_env("cgap-test") is False
    assert is_stg_or_prd_env("cgap-yellow") is False


def test_is_test_env():

    assert is_test_env(FF_ENV_HOTSEAT) is True
    assert is_test_env(FF_ENV_MASTERTEST) is True
    assert is_test_env(FF_ENV_WOLF) is True
    assert is_test_env(FF_ENV_WEBDEV) is True

    assert is_test_env(CGAP_ENV_HOTSEAT) is True
    assert is_test_env(CGAP_ENV_MASTERTEST) is True
    assert is_test_env(CGAP_ENV_WOLF) is True
    assert is_test_env(CGAP_ENV_WEBDEV) is True


def test_is_hotseat_env():

    assert is_hotseat_env(FF_ENV_HOTSEAT) is True
    assert is_hotseat_env(FF_ENV_MASTERTEST) is False
    assert is_hotseat_env(FF_ENV_WOLF) is False
    assert is_hotseat_env(FF_ENV_WEBDEV) is False

    assert is_hotseat_env(CGAP_ENV_HOTSEAT) is True
    assert is_hotseat_env(CGAP_ENV_MASTERTEST) is False
    assert is_hotseat_env(CGAP_ENV_WOLF) is False
    assert is_hotseat_env(CGAP_ENV_WEBDEV) is False

    assert is_hotseat_env(CGAP_ENV_HOTSEAT_NEW) is True
    assert is_hotseat_env(CGAP_ENV_MASTERTEST_NEW) is False
    assert is_hotseat_env(CGAP_ENV_WOLF_NEW) is False
    assert is_hotseat_env(CGAP_ENV_WEBDEV_NEW) is False


def test_get_mirror_env_from_context_without_environ():
    """ Tests that when getting mirror env on various envs returns the correct mirror """

    for allow_environ in (False, True):
        # If the environment doesn't have either the ENV_NAME or MIRROR_ENV_NAME environment variables,
        # it won't matter what value we pass for allow_environ.

        with mock.patch.object(os, "environ", {}):
            settings = {'env.name': FF_ENV_WEBPROD, 'mirror.env.name': 'anything'}
            mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
            assert mirror == 'anything'  # overrides any guess we might make

            settings = {'env.name': FF_ENV_WEBPROD}
            mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
            assert mirror == FF_ENV_WEBPROD2  # Not found in environment, but we can guess

            settings = {'env.name': FF_ENV_WEBPROD}
            mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ, allow_guess=False)
            assert mirror is None  # Guessing was suppressed

            settings = {'env.name': FF_ENV_WEBPROD2}
            mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
            assert mirror == FF_ENV_WEBPROD

            settings = {'env.name': FF_ENV_PRODUCTION_GREEN}
            mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
            assert mirror == FF_ENV_PRODUCTION_BLUE

            settings = {'env.name': FF_ENV_PRODUCTION_BLUE}
            mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
            assert mirror == FF_ENV_PRODUCTION_GREEN

            settings = {'env.name': FF_ENV_MASTERTEST}
            mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
            assert mirror is None


def test_get_mirror_env_from_context_with_environ_has_env():
    """ Tests override of env name from os.environ when getting mirror env on various envs """

    with mock.patch.object(os, "environ", {'ENV_NAME': 'foo'}):
        settings = {'env.name': FF_ENV_WEBPROD}
        mirror = get_mirror_env_from_context(settings, allow_environ=True)
        assert mirror is None  # "foo" has no mirror

    with mock.patch.object(os, "environ", {"ENV_NAME": FF_ENV_WEBPROD2}):

        settings = {}
        mirror = get_mirror_env_from_context(settings, allow_environ=True)
        assert mirror == FF_ENV_WEBPROD  # env name explicitly declared, then a guess

        settings = {}
        mirror = get_mirror_env_from_context(settings, allow_environ=True, allow_guess=False)
        assert mirror is None  # env name explicitly declared, but guessing disallowed

        settings = {'env.name': FF_ENV_WEBPROD}
        mirror = get_mirror_env_from_context(settings, allow_environ=True)
        assert mirror == FF_ENV_WEBPROD  # env name in environ overrides env name in file

        settings = {'env.name': FF_ENV_WEBPROD}
        mirror = get_mirror_env_from_context(settings, allow_environ=True, allow_guess=False)
        assert mirror is None  # env name in environ overrides env name in file, but guessing disallowed

        settings = {'env.name': FF_ENV_WEBPROD}
        mirror = get_mirror_env_from_context(settings, allow_environ=False)
        assert mirror == FF_ENV_WEBPROD2  # env name in environ suppressed

        settings = {'env.name': FF_ENV_WEBPROD}
        mirror = get_mirror_env_from_context(settings, allow_environ=False, allow_guess=False)
        assert mirror == None  # env name in environ suppressed, but guessing disallowed


def test_get_mirror_env_from_context_with_environ_has_mirror_env():
    """ Tests override of mirror env name from os.environ when getting mirror env on various envs """

    with mock.patch.object(os, "environ", {"MIRROR_ENV_NAME": 'bar'}):
        settings = {}
        mirror = get_mirror_env_from_context(settings, allow_environ=True)
        assert mirror == 'bar'  # explicitly declared, even if nothing else ise


def test_get_mirror_env_from_context_with_environ_has_env_and_mirror_env():
    """ Tests override of env name and mirror env name from os.environ when getting mirror env on various envs """

    with mock.patch.object(os, "environ", {'ENV_NAME': FF_ENV_WEBPROD2, "MIRROR_ENV_NAME": 'bar'}):
        settings = {'env.name': FF_ENV_WEBPROD}
        mirror = get_mirror_env_from_context(settings, allow_environ=True)
        assert mirror == 'bar'  # mirror explicitly declared, ignoring env name


def test_guess_mirror_env():

    def assert_prod_mirrors(env, expected_mirror_env):
        assert guess_mirror_env(env) is expected_mirror_env
        assert BEANSTALK_PROD_MIRRORS.get(env) is expected_mirror_env

    assert_prod_mirrors(FF_ENV_PRODUCTION_GREEN, FF_ENV_PRODUCTION_BLUE)
    assert_prod_mirrors(FF_ENV_PRODUCTION_BLUE, FF_ENV_PRODUCTION_GREEN)

    assert_prod_mirrors(FF_ENV_WEBPROD, FF_ENV_WEBPROD2)
    assert_prod_mirrors(FF_ENV_WEBPROD2, FF_ENV_WEBPROD)

    assert_prod_mirrors(FF_ENV_MASTERTEST, None)

    assert_prod_mirrors(CGAP_ENV_PRODUCTION_GREEN, CGAP_ENV_PRODUCTION_BLUE)
    assert_prod_mirrors(CGAP_ENV_PRODUCTION_BLUE, CGAP_ENV_PRODUCTION_GREEN)

    assert_prod_mirrors(CGAP_ENV_STAGING, None)

    assert_prod_mirrors(CGAP_ENV_WEBPROD, None)

    assert_prod_mirrors(CGAP_ENV_MASTERTEST, None)

    assert_prod_mirrors(CGAP_ENV_PRODUCTION_GREEN_NEW, CGAP_ENV_PRODUCTION_BLUE_NEW)
    assert_prod_mirrors(CGAP_ENV_PRODUCTION_BLUE_NEW, CGAP_ENV_PRODUCTION_GREEN_NEW)

    assert_prod_mirrors(CGAP_ENV_STAGING_NEW, None)

    # A key difference between the CGAP old and new names is that
    # the new naming assumes we have a blue/green deploy. -kmp 29-Mar-2020
    assert CGAP_ENV_PRODUCTION_GREEN_NEW is CGAP_ENV_WEBPROD_NEW
    assert_prod_mirrors(CGAP_ENV_WEBPROD_NEW, CGAP_ENV_PRODUCTION_BLUE_NEW)
    assert_prod_mirrors(CGAP_ENV_PRODUCTION_BLUE_NEW, CGAP_ENV_WEBPROD_NEW)

    assert_prod_mirrors(CGAP_ENV_MASTERTEST_NEW, None)
