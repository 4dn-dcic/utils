import pytest
import os

from dcicutils.common import APP_CGAP, APP_FOURFRONT
from dcicutils.env_utils import (
    is_stg_or_prd_env, is_cgap_env, is_fourfront_env, blue_green_mirror_env, BEANSTALK_PROD_MIRRORS,
    FF_ENV_PRODUCTION_BLUE, FF_ENV_PRODUCTION_GREEN, FF_ENV_WEBPROD, FF_ENV_WEBPROD2, FF_ENV_MASTERTEST,
    FF_ENV_HOTSEAT, FF_ENV_WEBDEV, FF_ENV_WOLF,
    CGAP_ENV_PRODUCTION_BLUE, CGAP_ENV_PRODUCTION_GREEN, CGAP_ENV_WEBPROD, CGAP_ENV_MASTERTEST,
    CGAP_ENV_HOTSEAT, CGAP_ENV_STAGING, CGAP_ENV_WEBDEV, CGAP_ENV_WOLF,
    CGAP_ENV_PRODUCTION_BLUE_NEW, CGAP_ENV_PRODUCTION_GREEN_NEW, CGAP_ENV_WEBPROD_NEW, CGAP_ENV_MASTERTEST_NEW,
    CGAP_ENV_HOTSEAT_NEW, CGAP_ENV_STAGING_NEW, CGAP_ENV_WEBDEV_NEW, CGAP_ENV_WOLF_NEW,
    FF_PRODUCTION_IDENTIFIER, _CGAP_MGB_PRODUCTION_IDENTIFIER,
    get_mirror_env_from_context, is_test_env, is_hotseat_env, get_standard_mirror_env, prod_bucket_env_for_app,
    prod_bucket_env, public_url_mappings, CGAP_PUBLIC_URLS, FF_PUBLIC_URLS, FF_PROD_BUCKET_ENV, CGAP_PROD_BUCKET_ENV,
    _CGAP_MGB_PUBLIC_URL_PRD, FF_PUBLIC_URL_PRD,
    infer_repo_from_env, data_set_for_env, get_bucket_env, infer_foursight_from_env, infer_foursight_url_from_env,
    FF_STAGING_IDENTIFIER, FF_PUBLIC_DOMAIN_PRD, FF_PUBLIC_DOMAIN_STG, CGAP_ENV_DEV, _CGAP_MGB_PUBLIC_DOMAIN_PRD,
    FF_ENV_INDEXER, CGAP_ENV_INDEXER, is_indexer_env, indexer_env_for_env, classify_server_url,
    short_env_name, full_env_name, full_cgap_env_name, full_fourfront_env_name, is_cgap_server, is_fourfront_server,
    make_env_name_cfn_compatible, default_workflow_env, permit_load_data, public_url_for_app, is_beanstalk_env,
)
from dcicutils.exceptions import InvalidParameterError
from dcicutils.qa_utils import raises_regexp
from unittest import mock


def test_default_workflow_env():

    assert (default_workflow_env('fourfront')
            == default_workflow_env(APP_FOURFRONT)
            == FF_ENV_WEBDEV
            == 'fourfront-webdev')
    assert (default_workflow_env('cgap')
            == default_workflow_env(APP_CGAP)
            == CGAP_ENV_WOLF
            == 'fourfront-cgapwolf')

    with pytest.raises(InvalidParameterError):
        default_workflow_env('foo')


def test_get_bucket_env():

    # Fourfront tests

    assert get_bucket_env('fourfront-webprod') == FF_PROD_BUCKET_ENV
    assert get_bucket_env('fourfront-webprod2') == FF_PROD_BUCKET_ENV

    assert get_bucket_env('fourfront-blue') == FF_PROD_BUCKET_ENV
    assert get_bucket_env('fourfront-green') == FF_PROD_BUCKET_ENV

    assert get_bucket_env('fourfront-mastertest') == 'fourfront-mastertest'
    assert get_bucket_env('fourfront-webdev') == 'fourfront-webdev'

    # CGAP tests

    assert get_bucket_env('fourfront-cgap') == CGAP_PROD_BUCKET_ENV

    assert get_bucket_env('fourfront-cgap-blue') == CGAP_PROD_BUCKET_ENV
    assert get_bucket_env('fourfront-cgap-green') == CGAP_PROD_BUCKET_ENV

    assert get_bucket_env('fourfront-cgapdev') == 'fourfront-cgapdev'
    assert get_bucket_env('fourfront-cgapwolf') == 'fourfront-cgapwolf'


def test_prod_bucket_env_for_app():

    assert prod_bucket_env_for_app('fourfront') == FF_PROD_BUCKET_ENV
    assert prod_bucket_env_for_app('cgap') == CGAP_PROD_BUCKET_ENV

    assert prod_bucket_env_for_app() == FF_PROD_BUCKET_ENV
    assert prod_bucket_env_for_app(None) == FF_PROD_BUCKET_ENV

    with pytest.raises(InvalidParameterError):
        prod_bucket_env_for_app('foo')


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


def test_permit_load_data():

    # Fourfront envs

    fourfront_envs = ['fourfront-blue', 'fourfront-green', 'fourfront-hotseat', 'fourfront-webdev',
                      'fourfront-webprod', 'fourfront-webprod2']

    # This shows concisely what's going on...

    for ff_env in fourfront_envs:
        for allow_prod in [True, False]:
            expected = False if ff_env == 'fourfront-hotseat' else (allow_prod or not is_stg_or_prd_env(ff_env))
            print(f"ff_env={ff_env} allow_prod={allow_prod} orchestrated_app='fourfront' expected={expected}")
            assert permit_load_data(ff_env, allow_prod=allow_prod, orchestrated_app='fourfront') == expected

    # This is redundant, doing it longhand for review...

    assert permit_load_data('fourfront-blue', allow_prod=True, orchestrated_app='fourfront') is True
    assert permit_load_data('fourfront-blue', allow_prod=False, orchestrated_app='fourfront') is False

    assert permit_load_data('fourfront-green', allow_prod=True, orchestrated_app='fourfront') is True
    assert permit_load_data('fourfront-green', allow_prod=False, orchestrated_app='fourfront') is False

    assert permit_load_data('fourfront-hotseat', allow_prod=True, orchestrated_app='fourfront') is False
    assert permit_load_data('fourfront-hotseat', allow_prod=False, orchestrated_app='fourfront') is False

    assert permit_load_data('fourfront-webdev', allow_prod=True, orchestrated_app='fourfront') is True
    assert permit_load_data('fourfront-webdev', allow_prod=False, orchestrated_app='fourfront') is True

    assert permit_load_data('fourfront-webprod', allow_prod=True, orchestrated_app='fourfront') is True
    assert permit_load_data('fourfront-webprod', allow_prod=False, orchestrated_app='fourfront') is False

    assert permit_load_data('fourfront-webprod2', allow_prod=True, orchestrated_app='fourfront') is True
    assert permit_load_data('fourfront-webprod2', allow_prod=False, orchestrated_app='fourfront') is False

    # CGAP envs

    cgap_envs = ['fourfront-cgap', 'fourfront-cgapdev', 'fourfront-cgaptest', 'fourfront-cgapwolf',
                 'cgap-blue', 'cgap-green', 'cgap-dev', 'cgap-test', 'cgap-wolf']

    # This shows concisely what's going on...

    for cgap_env in cgap_envs:
        for allow_prod in [True, False]:
            expected = True if cgap_env == 'fourfront-cgaptest' else allow_prod
            assert permit_load_data(cgap_env, allow_prod=allow_prod, orchestrated_app='cgap') == expected

    # This is redundant, doing it longhand for review...

    assert permit_load_data('fourfront-cgap', allow_prod=True, orchestrated_app='cgap') is True
    assert permit_load_data('fourfront-cgap', allow_prod=False, orchestrated_app='cgap') is False

    assert permit_load_data('fourfront-cgapdev', allow_prod=True, orchestrated_app='cgap') is True
    assert permit_load_data('fourfront-cgapdev', allow_prod=False, orchestrated_app='cgap') is False

    assert permit_load_data('fourfront-cgaptest', allow_prod=True, orchestrated_app='cgap') is True
    assert permit_load_data('fourfront-cgaptest', allow_prod=False, orchestrated_app='cgap') is True

    assert permit_load_data('fourfront-cgapwolf', allow_prod=True, orchestrated_app='cgap') is True
    assert permit_load_data('fourfront-cgapwolf', allow_prod=False, orchestrated_app='cgap') is False

    assert permit_load_data('cgap-blue', allow_prod=True, orchestrated_app='cgap') is True
    assert permit_load_data('cgap-blue', allow_prod=False, orchestrated_app='cgap') is False

    assert permit_load_data('cgap-green', allow_prod=True, orchestrated_app='cgap') is True
    assert permit_load_data('cgap-green', allow_prod=False, orchestrated_app='cgap') is False

    assert permit_load_data('cgap-dev', allow_prod=True, orchestrated_app='cgap') is True
    assert permit_load_data('cgap-dev', allow_prod=False, orchestrated_app='cgap') is False

    assert permit_load_data('cgap-test', allow_prod=True, orchestrated_app='cgap') is True
    assert permit_load_data('cgap-test', allow_prod=False, orchestrated_app='cgap') is False

    assert permit_load_data('cgap-wolf', allow_prod=True, orchestrated_app='cgap') is True
    assert permit_load_data('cgap-wolf', allow_prod=False, orchestrated_app='cgap') is False


def test_data_set_for_env():

    assert data_set_for_env('fourfront-blue') == 'prod'
    assert data_set_for_env('fourfront-green') == 'prod'
    assert data_set_for_env('fourfront-hotseat') == 'prod'
    assert data_set_for_env('fourfront-mastertest') == 'test'
    assert data_set_for_env('fourfront-webdev') == 'prod'
    assert data_set_for_env('fourfront-webprod') == 'prod'
    assert data_set_for_env('fourfront-webprod2') == 'prod'

    assert data_set_for_env('fourfront-cgap') == 'prod'
    assert data_set_for_env('fourfront-cgapdev') == 'test'
    assert data_set_for_env('fourfront-cgaptest') == 'prod'  # oddly, yes, not a typo
    assert data_set_for_env('fourfront-cgapwolf') == 'prod'  # same

    assert data_set_for_env('cgap-blue') == 'prod'
    assert data_set_for_env('cgap-green') == 'prod'
    assert data_set_for_env('cgap-dev') == 'test'
    assert data_set_for_env('cgap-test') == 'prod'  # see above
    assert data_set_for_env('cgap-wolf') == 'prod'  # see above


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


def test_public_url_for_app():

    assert public_url_for_app('cgap') == _CGAP_MGB_PUBLIC_URL_PRD
    assert public_url_for_app('fourfront') == FF_PUBLIC_URL_PRD

    with pytest.raises(InvalidParameterError):
        public_url_for_app('foo')


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

    with pytest.raises(ValueError):
        blue_green_mirror_env('green-blue')  # needs to be one or the other


def test_is_cgap_server():

    with pytest.raises(ValueError):
        is_cgap_server(None)

    assert is_cgap_server("https://data.4dnucleome.org") is False
    assert is_cgap_server("https://staging.4dnucleome.org") is False
    assert is_cgap_server("https://cgap.hms.harvard.edu") is True

    assert is_cgap_server("http://data.4dnucleome.org") is False
    assert is_cgap_server("http://staging.4dnucleome.org") is False
    assert is_cgap_server("http://cgap.hms.harvard.edu") is True

    assert is_cgap_server("data.4dnucleome.org") is False
    assert is_cgap_server("staging.4dnucleome.org") is False
    assert is_cgap_server("cgap.hms.harvard.edu") is True

    assert is_cgap_server("fourfront-mastertest.something.elasticsomething.com") is False
    assert is_cgap_server("fourfront-webdev.something.elasticsomething.com") is False
    assert is_cgap_server("fourfront-hotseat.something.elasticsomething.com") is False
    assert is_cgap_server("fourfront-foo.something.elasticsomething.com") is False
    assert is_cgap_server("fourfront-cgap.something.elasticsomething.com") is True
    assert is_cgap_server("fourfront-cgapdev.something.elasticsomething.com") is True
    assert is_cgap_server("fourfront-cgaptest.something.elasticsomething.com") is True
    assert is_cgap_server("fourfront-cgapwolf.something.elasticsomething.com") is True
    assert is_cgap_server("fourfront-cgapfoo.something.elasticsomething.com") is True

    assert is_cgap_server("localhost") is False
    assert is_cgap_server("localhost", allow_localhost=True) is True

    assert is_cgap_server("www.google.com") is False


def test_is_fourfront_server():
    with pytest.raises(ValueError):
        is_fourfront_server(None)

    assert is_fourfront_server("https://data.4dnucleome.org") is True
    assert is_fourfront_server("https://staging.4dnucleome.org") is True
    assert is_fourfront_server("https://cgap.hms.harvard.edu") is False

    assert is_fourfront_server("http://data.4dnucleome.org") is True
    assert is_fourfront_server("http://staging.4dnucleome.org") is True
    assert is_fourfront_server("http://cgap.hms.harvard.edu") is False

    assert is_fourfront_server("data.4dnucleome.org") is True
    assert is_fourfront_server("staging.4dnucleome.org") is True
    assert is_fourfront_server("cgap.hms.harvard.edu") is False

    assert is_fourfront_server("fourfront-mastertest.something.elasticsomething.com") is True
    assert is_fourfront_server("fourfront-webdev.something.elasticsomething.com") is True
    assert is_fourfront_server("fourfront-hotseat.something.elasticsomething.com") is True
    assert is_fourfront_server("fourfront-foo.something.elasticsomething.com") is True
    assert is_fourfront_server("fourfront-cgap.something.elasticsomething.com") is False
    assert is_fourfront_server("fourfront-cgapdev.something.elasticsomething.com") is False
    assert is_fourfront_server("fourfront-cgaptest.something.elasticsomething.com") is False
    assert is_fourfront_server("fourfront-cgapwolf.something.elasticsomething.com") is False
    assert is_fourfront_server("fourfront-cgapfoo.something.elasticsomething.com") is False

    assert is_fourfront_server("localhost") is False
    assert is_fourfront_server("localhost", allow_localhost=True) is True

    assert is_fourfront_server("www.google.com") is False


def test_is_cgap_env():

    assert is_cgap_env(None) is False

    assert is_cgap_env('fourfront-cgap') is True
    assert is_cgap_env('cgap-prod') is True
    assert is_cgap_env('fourfront-blue') is False


def test_is_fourfront_env():

    assert is_fourfront_env('fourfront-cgap') is False
    assert is_fourfront_env('cgap-prod') is False
    assert is_fourfront_env('fourfront-blue') is True

    assert is_fourfront_env(None) is False


def test_is_beanstalk_env():

    assert is_beanstalk_env('fourfront-webprod') is True
    assert is_beanstalk_env('fourfront-webprod2') is True
    assert is_beanstalk_env('fourfront-blue') is True
    assert is_beanstalk_env('fourfront-green') is True
    assert is_beanstalk_env('fourfront-hotseat') is True
    assert is_beanstalk_env('fourfront-webdev') is True
    assert is_beanstalk_env('fourfront-mastertest') is True

    assert is_beanstalk_env('fourfront-cgap') is True
    assert is_beanstalk_env('fourfront-cgaptest') is True
    assert is_beanstalk_env('fourfront-cgapdev') is True
    assert is_beanstalk_env('fourfront-cgapwolf') is True

    # The magic non-beanstalk names return False
    assert is_beanstalk_env('data') is False
    assert is_beanstalk_env('staging') is False
    assert is_beanstalk_env('cgap') is False

    # Other potential FF and CGAP environment names return False
    assert is_beanstalk_env('fourfront-devtest') is False
    assert is_beanstalk_env('fourfront-supertest') is False
    assert is_beanstalk_env('fourfront-megatest') is False
    assert is_beanstalk_env('fourfront-anything') is False

    assert is_beanstalk_env('fourfront-cgapanything') is False

    assert is_beanstalk_env('cgap-wolf') is False
    assert is_beanstalk_env('cgap-mgb') is False
    assert is_beanstalk_env('cgap-msa') is False
    assert is_beanstalk_env('cgap-dfci') is False
    assert is_beanstalk_env('cgap-core') is False
    assert is_beanstalk_env('cgap-training') is False
    assert is_beanstalk_env('cgap-clalit') is False
    assert is_beanstalk_env('cgap-anything') is False

    assert is_beanstalk_env('anything-at-all') is False


def test_is_stg_or_prd_env():

    assert is_stg_or_prd_env("fourfront-green") is True
    assert is_stg_or_prd_env("fourfront-blue") is True
    assert is_stg_or_prd_env("fourfront-blue-1") is True
    assert is_stg_or_prd_env("fourfront-webprod") is True
    assert is_stg_or_prd_env("fourfront-webprod2") is True

    assert is_stg_or_prd_env(FF_ENV_PRODUCTION_BLUE) is True
    assert is_stg_or_prd_env(FF_ENV_PRODUCTION_GREEN) is True
    assert is_stg_or_prd_env(FF_ENV_PRODUCTION_BLUE) is True
    assert is_stg_or_prd_env(FF_ENV_PRODUCTION_GREEN) is True
    assert is_stg_or_prd_env(FF_ENV_WEBPROD) is True
    assert is_stg_or_prd_env(FF_ENV_WEBPROD2) is True

    assert is_stg_or_prd_env("fourfront-yellow") is False
    assert is_stg_or_prd_env("fourfront-mastertest") is False
    assert is_stg_or_prd_env("fourfront-mastertest-1") is False
    assert is_stg_or_prd_env("fourfront-wolf") is False

    assert is_stg_or_prd_env(FF_ENV_HOTSEAT) is False
    assert is_stg_or_prd_env(FF_ENV_MASTERTEST) is False
    assert is_stg_or_prd_env(FF_ENV_WOLF) is False
    assert is_stg_or_prd_env(FF_ENV_WEBDEV) is False

    assert is_stg_or_prd_env("fourfront-cgap") is True
    assert is_stg_or_prd_env("fourfront-cgap-blue") is True
    assert is_stg_or_prd_env("fourfront-cgap-green") is True

    assert is_stg_or_prd_env(CGAP_ENV_PRODUCTION_BLUE) is True
    assert is_stg_or_prd_env(CGAP_ENV_PRODUCTION_BLUE_NEW) is True
    assert is_stg_or_prd_env(CGAP_ENV_PRODUCTION_GREEN) is True
    assert is_stg_or_prd_env(CGAP_ENV_PRODUCTION_GREEN_NEW) is True
    assert is_stg_or_prd_env(CGAP_ENV_WEBPROD) is True
    assert is_stg_or_prd_env(CGAP_ENV_WEBPROD_NEW) is True

    assert is_stg_or_prd_env("fourfront-cgap-yellow") is False
    assert is_stg_or_prd_env("fourfront-cgapwolf") is False
    assert is_stg_or_prd_env("fourfront-cgaptest") is False

    assert is_stg_or_prd_env(CGAP_ENV_HOTSEAT) is False
    assert is_stg_or_prd_env(CGAP_ENV_MASTERTEST) is False
    assert is_stg_or_prd_env(CGAP_ENV_WOLF) is False
    assert is_stg_or_prd_env(CGAP_ENV_WEBDEV) is False

    assert is_stg_or_prd_env("cgap-green") is True
    assert is_stg_or_prd_env("cgap-blue") is True
    assert is_stg_or_prd_env("cgap-dev") is False
    assert is_stg_or_prd_env("cgap-wolf") is False
    assert is_stg_or_prd_env("cgap-test") is False
    assert is_stg_or_prd_env("cgap-yellow") is False

    assert is_stg_or_prd_env(None) is False


def test_is_test_env():

    assert is_test_env(FF_ENV_PRODUCTION_BLUE) is False
    assert is_test_env(FF_ENV_PRODUCTION_GREEN) is False
    assert is_test_env(FF_ENV_PRODUCTION_BLUE) is False
    assert is_test_env(FF_ENV_PRODUCTION_GREEN) is False
    assert is_test_env(FF_ENV_WEBPROD) is False
    assert is_test_env(FF_ENV_WEBPROD2) is False

    assert is_test_env(FF_ENV_HOTSEAT) is True
    assert is_test_env(FF_ENV_MASTERTEST) is True
    assert is_test_env(FF_ENV_WOLF) is True
    assert is_test_env(FF_ENV_WEBDEV) is True

    assert is_test_env(CGAP_ENV_PRODUCTION_BLUE) is False
    assert is_test_env(CGAP_ENV_PRODUCTION_BLUE_NEW) is False
    assert is_test_env(CGAP_ENV_PRODUCTION_GREEN) is False
    assert is_test_env(CGAP_ENV_PRODUCTION_GREEN_NEW) is False
    assert is_test_env(CGAP_ENV_WEBPROD) is False
    assert is_test_env(CGAP_ENV_WEBPROD_NEW) is False

    assert is_test_env(CGAP_ENV_HOTSEAT) is True
    assert is_test_env(CGAP_ENV_MASTERTEST) is True
    assert is_test_env(CGAP_ENV_WOLF) is True
    assert is_test_env(CGAP_ENV_WEBDEV) is True

    assert is_test_env(None) is False


def test_is_hotseat_env():

    assert is_hotseat_env(FF_ENV_PRODUCTION_BLUE) is False
    assert is_hotseat_env(FF_ENV_PRODUCTION_GREEN) is False
    assert is_hotseat_env(FF_ENV_PRODUCTION_BLUE) is False
    assert is_hotseat_env(FF_ENV_PRODUCTION_GREEN) is False
    assert is_hotseat_env(FF_ENV_WEBPROD) is False
    assert is_hotseat_env(FF_ENV_WEBPROD2) is False

    assert is_hotseat_env(FF_ENV_HOTSEAT) is True
    assert is_hotseat_env(FF_ENV_MASTERTEST) is False
    assert is_hotseat_env(FF_ENV_WOLF) is False
    assert is_hotseat_env(FF_ENV_WEBDEV) is False

    assert is_hotseat_env(CGAP_ENV_PRODUCTION_BLUE) is False
    assert is_hotseat_env(CGAP_ENV_PRODUCTION_BLUE_NEW) is False
    assert is_hotseat_env(CGAP_ENV_PRODUCTION_GREEN) is False
    assert is_hotseat_env(CGAP_ENV_PRODUCTION_GREEN_NEW) is False
    assert is_hotseat_env(CGAP_ENV_WEBPROD) is False
    assert is_hotseat_env(CGAP_ENV_WEBPROD_NEW) is False

    assert is_hotseat_env(CGAP_ENV_HOTSEAT) is True
    assert is_hotseat_env(CGAP_ENV_MASTERTEST) is False
    assert is_hotseat_env(CGAP_ENV_WOLF) is False
    assert is_hotseat_env(CGAP_ENV_WEBDEV) is False

    assert is_hotseat_env(CGAP_ENV_HOTSEAT_NEW) is True
    assert is_hotseat_env(CGAP_ENV_MASTERTEST_NEW) is False
    assert is_hotseat_env(CGAP_ENV_WOLF_NEW) is False
    assert is_hotseat_env(CGAP_ENV_WEBDEV_NEW) is False

    assert is_hotseat_env(None) is False


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

            settings = {'env.name': CGAP_ENV_WEBPROD}
            mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
            assert mirror is None

            settings = {'env.name': CGAP_ENV_WEBDEV}
            mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
            assert mirror is None

            settings = {'env.name': CGAP_ENV_WOLF}
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
        assert mirror is None  # env name in environ suppressed, but guessing disallowed

    with mock.patch.object(os, "environ", {"ENV_NAME": CGAP_ENV_WEBPROD}):

        settings = {}
        mirror = get_mirror_env_from_context(settings, allow_environ=True)
        assert mirror is None  # env name explicitly declared, then a guess (but no CGAP mirror)

        settings = {}
        mirror = get_mirror_env_from_context(settings, allow_environ=True, allow_guess=False)
        assert mirror is None  # env name explicitly declared, but guessing disallowed (but no CGAP mirror)


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


def _test_get_standard_mirror_env(lookup_function):

    def assert_prod_mirrors(env, expected_mirror_env):
        assert lookup_function(env) is expected_mirror_env
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

    # Special cases ...
    assert_prod_mirrors('data', 'staging')
    assert_prod_mirrors('staging', 'data')
    assert_prod_mirrors('cgap', None)


def test_get_standard_mirror_env():
    _test_get_standard_mirror_env(get_standard_mirror_env)


def test_infer_repo_from_env():

    assert infer_repo_from_env(FF_ENV_PRODUCTION_BLUE) == 'fourfront'
    assert infer_repo_from_env(FF_ENV_PRODUCTION_GREEN) == 'fourfront'

    assert infer_repo_from_env(FF_ENV_WEBPROD) == 'fourfront'
    assert infer_repo_from_env(FF_ENV_WEBPROD2) == 'fourfront'

    assert infer_repo_from_env('fourfront-blue') == 'fourfront'
    assert infer_repo_from_env('fourfront-mastertest') == 'fourfront'

    assert infer_repo_from_env('fourfront-foo') == 'fourfront'

    assert infer_repo_from_env(CGAP_ENV_PRODUCTION_BLUE) == 'cgap-portal'
    assert infer_repo_from_env(CGAP_ENV_PRODUCTION_BLUE) == 'cgap-portal'

    assert infer_repo_from_env(CGAP_ENV_WEBPROD) == 'cgap-portal'

    assert infer_repo_from_env('fourfront-cgap') == 'cgap-portal'
    assert infer_repo_from_env('fourfront-cgapwolf') == 'cgap-portal'
    assert infer_repo_from_env('fourfront-cgapdev') == 'cgap-portal'

    assert infer_repo_from_env('cgap-green') == 'cgap-portal'
    assert infer_repo_from_env('cgap-blue') == 'cgap-portal'
    assert infer_repo_from_env('cgap-wolf') == 'cgap-portal'
    assert infer_repo_from_env('cgap-dev') == 'cgap-portal'

    assert infer_repo_from_env('cgap-foo') == 'cgap-portal'
    assert infer_repo_from_env('fourfront-cgapfoo') == 'cgap-portal'

    # Edge cases that the code specifically looks for:
    assert infer_repo_from_env(None) is None
    assert infer_repo_from_env('who-knows') is None


def test_infer_foursight_from_env():

    class MockedRequest:
        def __init__(self, domain):
            self.domain = domain

    def mock_request(domain=None):  # build a dummy request with the 'domain' member, checked in the method
        if domain is None:
            return None
        else:
            return MockedRequest(domain)

    def check(token_in, token_out, request=None):
        request = request or mock_request()
        assert infer_foursight_from_env(request, token_in) == token_out

    # (active) fourfront testing environments
    check(FF_ENV_MASTERTEST, 'mastertest')
    check(FF_ENV_WEBDEV, 'webdev')
    check(FF_ENV_HOTSEAT, 'hotseat')

    # (active) fourfront production environments
    check('fourfront-blue', FF_PRODUCTION_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_PRD))
    check('fourfront-green', FF_PRODUCTION_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_PRD))

    check('fourfront-blue', FF_STAGING_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_STG))
    check('fourfront-green', FF_STAGING_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_STG))

    # These next four are pathological and hopefully not used, but they illustrate that the domain dominates.
    # This does not illustrate intended use.
    check(FF_PRODUCTION_IDENTIFIER, FF_PRODUCTION_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_PRD))
    check(FF_STAGING_IDENTIFIER, FF_PRODUCTION_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_PRD))

    check(FF_PRODUCTION_IDENTIFIER, FF_STAGING_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_STG))
    check(FF_STAGING_IDENTIFIER, FF_STAGING_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_STG))

    # Traditionally these didn't work as inputs, but that seems silly, so I made them work, too. -kmp 4-Oct-2021
    check('data', FF_PRODUCTION_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_PRD))
    check('staging', FF_STAGING_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_STG))

    check('', None)
    check(None, None)

    # (active) cgap environments
    check(CGAP_ENV_DEV, 'cgapdev')
    check(CGAP_ENV_MASTERTEST, 'cgaptest')
    check(CGAP_ENV_WOLF, 'cgapwolf')
    check(CGAP_ENV_WEBPROD, 'cgap')

    # Traditionally this didn't work as inputs, but that seems silly, so I made it work, too. -kmp 4-Oct-2021
    check('cgap', _CGAP_MGB_PRODUCTION_IDENTIFIER, request=mock_request(_CGAP_MGB_PUBLIC_DOMAIN_PRD))

    check('', None)
    check(None, None)


def test_infer_foursight_url_from_env():

    class MockedRequest:
        def __init__(self, domain):
            self.domain = domain

    def mock_request(domain=None):  # build a dummy request with the 'domain' member, checked in the method
        if domain is None:
            return None
        else:
            return MockedRequest(domain)

    ff_foursight_prefix = 'https://foursight.4dnucleome.org/view/'
    cg_foursight_prefix = "https://u9feld4va7.execute-api.us-east-1.amazonaws.com/api/view/"

    def check(token_in, token_out, request=None, cgap=False):
        request = request or mock_request()
        prefix = cg_foursight_prefix if cgap else ff_foursight_prefix
        expected = prefix + token_out if token_out else None
        assert infer_foursight_url_from_env(request, token_in) == expected

    # (active) fourfront testing environments
    check(FF_ENV_MASTERTEST, 'mastertest')
    check(FF_ENV_WEBDEV, 'webdev')
    check(FF_ENV_HOTSEAT, 'hotseat')

    # (active) fourfront production environments
    check('fourfront-blue', FF_PRODUCTION_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_PRD))
    check('fourfront-green', FF_PRODUCTION_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_PRD))

    check('fourfront-blue', FF_STAGING_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_STG))
    check('fourfront-green', FF_STAGING_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_STG))

    # These next four are pathological and hopefully not used, but they illustrate that the domain dominates.
    # This does not illustrate intended use.
    check(FF_PRODUCTION_IDENTIFIER, FF_PRODUCTION_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_PRD))
    check(FF_STAGING_IDENTIFIER, FF_PRODUCTION_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_PRD))

    check(FF_PRODUCTION_IDENTIFIER, FF_STAGING_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_STG))
    check(FF_STAGING_IDENTIFIER, FF_STAGING_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_STG))

    # Traditionally these didn't work as inputs, but that seems silly, so I made them work, too. -kmp 4-Oct-2021
    check('data', FF_PRODUCTION_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_PRD))
    check('staging', FF_STAGING_IDENTIFIER, request=mock_request(FF_PUBLIC_DOMAIN_STG))

    check('', None)
    check(None, None)

    # (active) cgap environments
    check(CGAP_ENV_DEV, 'cgapdev', cgap=True)
    check(CGAP_ENV_MASTERTEST, 'cgaptest', cgap=True)
    check(CGAP_ENV_WOLF, 'cgapwolf', cgap=True)
    check(CGAP_ENV_WEBPROD, 'cgap', cgap=True)

    # Traditionally this didn't work as inputs, but that seems silly, so I made it work, too. -kmp 4-Oct-2021
    check('cgap', _CGAP_MGB_PRODUCTION_IDENTIFIER, request=mock_request(_CGAP_MGB_PUBLIC_DOMAIN_PRD), cgap=True)

    check('', None, cgap=True)
    check(None, None, cgap=True)


def test_indexer_env_for_env():

    assert indexer_env_for_env('fourfront-mastertest') == FF_ENV_INDEXER
    assert indexer_env_for_env('fourfront-blue') == FF_ENV_INDEXER
    assert indexer_env_for_env('fourfront-green') == FF_ENV_INDEXER
    assert indexer_env_for_env('fourfront-webdev') == FF_ENV_INDEXER
    assert indexer_env_for_env('fourfront-hotseat') == FF_ENV_INDEXER

    assert indexer_env_for_env('fourfront-cgap') == CGAP_ENV_INDEXER
    assert indexer_env_for_env('fourfront-cgapdev') == CGAP_ENV_INDEXER
    assert indexer_env_for_env('fourfront-cgaptest') == CGAP_ENV_INDEXER
    assert indexer_env_for_env('fourfront-cgapwolf') == CGAP_ENV_INDEXER

    assert indexer_env_for_env('fourfront-indexer') is None
    assert indexer_env_for_env('cgap-indexer') is None
    assert indexer_env_for_env('blah-env') is None


def test_is_indexer_env():

    assert is_indexer_env('fourfront-indexer')
    assert is_indexer_env(FF_ENV_INDEXER)

    assert is_indexer_env('cgap-indexer')
    assert is_indexer_env(CGAP_ENV_INDEXER)

    # Try a few non-indexers ...
    assert not is_indexer_env('fourfront-cgap')
    assert not is_indexer_env('fourfront-blue')
    assert not is_indexer_env('fourfront-green')
    assert not is_indexer_env('fourfront-mastertest')
    assert not is_indexer_env('fourfront-cgapwolf')


def test_short_env_name():

    assert short_env_name('cgapdev') == 'cgapdev'
    assert short_env_name('mastertest') == 'mastertest'

    assert short_env_name('fourfront-cgapdev') == 'cgapdev'
    assert short_env_name('fourfront-mastertest') == 'mastertest'

    # Does not require a registered env
    assert short_env_name('foo') == 'foo'
    assert short_env_name('cgapfoo') == 'cgapfoo'

    # Names like 'staging' and 'data' are not specially recognized, but are unperturbed
    # because this just returns the token when it doesn't start with 'fourfront-'.
    assert short_env_name('staging') == 'staging'
    assert short_env_name('data') == 'data'
    assert short_env_name('cgap') == 'cgap'
    assert short_env_name('anything') == 'anything'

    assert short_env_name('fourfront_mastertest') == 'fourfront_mastertest'


def test_full_env_name():

    assert full_env_name('cgapdev') == 'fourfront-cgapdev'
    assert full_env_name('mastertest') == 'fourfront-mastertest'

    assert full_env_name('fourfront-cgapdev') == 'fourfront-cgapdev'
    assert full_env_name('fourfront-mastertest') == 'fourfront-mastertest'

    assert full_env_name('fourfront_mastertest') == 'fourfront_mastertest'

    # Does not require a registered env
    assert full_env_name('foo') == 'fourfront-foo'
    assert full_env_name('cgapfoo') == 'fourfront-cgapfoo'

    # But 'staging' and 'data' are special and don't work here.
    with pytest.raises(ValueError):
        full_env_name('staging')

    with pytest.raises(ValueError):
        full_env_name('data')


def test_full_cgap_env_name():
    assert full_cgap_env_name('cgapdev') == 'fourfront-cgapdev'
    assert full_cgap_env_name('fourfront-cgapdev') == 'fourfront-cgapdev'

    # Does not require a registered env
    assert full_cgap_env_name('cgapfoo') == 'fourfront-cgapfoo'

    with pytest.raises(ValueError):
        full_cgap_env_name('mastertest')

    with pytest.raises(ValueError):
        full_cgap_env_name('fourfront-mastertest')

    with pytest.raises(ValueError):
        full_cgap_env_name('fourfront_mastertest')

    with pytest.raises(ValueError):
        full_cgap_env_name('foo')

    # Special names 'staging' and 'data' don't work here.
    with pytest.raises(ValueError):
        full_cgap_env_name('staging')

    with pytest.raises(ValueError):
        full_cgap_env_name('data')


def test_full_fourfront_env_name():

    assert full_fourfront_env_name('mastertest') == 'fourfront-mastertest'
    assert full_fourfront_env_name('fourfront-mastertest') == 'fourfront-mastertest'

    assert full_fourfront_env_name('fourfront_mastertest') == 'fourfront_mastertest'

    # Does not require a registered env
    assert full_fourfront_env_name('foo') == 'fourfront-foo'

    with pytest.raises(ValueError):
        full_fourfront_env_name('cgapdev')

    with pytest.raises(ValueError):
        full_fourfront_env_name('fourfront-cgapdev')

    with pytest.raises(ValueError):
        full_fourfront_env_name('cgapfoo')

    # Special names 'staging' and 'data' don't work here.
    with pytest.raises(ValueError):
        full_fourfront_env_name('staging')

    with pytest.raises(ValueError):
        full_fourfront_env_name('data')


def test_classify_server_url_localhost():

    assert classify_server_url("http://localhost/foo/bar") == {
        'kind': 'localhost',
        'environment': 'unknown',
        'is_stg_or_prd': False,
    }

    assert classify_server_url("http://localhost:8000/foo/bar") == {
        'kind': 'localhost',
        'environment': 'unknown',
        'is_stg_or_prd': False,
    }

    assert classify_server_url("http://localhost:1234/foo/bar") == {
        'kind': 'localhost',
        'environment': 'unknown',
        'is_stg_or_prd': False,
    }

    assert classify_server_url("http://127.0.0.1:8000/foo/bar") == {
        'kind': 'localhost',
        'environment': 'unknown',
        'is_stg_or_prd': False,
    }


def test_classify_server_url_cgap():

    assert classify_server_url("https://cgap.hms.harvard.edu/foo/bar") == {
        'kind': 'cgap',
        'environment': 'fourfront-cgap',
        'is_stg_or_prd': True,
    }

    assert classify_server_url("http://fourfront-cgapdev.9wzadzju3p.us-east-1.elasticbeanstalk.com/foo/bar") == {
        'kind': 'cgap',
        'environment': 'fourfront-cgapdev',
        'is_stg_or_prd': False,
    }

    assert classify_server_url("http://fourfront-cgapdev.anything.us-east-1.elasticbeanstalk.com/foo/bar") == {
        'kind': 'cgap',
        'environment': 'fourfront-cgapdev',
        'is_stg_or_prd': False,
    }

    assert classify_server_url("http://fourfront-cgapwolf.anything.us-east-1.elasticbeanstalk.com/foo/bar") == {
        'kind': 'cgap',
        'environment': 'fourfront-cgapwolf',
        'is_stg_or_prd': False,
    }


def test_classify_server_url_fourfront():

    assert classify_server_url("https://data.4dnucleome.org/foo/bar") == {
        'kind': 'fourfront',
        'environment': 'fourfront-webprod',
        'is_stg_or_prd': True,
    }

    assert classify_server_url("https://staging.4dnucleome.org/foo/bar") == {
        'kind': 'fourfront',
        'environment': 'fourfront-webprod',
        'is_stg_or_prd': True,
    }

    assert classify_server_url("http://fourfront-blue.9wzadzju3p.us-east-1.elasticbeanstalk.com/foo/bar") == {
        'kind': 'fourfront',
        'environment': 'fourfront-webprod',
        'is_stg_or_prd': True,
    }

    assert classify_server_url("http://fourfront-green.9wzadzju3p.us-east-1.elasticbeanstalk.com/foo/bar") == {
        'kind': 'fourfront',
        'environment': 'fourfront-webprod',
        'is_stg_or_prd': True,
    }

    assert classify_server_url("http://fourfront-mastertest.9wzadzju3p.us-east-1.elasticbeanstalk.com/foo/bar") == {
        'kind': 'fourfront',
        'environment': 'fourfront-mastertest',
        'is_stg_or_prd': False,
    }


def test_classify_server_url_other():

    with raises_regexp(RuntimeError, "not a Fourfront or CGAP server"):
        classify_server_url("http://google.com")  # raise_error=True is the default

    with raises_regexp(RuntimeError, "not a Fourfront or CGAP server"):
        classify_server_url("http://google.com", raise_error=True)

    assert classify_server_url("http://google.com", raise_error=False) == {
        'kind': 'unknown',
        'environment': 'unknown',
        'is_stg_or_prd': False,
    }


@pytest.mark.parametrize('env_name, cfn_id', [
    ('cgap-mastertest', 'cgapmastertest'),
    ('fourfront-cgap', 'fourfrontcgap'),
    ('cgap-msa', 'cgapmsa'),
    ('fourfrontmastertest', 'fourfrontmastertest')
])
def test_make_env_name_cfn_compatible(env_name, cfn_id):
    assert make_env_name_cfn_compatible(env_name) == cfn_id
