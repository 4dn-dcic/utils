import contextlib
import functools
import json
import os
import pytest

from dcicutils.env_base import LegacyController
from dcicutils.common import APP_CGAP, APP_FOURFRONT  # , LEGACY_GLOBAL_ENV_BUCKET
from dcicutils.env_manager import EnvManager
from dcicutils.env_utils import (
    is_stg_or_prd_env, is_cgap_env, is_fourfront_env, blue_green_mirror_env, env_equals,
    get_mirror_env_from_context, is_test_env, is_hotseat_env, get_standard_mirror_env,
    prod_bucket_env, prod_bucket_env_for_app, public_url_mappings, public_url_for_app, permit_load_data,
    default_workflow_env, infer_foursight_url_from_env, foursight_env_name,
    infer_repo_from_env, data_set_for_env, get_bucket_env, infer_foursight_from_env,
    is_indexer_env, indexer_env_for_env, classify_server_url,
    short_env_name, full_env_name, full_cgap_env_name, full_fourfront_env_name, is_cgap_server, is_fourfront_server,
    # make_env_name_cfn_compatible,
    get_foursight_bucket, get_foursight_bucket_prefix, ecr_repository_for_env,
    # New support
    EnvUtils, p, c, get_env_real_url,
    _make_no_legacy,  # noQA - yes, protected, but we want to test it
    if_orchestrated, UseLegacy,
)
from dcicutils.env_utils_legacy import (
    FF_PRODUCTION_ECR_REPOSITORY, blue_green_mirror_env as legacy_blue_green_mirror_env
)
from dcicutils.exceptions import (
    BeanstalkOperationNotImplemented,  # MissingFoursightBucketTable, IncompleteFoursightBucketTable,
    EnvUtilsLoadError, LegacyDispatchDisabled,
)
from dcicutils.misc_utils import decorator, local_attrs, ignorable, override_environ
from dcicutils.qa_utils import raises_regexp
from typing import Optional
from unittest import mock
from urllib.parse import urlparse
from .helpers import using_fresh_cgap_state_for_testing, using_fresh_ff_state_for_testing


ignorable(BeanstalkOperationNotImplemented)  # Stuff that does or doesn't use this might come and go


@contextlib.contextmanager
def stage_mirroring(*, enabled=True):
    with local_attrs(EnvUtils, STAGE_MIRRORING_ENABLED=enabled):
        yield


@contextlib.contextmanager
def orchestrated_behavior_for_testing(data: Optional[dict] = None):
    """
    Context manager that arranges for a dynamic executation context to use a specified ecosystem description.

    :param data: an ecosystem description (default EnvUtils.SAMPLE_TEMPLATE_FOR_CGAP_TESTING)
    """
    snapshot = EnvUtils.snapshot_envutils_state_for_testing()
    try:
        EnvUtils.set_declared_data(data or EnvUtils.SAMPLE_TEMPLATE_FOR_CGAP_TESTING)
        yield
    finally:
        EnvUtils.restore_envutils_state_from_snapshot_for_testing(snapshot)


@decorator()
def using_orchestrated_behavior(data: Optional[dict] = None):
    """
     Decorator that arranges for the function it decorates to dynamically use a specified ecosystem description.

    :param data: an ecosystem description (default EnvUtils.SAMPLE_TEMPLATE_FOR_CGAP_TESTING)
    """
    def _decorate(fn):

        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            with orchestrated_behavior_for_testing(data=data):
                result = fn(*args, **kwargs)
            return result

        return _wrapped

    return _decorate


@using_orchestrated_behavior()
def test_orchestrated_ecr_repository_for_cgap_env():

    for env in ['acme-prd', 'acme-stg']:
        val = ecr_repository_for_env(env)
        print(f"env={env} val={val}")
        assert val != FF_PRODUCTION_ECR_REPOSITORY
        assert val == env
    for env in ['acme-mastertest', 'acme-foo']:
        val = ecr_repository_for_env(env)
        print(f"env={env} val={val}")
        assert val == env


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_ecr_repository_for_ff_env():

    for env in ['acme-prd', 'acme-stg']:
        val = ecr_repository_for_env(env)
        print(f"env={env} is_stg_or_prd={is_stg_or_prd_env(env)} val={val}")
        assert val != FF_PRODUCTION_ECR_REPOSITORY
        assert val != env
        assert val == EnvUtils.PRODUCTION_ECR_REPOSITORY
    for env in ['acme-mastertest', 'acme-foo']:
        val = ecr_repository_for_env(env)
        print(f"env={env} is_stg_or_prd={is_stg_or_prd_env(env)} val={val}")
        assert val == env


@using_orchestrated_behavior()
def test_orchestrated_get_bucket_env():

    assert EnvUtils.PRD_ENV_NAME == 'acme-prd'
    assert EnvUtils.WEBPROD_PSEUDO_ENV == 'production-data'
    assert get_bucket_env(EnvUtils.PRD_ENV_NAME) == EnvUtils.WEBPROD_PSEUDO_ENV
    assert EnvUtils.STG_ENV_NAME is None

    def test_the_usual_scenario():
        assert get_bucket_env('acme-prd') == 'production-data'  # PRD_ENV_NAME
        assert get_bucket_env('cgap') == 'production-data'      # mentioned in PUBLIC_URL_TABLE

        assert get_bucket_env('acme-wolf') == 'acme-wolf'       # normal env, uses bucket name exactly
        assert get_bucket_env('acme-foo') == 'acme-foo'         # normal env, uses bucket name exactly
        assert get_bucket_env('foo') == 'foo'                   # normal env, uses bucket name exactly

        assert get_bucket_env('acme-stg') == 'acme-stg'         # NOTE: Just a normal env. Staging is not enabled.
        assert get_bucket_env('stg') == 'stg'                   # NOTE: Alias for acme-stg, but that's no special env.

    test_the_usual_scenario()

    # The only way get_bucket_env differs from the identity function in the orchestrated environment
    # is the hypothetical situation where we were supporting mirroring. In that case, the staging environment
    # would be collapsed with the production environment so that they share buckets. Since we have no mirroring,
    # we can only test that by a mock of is_stg_or_prd_env. -kmp 24-Jul-2021
    with local_attrs(EnvUtils, STG_ENV_NAME='acme-stg'):

        test_the_usual_scenario()  # The STG_ENV_NAME is ignored if "stage_mirroring_enabled": false

        with stage_mirroring(enabled=True):

            assert EnvUtils.STG_ENV_NAME == 'acme-stg'

            assert get_bucket_env('acme-prd') == 'production-data'  # PRD_ENV_NAME
            assert get_bucket_env('cgap') == 'production-data'      # mentioned in PUBLIC_URL_TABLE

            assert get_bucket_env('acme-wolf') == 'acme-wolf'       # normal env, uses bucket name exactly
            assert get_bucket_env('acme-foo') == 'acme-foo'         # normal env, uses bucket name exactly
            assert get_bucket_env('foo') == 'foo'                   # normal env, uses bucket name exactly

            # with mirroring enabled, this uses prod bucket
            assert get_bucket_env('acme-stg') == 'production-data'
            # with mirroring enabled, this alias for acme-stg uses prod bucket
            assert get_bucket_env('stg') == 'production-data'


@pytest.mark.skip(reason="Beanstalk functionality no longer supported.")
@using_orchestrated_behavior()
def test_orchestrated_prod_bucket_env():

    assert EnvUtils.PRD_ENV_NAME == 'acme-prd'
    assert EnvUtils.WEBPROD_PSEUDO_ENV == 'production-data'
    assert prod_bucket_env(EnvUtils.PRD_ENV_NAME) == EnvUtils.WEBPROD_PSEUDO_ENV
    assert EnvUtils.STG_ENV_NAME is None

    def test_the_usual_scenario():

        assert prod_bucket_env('acme-prd') == 'production-data'  # PRD_ENV_NAME
        assert prod_bucket_env('cgap') == 'production-data'      # Aliased to acmd-prd in PUBLIC_URL_TABLE

        assert prod_bucket_env('acme-wolf') is None     # normal env, just returns None
        assert prod_bucket_env('acme-foo') is None      # normal env, just returns None
        assert prod_bucket_env('foo') is None           # normal env, just returns None

        assert prod_bucket_env('acme-stg') is None      # NOTE: Just a normal env. Staging is not enabled.
        assert prod_bucket_env('stg') is None           # NOTE: Just a normal env. Staging is not enabled.

    test_the_usual_scenario()

    # The only way get_bucket_env differs from the identity function in the orchestrated environment
    # is the hypothetical situation where we were supporting mirroring. In that case, the staging environment
    # would be collapsed with the production environment so that they share buckets. Since we have no mirroring,
    # we can only test that by a mock of is_stg_or_prd_env. -kmp 24-Jul-2021
    with local_attrs(EnvUtils, STG_ENV_NAME='acme-stg'):

        test_the_usual_scenario()  # The STG_ENV_NAME is ignored if "stage_mirroring_enabled": false

        with stage_mirroring(enabled=True):

            assert EnvUtils.STG_ENV_NAME == 'acme-stg'

            assert prod_bucket_env('acme-prd') == 'production-data'      # PRD_ENV_NAME
            assert prod_bucket_env('cgap') == 'production-data'          # in PUBLIC_URL_TABLE

            assert prod_bucket_env('acme-wolf') is None                  # normal env, just returns None
            assert prod_bucket_env('acme-foo') is None                   # normal env, just returns None
            assert prod_bucket_env('foo') is None                        # normal env, just returns None

            assert prod_bucket_env('acme-stg') == 'production-data'      # WIT mirroring enabled, this uses prod bucket
            assert prod_bucket_env('stg') == 'production-data'           # WITH mirroring enabled, this uses prod bucket


@pytest.mark.skip(reason="Beanstalk functionality no longer supported.")
@using_orchestrated_behavior()
def test_orchestrated_prod_bucket_env_for_app():

    assert prod_bucket_env_for_app() == EnvUtils.WEBPROD_PSEUDO_ENV
    assert prod_bucket_env_for_app('cgap') == EnvUtils.WEBPROD_PSEUDO_ENV
    with pytest.raises(Exception):
        prod_bucket_env_for_app('fourfront')

    with local_attrs(EnvUtils, **CGAP_SETTINGS_FOR_TESTING):
        assert prod_bucket_env_for_app() == EnvUtils.WEBPROD_PSEUDO_ENV == 'fourfront-cgap'
        assert prod_bucket_env_for_app('cgap') == EnvUtils.WEBPROD_PSEUDO_ENV
        with pytest.raises(Exception):
            prod_bucket_env_for_app('fourfront')

    with local_attrs(EnvUtils, **FOURFRONT_SETTINGS_FOR_TESTING):
        assert prod_bucket_env_for_app() == EnvUtils.WEBPROD_PSEUDO_ENV == 'fourfront-webprod'
        assert prod_bucket_env_for_app('fourfront') == EnvUtils.WEBPROD_PSEUDO_ENV
        with pytest.raises(Exception):
            prod_bucket_env_for_app('cgap')


@using_orchestrated_behavior()
def test_orchestrated_infer_foursight_url_from_env():

    assert (infer_foursight_url_from_env(request='ignored-request', envname='demo')
            == 'https://foursight.genetics.example.com/api/view/demo')
    actual = infer_foursight_url_from_env(request='ignored-request', envname='acme-foo')
    expected = 'https://foursight.genetics.example.com/api/view/foo'
    assert actual == expected
    assert (infer_foursight_url_from_env(request='ignored-request', envname='fourfront-cgapwolf')
            == 'https://foursight.genetics.example.com/api/view/fourfront-cgapwolf')

    with local_attrs(EnvUtils, **FOURFRONT_SETTINGS_FOR_TESTING):
        assert (infer_foursight_url_from_env(request='ignored-request', envname='data')
                == 'https://foursight.4dnucleome.org/api/view/data')
        assert (infer_foursight_url_from_env(request='ignored-request', envname='acme-foo')
                == 'https://foursight.4dnucleome.org/api/view/acme-foo')
        assert (infer_foursight_url_from_env(request='ignored-request', envname='fourfront-cgapwolf')
                == 'https://foursight.4dnucleome.org/api/view/cgapwolf')

    with local_attrs(EnvUtils, **CGAP_SETTINGS_FOR_TESTING):
        assert (infer_foursight_url_from_env(request='ignored-request', envname='data')
                == 'https://u9feld4va7.execute-api.us-east-1.amazonaws.com/api/view/data')
        assert (infer_foursight_url_from_env(request='ignored-request', envname='acme-foo')
                == 'https://u9feld4va7.execute-api.us-east-1.amazonaws.com/api/view/acme-foo')
        assert (infer_foursight_url_from_env(request='ignored-request', envname='fourfront-cgapwolf')
                == 'https://u9feld4va7.execute-api.us-east-1.amazonaws.com/api/view/cgapwolf')


@using_fresh_ff_state_for_testing()
def test_ff_default_workflow_env():

    assert (default_workflow_env('fourfront')
            == default_workflow_env(APP_FOURFRONT)
            == 'fourfront-webdev')

    with pytest.raises(Exception):
        default_workflow_env('foo')  # noQA - we expect this error

    with pytest.raises(Exception):
        default_workflow_env(APP_CGAP)  # noQA - we expect this error


@using_fresh_cgap_state_for_testing()
def test_cgap_default_workflow_env():

    assert (default_workflow_env('cgap')
            == default_workflow_env(APP_CGAP)
            == 'cgap-wolf')

    with pytest.raises(Exception):
        default_workflow_env('foo')  # noQA - we expect this error

    with pytest.raises(Exception):
        default_workflow_env(APP_FOURFRONT)  # noQA - we expect this error


@using_orchestrated_behavior()
def test_orchestrated_permit_load_data():

    def test_it():
        assert permit_load_data(envname=EnvUtils.PRD_ENV_NAME, allow_prod=True, orchestrated_app='cgap') is True
        assert permit_load_data(envname=EnvUtils.PRD_ENV_NAME, allow_prod=False, orchestrated_app='cgap') is False

        assert permit_load_data(envname=EnvUtils.PRD_ENV_NAME, allow_prod=True, orchestrated_app='fourfront') is True
        assert permit_load_data(envname=EnvUtils.PRD_ENV_NAME, allow_prod=False, orchestrated_app='fourfront') is False

        assert permit_load_data(envname='anything', allow_prod=True, orchestrated_app='cgap') is True
        assert permit_load_data(envname='anything', allow_prod=False, orchestrated_app='cgap') is False

        assert permit_load_data(envname='anything', allow_prod=True, orchestrated_app='fourfront') is True
        assert permit_load_data(envname='anything', allow_prod=False, orchestrated_app='fourfront') is False

    # The orchestrated definition is not dependent on the environment name or orchestrated app,
    # so our testing reflects the constancy of effect...

    test_it()

    with local_attrs(EnvUtils, **CGAP_SETTINGS_FOR_TESTING):
        test_it()

    with local_attrs(EnvUtils, **FOURFRONT_SETTINGS_FOR_TESTING):
        test_it()


@using_orchestrated_behavior()
def test_orchestrated_data_set_for_env():

    assert EnvUtils.DEV_DATA_SET_TABLE == {'acme-hotseat': 'prod', 'acme-test': 'test'}

    # Production environments are always prod
    assert data_set_for_env('acme-prd') == 'prod'
    assert data_set_for_env('cgap') == 'prod'
    # These are declared in the data sets table
    assert data_set_for_env('acme-hotseat') == 'prod'
    assert data_set_for_env('acme-test') == 'test'
    # These are not declared in the data sets
    assert data_set_for_env('acme-mastertest') is None
    assert data_set_for_env('acme-dev') is None
    assert data_set_for_env('acme-foo') is None

    # Production environments are always prod
    assert data_set_for_env('acme-prd', 'test') == 'prod'
    assert data_set_for_env('cgap', 'test') == 'prod'
    # These are declared in the data sets table
    assert data_set_for_env('acme-hotseat', 'test') == 'prod'
    assert data_set_for_env('acme-test', 'test') == 'test'
    # These are not declared in the data sets
    assert data_set_for_env('acme-mastertest', 'test') == 'test'
    assert data_set_for_env('acme-dev', 'test') == 'test'
    assert data_set_for_env('acme-foo', 'test') == 'test'

    assert data_set_for_env('acme-stg') is None
    assert data_set_for_env('stg') is None
    assert data_set_for_env('stg', 'test') == 'test'
    assert data_set_for_env('acme-stg', 'test') == 'test'

    with local_attrs(EnvUtils, STG_ENV_NAME='acme-stg'):

        # Setting EnvUtils.STG_ENV_NAME doesn't work unless "stage_mirroring_enabled": true at top-level.
        assert data_set_for_env('acme-stg') is None
        assert data_set_for_env('stg') is None
        assert data_set_for_env('acme-stg', 'test') == 'test'
        assert data_set_for_env('stg', 'test') == 'test'

        with stage_mirroring(enabled=True):

            assert data_set_for_env('acme-stg') == 'prod'
            assert data_set_for_env('stg') == 'prod'
            assert data_set_for_env('acme-stg', 'test') == 'prod'
            assert data_set_for_env('stg', 'test') == 'prod'


@using_orchestrated_behavior()
def test_get_foursight_bucket_prefix():

    with override_environ(GLOBAL_BUCKET_ENV=None, GLOBAL_ENV_BUCKET=None):

        with EnvManager.global_env_bucket_named('some-sample-envs'):
            # If the global bucket ends in '-envs', we guess
            assert get_foursight_bucket_prefix() == 'some-sample'

        with EnvManager.global_env_bucket_named('some-sample-environments'):
            # If the global bucket doesn't end in '-envs', we don't guess
            with pytest.raises(Exception):
                get_foursight_bucket_prefix()

        with EnvUtils.local_env_utils_for_testing():
            some_prefix = 'sample-foursight-bucket-prefix'
            EnvUtils.FOURSIGHT_BUCKET_PREFIX = some_prefix
            assert get_foursight_bucket_prefix() == some_prefix

        with pytest.raises(Exception):
            get_foursight_bucket_prefix()


@using_orchestrated_behavior()
def test_orchestrated_public_url_mappings():

    sample_table_for_testing = EnvUtils.PUBLIC_URL_TABLE

    # This "test" is to show you what's there. Note that the URL doesn't have to have 'cgap' in its name.
    # For that matter, the key name doesn't have to be cgap either. But it should be PRD_ENV_NAME
    # or something in PUBLIC_URL_TABLE.
    public_name_1 = 'cgap'
    public_url_1 = 'https://cgap.genetics.example.com'
    public_env_1 = 'acme-prd'
    public_name_2 = 'stg'
    public_url_2 = 'https://staging.genetics.example.com'
    public_env_2 = 'acme-stg'
    public_name_3 = 'testing'
    public_url_3 = 'https://testing.genetics.example.com'
    public_env_3 = 'acme-pubtest'
    public_name_4 = 'demo'
    public_url_4 = 'https://demo.genetics.example.com'
    public_env_4 = 'acme-pubdemo'
    expected_table = [
        {
            p.NAME: public_name_1,
            p.URL: public_url_1,
            p.HOST: urlparse(public_url_1).hostname,
            p.ENVIRONMENT: public_env_1,
        },
        {
            p.NAME: public_name_2,
            p.URL: public_url_2,
            p.HOST: urlparse(public_url_2).hostname,
            p.ENVIRONMENT: public_env_2,
        },
        {
            p.NAME: public_name_3,
            p.URL: public_url_3,
            p.HOST: urlparse(public_url_3).hostname,
            p.ENVIRONMENT: public_env_3,
        },
        {
            p.NAME: public_name_4,
            p.URL: public_url_4,
            p.HOST: urlparse(public_url_4).hostname,
            p.ENVIRONMENT: public_env_4,
        },
    ]
    assert sample_table_for_testing == expected_table
    sample_mapping_for_testing = {
        public_name_1: public_url_1,
        public_name_2: public_url_2,
        public_name_3: public_url_3,
        public_name_4: public_url_4,
    }

    assert public_url_mappings('acme-prd') == sample_mapping_for_testing  # PRD_ENV_NAME
    assert public_url_mappings('cgap') == sample_mapping_for_testing      # member of PUBLIC_URL_TABLE
    assert public_url_mappings('acme-foo') == sample_mapping_for_testing  # correct prefix ("acme-")

    # This last one is slightly different than in legacy where there was a chance of bumping into Foursight.
    # The whole point of an orchestrated version is there is no Fourfront in a CGAP ecosystem, or vice versa.
    assert public_url_mappings('foo') == sample_mapping_for_testing       # everything has same table


@using_orchestrated_behavior()
def test_orchestrated_blue_green_mirror_env():

    # This doesn't depend on anything but the name. It's completely a string operation.
    # We could just use the legacy version, but it has bugs. This one should instead become the legacy version.

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
        blue_green_mirror_env('xyz-blue-green')  # needs to be one or the other


@using_orchestrated_behavior()
def test_orchestrated_public_url_for_app():

    assert public_url_for_app() == "https://cgap.genetics.example.com"
    assert public_url_for_app('cgap') == "https://cgap.genetics.example.com"
    with pytest.raises(Exception):
        public_url_for_app('fourfront')  # The example app is not a fourfront app

    with local_attrs(EnvUtils, **CGAP_SETTINGS_FOR_TESTING):
        assert public_url_for_app() == "https://cgap.hms.harvard.edu"
        assert public_url_for_app('cgap') == "https://cgap.hms.harvard.edu"
        with pytest.raises(Exception):
            public_url_for_app('fourfront')  # A cgap app won't know about fourfront

    with local_attrs(EnvUtils, **FOURFRONT_SETTINGS_FOR_TESTING):
        assert public_url_for_app() == "https://data.4dnucleome.org"
        assert public_url_for_app('fourfront') == "https://data.4dnucleome.org"
        with pytest.raises(Exception):
            public_url_for_app('cgap')  # A fourfront app won't know about fourfront


@using_orchestrated_behavior()
def test_orchestrated_is_cgap_server_for_cgap():

    assert is_cgap_server('anything') is True
    assert is_cgap_server('anything', allow_localhost=True) is True


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_is_cgap_server_for_fourfront():

    assert is_cgap_server('anything') is False
    assert is_cgap_server('anything', allow_localhost=True) is False


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_is_fourfront_server_for_fourfront():

    assert is_fourfront_server('anything') is True
    assert is_fourfront_server('anything', allow_localhost=True) is True


@using_orchestrated_behavior()
def test_orchestrated_is_fourfront_server_for_cgap():

    assert is_fourfront_server('anything') is False
    assert is_fourfront_server('anything', allow_localhost=True) is False


@using_orchestrated_behavior()
def test_orchestrated_is_cgap_env_for_cgap():

    # Non-strings return False
    assert is_cgap_env(None) is False

    # Anything starting with the prefix we declared ("acme-")
    assert is_cgap_env('acme-prd') is True
    assert is_cgap_env('acme-foo') is True
    # Anything that's in the PUBLIC_URL_TABLE
    assert is_cgap_env('cgap') is True  # in the cgap table only
    assert is_cgap_env('data') is False  # in the fourfront table only

    # We don't operate on wired substrings now, and these don't have the right prefix.
    assert is_cgap_env('fourfront-cgap') is False
    assert is_cgap_env('cgap-prod') is False
    assert is_cgap_env('fourfront-blue') is False


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_is_cgap_env_for_fourfront():

    # Non-strings return False
    assert is_cgap_env(None) is False

    # This never returns True in a fourfront orchestration
    assert is_cgap_env('acme-prd') is False
    assert is_cgap_env('acme-foo') is False
    assert is_cgap_env('cgap') is False
    assert is_cgap_env('data') is False
    assert is_cgap_env('fourfront-cgap') is False
    assert is_cgap_env('cgap-prod') is False
    assert is_cgap_env('fourfront-blue') is False


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_is_fourfront_env_for_fourfront():

    # Non-strings return False
    assert is_fourfront_env(None) is False

    # Anything starting with the prefix we declared ("acme-")
    assert is_fourfront_env('acme-prd') is True
    assert is_fourfront_env('acme-foo') is True
    # Anything that's in the PUBLIC_URL_TABLE
    assert is_fourfront_env('data') is True   # in the fourfront table only
    assert is_fourfront_env('cgap') is False  # in the cgap table only

    # We don't operate on wired substrings now, and these don't have the right prefix.
    assert is_fourfront_env('fourfront-cgap') is False
    assert is_fourfront_env('cgap-prod') is False
    assert is_fourfront_env('fourfront-blue') is False


@using_orchestrated_behavior()
def test_orchestrated_is_fourfront_env_for_cgap():

    # Non-strings return False
    assert is_fourfront_env(None) is False

    # This never returns True in a fourfront orchestration
    assert is_fourfront_env('acme-prd') is False
    assert is_fourfront_env('acme-foo') is False
    assert is_fourfront_env('data') is False
    assert is_fourfront_env('cgap') is False
    assert is_fourfront_env('fourfront-cgap') is False
    assert is_fourfront_env('cgap-prod') is False
    assert is_fourfront_env('fourfront-blue') is False


@using_orchestrated_behavior()
def test_orchestrated_is_stg_or_prd_env_for_cgap():

    assert is_stg_or_prd_env('cgap') is True
    assert is_stg_or_prd_env('data') is False
    assert is_stg_or_prd_env('acme-prd') is True
    assert is_stg_or_prd_env('acme-test') is False
    assert is_stg_or_prd_env('anything') is False

    assert is_stg_or_prd_env('acme-stg') is False

    with local_attrs(EnvUtils, STG_ENV_NAME='acme-stg'):

        assert is_stg_or_prd_env('acme-stg') is False

        with stage_mirroring(enabled=True):

            assert is_stg_or_prd_env('acme-stg') is True


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_is_stg_or_prd_env_for_fourfront():

    assert is_stg_or_prd_env('data') is True
    assert is_stg_or_prd_env('cgap') is False
    assert is_stg_or_prd_env('acme-prd') is True
    assert is_stg_or_prd_env('acme-test') is False
    assert is_stg_or_prd_env('anything') is False

    assert is_stg_or_prd_env('acme-stg') is True

    # Not declaring a stg_env_name is enough to disable staging.
    with local_attrs(EnvUtils, STG_ENV_NAME=None):
        assert is_stg_or_prd_env('acme-stg') is False

    # Not enabling stage mirroring is enough to disable staging
    with stage_mirroring(enabled=False):
        assert is_stg_or_prd_env('acme-stg') is False


@using_orchestrated_behavior()
def test_orchestrated_is_test_env_for_cgap():

    assert EnvUtils.TEST_ENVS == ['acme-test', 'acme-mastertest', 'acme-pubtest']

    assert is_test_env('acme-prd') is False
    assert is_test_env('acme-stg') is False

    assert is_test_env('acme-test') is True
    assert is_test_env('acme-mastertest') is True
    assert is_test_env('acme-pubtest') is True
    assert is_test_env('testing') is True          # Declared for CGAP testing, not for Fourfront testing

    assert is_test_env('test') is False            # Declared for Fourfront testing, not for CGAP testing
    assert is_test_env('acme-supertest') is False  # Not a declared test env for either ecosystem

    assert is_test_env('foo') is False


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_is_test_env_for_fourfront():

    assert EnvUtils.TEST_ENVS == ['acme-test', 'acme-mastertest', 'acme-pubtest']

    assert is_test_env('acme-prd') is False
    assert is_test_env('acme-stg') is False

    assert is_test_env('acme-test') is True
    assert is_test_env('acme-mastertest') is True
    assert is_test_env('acme-pubtest') is True
    assert is_test_env('test') is True             # Declared for Fourfront testing, not for CGAP testing

    assert is_test_env('testing') is False         # Declared for CGAP testing, not for Fourfront testing
    assert is_test_env('acme-supertest') is False  # Not a declared test env for either ecosystem

    assert is_test_env('foo') is False


@using_orchestrated_behavior()
def test_orchestrated_is_hotseat_env_for_cgap():

    assert EnvUtils.HOTSEAT_ENVS == ['acme-hotseat', 'acme-pubdemo']

    assert is_hotseat_env('acme-prd') is False
    assert is_hotseat_env('acme-stg') is False

    assert is_hotseat_env('acme-hotseat') is True
    assert is_hotseat_env('acme-pubdemo') is True
    assert is_hotseat_env('demo') is True  # in PUBLIC_URL_TABLE, this is an alias for acme-pubdemo

    assert is_hotseat_env('acme-demo') is False  # not a declared hotseat env, not in PUBLIC_URL_TABLE

    assert is_hotseat_env('foo') is False


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_is_hotseat_env_for_fourfront():

    assert EnvUtils.HOTSEAT_ENVS == ['acme-hotseat']

    assert is_hotseat_env('acme-prd') is False
    assert is_hotseat_env('acme-stg') is False

    assert is_hotseat_env('acme-hotseat') is True
    assert is_hotseat_env('acme-pubdemo') is False  # acme-pubdemo is not a hotseat environments in Fourfront testing
    assert is_hotseat_env('demo') is False  # PUBLIC_URL_TABLE declares an alias for non-hotseat env acme-pubdemo
    assert is_hotseat_env('hot') is True    # PUBLIC_URL_TABLE declares an alias for hotseat env acme-hotseat

    assert is_hotseat_env('acme-demo') is False  # not a declared hotseat env, not in PUBLIC_URL_TABLE

    assert is_hotseat_env('foo') is False


@using_orchestrated_behavior
def test_orchestrated_get_env_from_context():
    # There was no legacy unit test for this.
    # TODO: Write a unit test for both legacy and orchestrated case.
    #       But for now we're using the legacy definition, and it should be OK.
    #       Also, this is implicitly tested by functions that call it.
    pass


@using_orchestrated_behavior()
def test_orchestrated_get_mirror_env_from_context_without_environ_with_mirror_disabled():
    """ Tests that when getting mirror env on various envs returns the correct mirror """

    for allow_environ in (False, True):
        # If the environment doesn't have either the ENV_NAME or MIRROR_ENV_NAME environment variables,
        # it won't matter what value we pass for allow_environ.

        with mock.patch.object(os, "environ", {}):
            settings = {'env.name': 'acme-prd', 'mirror.env.name': 'anything'}
            mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
            assert mirror is None

            settings = {'env.name': 'acme-prd'}
            mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
            assert mirror is None

            settings = {'env.name': 'acme-prd'}
            mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ, allow_guess=False)
            assert mirror is None

            settings = {'env.name': 'acme-stg'}
            mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
            assert mirror is None

            settings = {'env.name': 'acme-test'}
            mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
            assert mirror is None

            settings = {'env.name': 'acme-mastertest'}
            mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
            assert mirror is None


@using_orchestrated_behavior()
def test_orchestrated_get_mirror_env_from_context_without_environ_with_mirror_enabled():
    """ Tests that when getting mirror env on various envs returns the correct mirror """

    with stage_mirroring(enabled=True):
        with local_attrs(EnvUtils, STG_ENV_NAME='acme-stg'):
            for allow_environ in (False, True):
                # If the environment doesn't have either the ENV_NAME or MIRROR_ENV_NAME environment variables,
                # it won't matter what value we pass for allow_environ.

                with mock.patch.object(os, "environ", {}):
                    settings = {'env.name': 'acme-prd', 'mirror.env.name': 'anything'}
                    mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
                    assert mirror == 'anything'  # overrides any guess we might make

                    settings = {'env.name': 'acme-prd'}
                    mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
                    assert mirror == 'acme-stg'  # Not found in environment, but we can guess

                    settings = {'env.name': 'acme-prd'}
                    mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ, allow_guess=False)
                    assert mirror is None  # Guessing was suppressed

                    settings = {'env.name': 'acme-stg'}
                    mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
                    assert mirror == 'acme-prd'

                    settings = {'env.name': 'acme-test'}
                    mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
                    assert mirror is None

                    settings = {'env.name': 'acme-mastertest'}
                    mirror = get_mirror_env_from_context(settings, allow_environ=allow_environ)
                    assert mirror is None


@using_orchestrated_behavior()
def test_orchestrated_get_mirror_env_from_context_with_environ_has_env_with_mirror_disabled():
    """ Tests override of env name from os.environ when getting mirror env on various envs """

    with mock.patch.object(os, "environ", {'ENV_NAME': 'foo'}):
        settings = {'env.name': 'acme-prd'}
        mirror = get_mirror_env_from_context(settings, allow_environ=True)
        assert mirror is None

    with mock.patch.object(os, "environ", {"ENV_NAME": 'acme-stg'}):

        settings = {}
        mirror = get_mirror_env_from_context(settings, allow_environ=True)
        assert mirror is None

        settings = {}
        mirror = get_mirror_env_from_context(settings, allow_environ=True, allow_guess=False)
        assert mirror is None

        settings = {'env.name': 'acme-prd'}
        mirror = get_mirror_env_from_context(settings, allow_environ=True)
        assert mirror is None

        settings = {'env.name': 'acme-prd'}
        mirror = get_mirror_env_from_context(settings, allow_environ=True, allow_guess=False)
        assert mirror is None

        settings = {'env.name': 'acme-prd'}
        mirror = get_mirror_env_from_context(settings, allow_environ=False)
        assert mirror is None

        settings = {'env.name': 'acme-prd'}
        mirror = get_mirror_env_from_context(settings, allow_environ=False, allow_guess=False)
        assert mirror is None

    with mock.patch.object(os, "environ", {"ENV_NAME": 'acme-prd'}):

        settings = {}
        mirror = get_mirror_env_from_context(settings, allow_environ=True)
        assert mirror is None

        settings = {}
        mirror = get_mirror_env_from_context(settings, allow_environ=True, allow_guess=False)
        assert mirror is None


@using_orchestrated_behavior()
def test_orchestrated_get_mirror_env_from_context_with_environ_has_env_with_mirror_enabled():
    """ Tests override of env name from os.environ when getting mirror env on various envs """

    with stage_mirroring(enabled=True):
        with local_attrs(EnvUtils, STG_ENV_NAME='acme-stg'):

            with mock.patch.object(os, "environ", {'ENV_NAME': 'foo'}):
                settings = {'env.name': 'acme-prd'}
                mirror = get_mirror_env_from_context(settings, allow_environ=True)
                assert mirror is None  # "foo" has no mirror

            with mock.patch.object(os, "environ", {"ENV_NAME": 'acme-stg'}):

                settings = {}
                mirror = get_mirror_env_from_context(settings, allow_environ=True)
                assert mirror == 'acme-prd'  # env name explicitly declared, then a guess

                settings = {}
                mirror = get_mirror_env_from_context(settings, allow_environ=True, allow_guess=False)
                assert mirror is None  # env name explicitly declared, but guessing disallowed

                settings = {'env.name': 'acme-prd'}
                mirror = get_mirror_env_from_context(settings, allow_environ=True)
                assert mirror == 'acme-prd'  # env name in environ overrides env name in file

                settings = {'env.name': 'acme-prd'}
                mirror = get_mirror_env_from_context(settings, allow_environ=True, allow_guess=False)
                assert mirror is None  # env name in environ overrides env name in file, but guessing disallowed

                settings = {'env.name': 'acme-prd'}
                mirror = get_mirror_env_from_context(settings, allow_environ=False)
                assert mirror == 'acme-stg'  # env name in environ suppressed

                settings = {'env.name': 'acme-prd'}
                mirror = get_mirror_env_from_context(settings, allow_environ=False, allow_guess=False)
                assert mirror is None  # env name in environ suppressed, but guessing disallowed

            with mock.patch.object(os, "environ", {"ENV_NAME": 'acme-prd'}):

                settings = {}
                mirror = get_mirror_env_from_context(settings, allow_environ=True)
                assert mirror


@using_orchestrated_behavior()
def test_orchestrated_get_mirror_env_from_context_with_environ_has_mirror_env_with_mirror_disabled():
    """ Tests override of mirror env name from os.environ when getting mirror env on various envs """

    with mock.patch.object(os, "environ", {"MIRROR_ENV_NAME": 'bar'}):
        settings = {}
        mirror = get_mirror_env_from_context(settings, allow_environ=True)
        assert mirror is None


@using_orchestrated_behavior()
def test_orchestrated_get_mirror_env_from_context_with_environ_has_mirror_env_with_mirror_enabled():
    """ Tests override of mirror env name from os.environ when getting mirror env on various envs """

    with stage_mirroring(enabled=True):
        with local_attrs(EnvUtils, STG_ENV_NAME='acme-stg'):

            with mock.patch.object(os, "environ", {"MIRROR_ENV_NAME": 'bar'}):
                settings = {}
                mirror = get_mirror_env_from_context(settings, allow_environ=True)
                assert mirror == 'bar'  # explicitly declared, even if nothing else ise


@using_orchestrated_behavior()
def test_orchestrated_get_mirror_env_from_context_with_environ_has_env_and_mirror_env_with_mirror_disabled():
    """ Tests override of env name and mirror env name from os.environ when getting mirror env on various envs """

    with mock.patch.object(os, "environ", {'ENV_NAME': 'acme-stg', "MIRROR_ENV_NAME": 'bar'}):
        settings = {'env.name': 'acme-prd'}
        mirror = get_mirror_env_from_context(settings, allow_environ=True)
        assert mirror is None


@using_orchestrated_behavior()
def test_orchestrated_get_mirror_env_from_context_with_environ_has_env_and_mirror_env_with_mirror_enabled():
    """ Tests override of env name and mirror env name from os.environ when getting mirror env on various envs """

    with stage_mirroring(enabled=True):
        with local_attrs(EnvUtils, STG_ENV_NAME='acme-stg'):

            with mock.patch.object(os, "environ", {'ENV_NAME': 'acme-stg', "MIRROR_ENV_NAME": 'bar'}):
                settings = {'env.name': 'acme-prd'}
                mirror = get_mirror_env_from_context(settings, allow_environ=True)
                assert mirror == 'bar'  # mirror explicitly declared, ignoring env name


@using_orchestrated_behavior()
def test_orchestrated_get_standard_mirror_env_for_cgap():

    for mirroring_enabled in [True, False]:
        with local_attrs(EnvUtils, STG_ENV_NAME='acme-stg'):
            with stage_mirroring(enabled=mirroring_enabled):

                def expected_result(value):
                    return value if mirroring_enabled else None

                assert get_standard_mirror_env('acme-prd') == expected_result('acme-stg')
                assert get_standard_mirror_env('acme-stg') == expected_result('acme-prd')

                assert get_standard_mirror_env('cgap') == expected_result('stg')
                assert get_standard_mirror_env('stg') == expected_result('cgap')

                assert get_standard_mirror_env('acme-foo') is None


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_get_standard_mirror_env_for_fourfront():

    for mirroring_enabled in [True, False]:
        with local_attrs(EnvUtils, STG_ENV_NAME='acme-stg'):
            with stage_mirroring(enabled=mirroring_enabled):

                def expected_result(value):
                    return value if mirroring_enabled else None

                assert get_standard_mirror_env('acme-prd') == expected_result('acme-stg')
                assert get_standard_mirror_env('acme-stg') == expected_result('acme-prd')

                assert get_standard_mirror_env('data') == expected_result('staging')
                assert get_standard_mirror_env('staging') == expected_result('data')

                assert get_standard_mirror_env('acme-foo') is None


@using_orchestrated_behavior
def test_orchestrated_infer_repo_from_env_for_cgap():

    assert infer_repo_from_env('acme-prd') == 'cgap-portal'
    assert infer_repo_from_env('acme-stg') == 'cgap-portal'
    assert infer_repo_from_env('acme-test') == 'cgap-portal'

    assert infer_repo_from_env('cgap') == 'cgap-portal'  # this is a declared name

    assert infer_repo_from_env('demo') == 'cgap-portal'  # this is a declared name

    assert infer_repo_from_env('data') is None
    assert infer_repo_from_env('staging') is None

    assert infer_repo_from_env('who-knows') is None
    assert infer_repo_from_env(None) is None


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_infer_repo_from_env_for_fourfront():

    assert infer_repo_from_env('acme-prd') == 'fourfront'
    assert infer_repo_from_env('acme-stg') == 'fourfront'
    assert infer_repo_from_env('acme-test') == 'fourfront'

    assert infer_repo_from_env('cgap') is None

    assert infer_repo_from_env('demo') == 'fourfront'

    assert infer_repo_from_env('data') == 'fourfront'  # this is a declared name
    assert infer_repo_from_env('staging') == 'fourfront'  # this is a declared name

    assert infer_repo_from_env('who-knows') is None
    assert infer_repo_from_env(None) is None


CGAP_SETTINGS_FOR_TESTING = dict(
    ORCHESTRATED_APP='cgap',
    FOURSIGHT_URL_PREFIX='https://u9feld4va7.execute-api.us-east-1.amazonaws.com/api/view/',
    FULL_ENV_PREFIX='fourfront-',
    DEV_ENV_DOMAIN_SUFFIX=EnvUtils.DEV_SUFFIX_FOR_TESTING,
    WEBPROD_PSEUDO_ENV='fourfront-cgap',
    PRD_ENV_NAME='fourfront-cgap',
    STG_ENV_NAME=None,
    PUBLIC_URL_TABLE=[
        {
            p.NAME: 'cgap',
            p.URL: "https://cgap.hms.harvard.edu",
            p.HOST: "cgap.hms.harvard.edu",
            p.ENVIRONMENT: "fourfront-cgap"
        },
    ]
)

FOURFRONT_SETTINGS_FOR_TESTING = dict(
    ORCHESTRATED_APP='fourfront',
    FOURSIGHT_URL_PREFIX='https://foursight.4dnucleome.org/api/view/',
    FULL_ENV_PREFIX='fourfront-',
    DEV_ENV_DOMAIN_SUFFIX=EnvUtils.DEV_SUFFIX_FOR_TESTING,
    WEBPROD_PSEUDO_ENV='fourfront-webprod',
    PRD_ENV_NAME='fourfront-blue',
    STG_ENV_NAME='fourfront-green',
    PUBLIC_URL_TABLE=[
        {
            p.NAME: 'data',
            p.URL: "https://data.4dnucleome.org",
            p.HOST: "data.4dnucleome.org",
            p.ENVIRONMENT: "fourfront-blue"
        },
        {
            p.NAME: 'staging',
            p.URL: "https://staging.4dnucleome.org",
            p.HOST: "staging.4dnucleome.org",
            p.ENVIRONMENT: "fourfront-green"
        }
    ]
)


@using_orchestrated_behavior()
def test_orchestrated_foursight_env_name():

    assert foursight_env_name('acme-prd') == 'cgap'  # this is in our test ecosystem's public_urL_table
    assert foursight_env_name('acme-mastertest') == 'mastertest'  # the rest of these are short names
    assert foursight_env_name('acme-webdev') == 'webdev'
    assert foursight_env_name('acme-hotseat') == 'hotseat'

    with stage_mirroring(enabled=True):
        with local_attrs(EnvUtils, **FOURFRONT_SETTINGS_FOR_TESTING):  # PRD = fourfront-blue, STG = fourfront-green

            assert foursight_env_name('fourfront-blue') == 'data'  # this is in the public_url_table
            assert foursight_env_name('fourfront-green') == 'staging'  # this is, too
            assert foursight_env_name('fourfront-mastertest') == 'mastertest'  # these are short names
            assert foursight_env_name('fourfront-hotseat') == 'hotseat'


@using_orchestrated_behavior()
def test_orchestrated_infer_foursight_from_env():

    dev_suffix = EnvUtils.DEV_SUFFIX_FOR_TESTING

    class MockedRequest:
        def __init__(self, domain):
            self.domain = domain

    def mock_request(domain):  # build a dummy request with the 'domain' member, checked in the method
        return MockedRequest(domain)

    assert infer_foursight_from_env(request=mock_request('acme-prd' + dev_suffix),
                                    envname='acme-prd') == 'cgap'
    assert infer_foursight_from_env(request=mock_request('acme-mastertest' + dev_suffix),
                                    envname='acme-mastertest') == 'mastertest'
    assert infer_foursight_from_env(request=mock_request('acme-webdev' + dev_suffix),
                                    envname='acme-webdev') == 'webdev'
    assert infer_foursight_from_env(request=mock_request('acme-hotseat' + dev_suffix),
                                    envname='acme-hotseat') == 'hotseat'

    with stage_mirroring(enabled=True):

        with local_attrs(EnvUtils, **FOURFRONT_SETTINGS_FOR_TESTING):  # PRD = Blue, STG = Green

            # (active) fourfront testing environments
            assert infer_foursight_from_env(request=mock_request('fourfront-mastertest' + dev_suffix),
                                            envname='fourfront-mastertest') == 'mastertest'
            assert infer_foursight_from_env(request=mock_request('fourfront-webdev' + dev_suffix),
                                            envname='fourfront-webdev') == 'webdev'
            assert infer_foursight_from_env(request=mock_request('fourfront-hotseat' + dev_suffix),
                                            envname='fourfront-hotseat') == 'hotseat'

            # (active) fourfront production environments
            assert (infer_foursight_from_env(request=mock_request(domain='data.4dnucleome.org'),
                                             envname='fourfront-blue')
                    == 'data')
            assert (infer_foursight_from_env(request=mock_request(domain='data.4dnucleome.org'),
                                             envname='fourfront-green')
                    == 'staging')  # Inconsistent args. The envname is used in preference to the request
            assert (infer_foursight_from_env(request=mock_request(domain='staging.4dnucleome.org'),
                                             envname='fourfront-blue')
                    == 'data')  # Inconsistent args. The envname is used in preference to the request
            assert (infer_foursight_from_env(request=mock_request(domain='staging.4dnucleome.org'),
                                             envname='fourfront-green')
                    == 'staging')

            # These next four are pathological and hopefully not used, but they illustrate that the domain dominates.
            # This does not illustrate intended use.
            assert (infer_foursight_from_env(request=mock_request(domain='data.4dnucleome.org'), envname='data')
                    == 'data')
            assert (infer_foursight_from_env(request=mock_request(domain='data.4dnucleome.org'), envname='staging')
                    == 'staging')  # Inconsistent args. The envname is used in preference to the request

            assert (infer_foursight_from_env(request=mock_request(domain='staging.4dnucleome.org'), envname='data')
                    == 'data')  # Inconsistent args. The envname is used in preference to the request
            assert (infer_foursight_from_env(request=mock_request(domain='staging.4dnucleome.org'), envname='staging')
                    == 'staging')

            assert (infer_foursight_from_env(request='data.4dnucleome.org', envname='data') == 'data')
            # Inconsistent args. The envname is used in preference to the request
            assert (infer_foursight_from_env(request='data.4dnucleome.org', envname='staging') == 'staging')

            assert (infer_foursight_from_env(request='https://data.4dnucleome.org', envname='data') == 'data')
            # Inconsistent args. The envname is used in preference to the request
            assert (infer_foursight_from_env(request='https://data.4dnucleome.org', envname='staging') == 'staging')

            # Inconsistent args. The envname is used in preference to the request
            assert (infer_foursight_from_env(request='staging.4dnucleome.org', envname='data') == 'data')
            assert (infer_foursight_from_env(request='staging.4dnucleome.org', envname='staging') == 'staging')

            # Inconsistent args. The envname is used in preference to the request
            assert (infer_foursight_from_env(request='http://staging.4dnucleome.org', envname='data') == 'data')
            assert (infer_foursight_from_env(request='http://staging.4dnucleome.org', envname='staging') == 'staging')

            assert (infer_foursight_from_env(request=None, envname='data') == 'data')
            assert (infer_foursight_from_env(request=None, envname='staging') == 'staging')

        # (active) cgap environments
        with local_attrs(EnvUtils, **CGAP_SETTINGS_FOR_TESTING):

            assert infer_foursight_from_env(request=mock_request('fourfront-cgapdev' + dev_suffix),
                                            envname='fourfront-cgapdev') == 'cgapdev'
            assert infer_foursight_from_env(request=mock_request('fourfront-cgaptest' + dev_suffix),
                                            envname='fourfront-cgaptest') == 'cgaptest'
            assert infer_foursight_from_env(request=mock_request('fourfront-cgapwolf' + dev_suffix),
                                            envname='fourfront-cgapwolf') == 'cgapwolf'
            assert infer_foursight_from_env(request=mock_request('fourfront-cgap' + dev_suffix),
                                            envname='fourfront-cgap') == 'cgap'

            assert infer_foursight_from_env(request=mock_request('cgap.hms.harvard.edu'),
                                            envname='fourfront-cgap') == 'cgap'
            assert infer_foursight_from_env(request=mock_request('cgap.hms.harvard.edu'),
                                            envname='cgap') == 'cgap'


@pytest.mark.skip
@using_orchestrated_behavior()
def test_orchestrated_indexer_env_for_env():

    assert EnvUtils.INDEXER_ENV_NAME == 'acme-indexer'

    # The indexer does not think it has an indexer
    assert indexer_env_for_env('acme-indexer') is None

    # All other environments use a canonical indexer
    assert indexer_env_for_env('acme-prd') == 'acme-indexer'
    assert indexer_env_for_env('acme-test') == 'acme-indexer'
    assert indexer_env_for_env('acme-anything') == 'acme-indexer'
    assert indexer_env_for_env('blah-blah') == 'acme-indexer'


@using_orchestrated_behavior()
def test_orchestrated_indexer_env_for_env_disabled():

    assert EnvUtils.INDEXER_ENV_NAME == 'acme-indexer'

    # We've disabled calls to this. The indexer isn't done this way in containers.
    for env in ['acme-indexer', 'acme-prd', 'acme-test', 'acme-anything', 'blah-blah']:
        # We tried raising an error and opted to just return None
        # with pytest.raises(BeanstalkOperationNotImplemented):
        assert indexer_env_for_env(env) is None


@pytest.mark.skip
@using_orchestrated_behavior()
def test_orchestrated_is_indexer_env():

    assert EnvUtils.INDEXER_ENV_NAME == 'acme-indexer'

    # This should be true for the indexer env, False for others
    assert is_indexer_env('acme-indexer') is True

    assert is_indexer_env('acme-prd') is False
    assert is_indexer_env('acme-test') is False
    assert is_indexer_env('acme-foo') is False
    assert is_indexer_env('pretty-much-anything') is False


@using_orchestrated_behavior()
def test_orchestrated_is_indexer_env_disabled():

    assert EnvUtils.INDEXER_ENV_NAME == 'acme-indexer'

    # We've disabled calls to this. The indexer isn't done this way in containers.
    for env in ['acme-indexer', 'acme-prd', 'acme-test', 'acme-anything', 'blah-blah']:
        # We tried raising an error and opted to just return False
        # with pytest.raises(BeanstalkOperationNotImplemented):
        assert is_indexer_env(env) is False


@using_orchestrated_behavior()
def test_orchestrated_short_env_name():

    assert short_env_name(None) is None
    assert short_env_name('demo') == 'pubdemo'
    assert short_env_name('anything') == 'anything'
    assert short_env_name('acme-anything') == 'anything'
    assert short_env_name('cgap-anything') == 'cgap-anything'
    assert short_env_name('fourfront-cgapfoo') == 'fourfront-cgapfoo'
    assert short_env_name('fourfront-anything') == 'fourfront-anything'

    with local_attrs(EnvUtils, **CGAP_SETTINGS_FOR_TESTING):  # Legacy CGAP settings use a 'fourfront-' prefix!

        assert short_env_name(None) is None
        assert short_env_name('demo') == 'demo'
        assert short_env_name('anything') == 'anything'
        assert short_env_name('acme-anything') == 'acme-anything'
        assert short_env_name('cgap-anything') == 'cgap-anything'
        assert short_env_name('fourfront-cgapfoo') == 'cgapfoo'
        assert short_env_name('fourfront-anything') == 'anything'

        with local_attrs(EnvUtils, FULL_ENV_PREFIX='cgap-'):  # Of course, we could have defined it otherwise.

            assert short_env_name(None) is None
            assert short_env_name('demo') == 'demo'
            assert short_env_name('anything') == 'anything'
            assert short_env_name('acme-anything') == 'acme-anything'
            assert short_env_name('cgap-anything') == 'anything'
            assert short_env_name('fourfront-cgapfoo') == 'fourfront-cgapfoo'
            assert short_env_name('fourfront-anything') == 'fourfront-anything'

    with local_attrs(EnvUtils, **FOURFRONT_SETTINGS_FOR_TESTING):

        assert short_env_name(None) is None
        assert short_env_name('demo') == 'demo'
        assert short_env_name('anything') == 'anything'
        assert short_env_name('acme-anything') == 'acme-anything'
        assert short_env_name('cgap-anything') == 'cgap-anything'
        assert short_env_name('fourfront-cgapfoo') == 'cgapfoo'
        assert short_env_name('fourfront-anything') == 'anything'


@using_orchestrated_behavior()
def test_orchestrated_full_env_name():

    assert full_env_name('cgap') == 'acme-prd'
    assert full_env_name('acme-foo') == 'acme-foo'
    assert full_env_name('foo') == 'acme-foo'

    with pytest.raises(Exception):
        full_env_name(None)

    with local_attrs(EnvUtils, **FOURFRONT_SETTINGS_FOR_TESTING):

        assert full_env_name('cgapdev') == 'fourfront-cgapdev'
        assert full_env_name('mastertest') == 'fourfront-mastertest'

        assert full_env_name('fourfront-cgapdev') == 'fourfront-cgapdev'
        assert full_env_name('fourfront-mastertest') == 'fourfront-mastertest'

        # Does not require a registered env
        assert full_env_name('foo') == 'fourfront-foo'
        assert full_env_name('cgapfoo') == 'fourfront-cgapfoo'

        # In legacy mode, these raise ValueError, but here we know we are in a cgap env, so just do the normal thing.
        # fourfront would be in its own orchestrated account.
        assert full_env_name('data') == 'fourfront-blue'
        assert full_env_name('staging') == 'fourfront-green'

        # In an orchestrated Fourfront, the name 'cgap' is not special, but coincidentally selects the same name.
        assert full_env_name('cgap') == 'fourfront-cgap'

    with local_attrs(EnvUtils, **CGAP_SETTINGS_FOR_TESTING):

        assert full_env_name('cgapdev') == 'fourfront-cgapdev'
        assert full_env_name('mastertest') == 'fourfront-mastertest'

        assert full_env_name('fourfront-cgapdev') == 'fourfront-cgapdev'
        assert full_env_name('fourfront-mastertest') == 'fourfront-mastertest'

        # Does not require a registered env
        assert full_env_name('foo') == 'fourfront-foo'
        assert full_env_name('cgapfoo') == 'fourfront-cgapfoo'

        # In an orchestrated CGAP, the names 'data' and 'staging' are not special
        assert full_env_name('data') == 'fourfront-data'
        assert full_env_name('staging') == 'fourfront-staging'

        # The name 'cgap' is found in PUBLIC_URL_TABLE, but happens by coincidence to expand the obvious way.
        assert full_env_name('cgap') == 'fourfront-cgap'


@using_orchestrated_behavior()
def test_orchestrated_full_cgap_env_name_for_cgap():

    assert full_cgap_env_name('foo') == 'acme-foo'
    assert full_cgap_env_name('acme-foo') == 'acme-foo'
    assert full_cgap_env_name('cgap') == 'acme-prd'
    assert full_cgap_env_name('test') == 'acme-test'

    with pytest.raises(Exception):
        full_cgap_env_name(None)


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_full_cgap_env_name_for_fourfront():

    # Everything is just going to return errors if you try this in a Fourfront orchestration
    with pytest.raises(ValueError):
        full_cgap_env_name('foo')
    with pytest.raises(ValueError):
        full_cgap_env_name('acme-foo')
    with pytest.raises(ValueError):
        full_cgap_env_name('cgap')
    with pytest.raises(ValueError):
        full_cgap_env_name('test')


@using_orchestrated_behavior()
def test_orchestrated_full_cgap_env_name_for_simulated_legacy_cgap():

    with local_attrs(EnvUtils, **CGAP_SETTINGS_FOR_TESTING):
        assert full_cgap_env_name('cgap') == 'fourfront-cgap'
        assert full_cgap_env_name('cgapdev') == 'fourfront-cgapdev'
        assert full_cgap_env_name('fourfront-cgapdev') == 'fourfront-cgapdev'

        # Does not require a registered env
        assert full_cgap_env_name('cgapfoo') == 'fourfront-cgapfoo'

        # This was an error in legacy CGAP because we couldn't tell if it was fourfront we were talking about.
        # In an orchestrated version, this name is available for use.
        assert full_cgap_env_name('mastertest') == 'fourfront-mastertest'


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_full_fourfront_env_name_for_fourfront():

    assert full_fourfront_env_name('foo') == 'acme-foo'
    assert full_fourfront_env_name('acme-foo') == 'acme-foo'
    assert full_fourfront_env_name('cgap') == 'acme-cgap'  # cgap is just an ordinary name in a fourfront orchestration
    assert full_fourfront_env_name('test') == 'acme-pubtest'

    with pytest.raises(Exception):
        full_fourfront_env_name(None)


@using_orchestrated_behavior()
def test_orchestrated_full_fourfront_env_name_for_cgap():

    # Everything is just going to return errors if you try this in a Fourfront orchestration
    with pytest.raises(ValueError):
        full_fourfront_env_name('foo')
    with pytest.raises(ValueError):
        full_fourfront_env_name('acme-foo')
    with pytest.raises(ValueError):
        full_fourfront_env_name('cgap')
    with pytest.raises(ValueError):
        full_fourfront_env_name('test')


@using_orchestrated_behavior()
def test_orchestrated_classify_server_url_localhost():

    assert classify_server_url("http://localhost/foo/bar") == {
        'kind': 'localhost',
        'environment': 'unknown',
        'bucket_env': 'unknown',
        'server_env': 'unknown',
        'is_stg_or_prd': False,
        'public_name': None,
    }

    assert classify_server_url("http://localhost:8000/foo/bar") == {
        'kind': 'localhost',
        'environment': 'unknown',
        'bucket_env': 'unknown',
        'server_env': 'unknown',
        'is_stg_or_prd': False,
        'public_name': None,
    }

    assert classify_server_url("http://localhost:1234/foo/bar") == {
        'kind': 'localhost',
        'environment': 'unknown',
        'bucket_env': 'unknown',
        'server_env': 'unknown',
        'is_stg_or_prd': False,
        'public_name': None,
    }

    assert classify_server_url("http://127.0.0.1:8000/foo/bar") == {
        'kind': 'localhost',
        'environment': 'unknown',
        'bucket_env': 'unknown',
        'server_env': 'unknown',
        'is_stg_or_prd': False,
        'public_name': None,
    }


@using_orchestrated_behavior()
def test_orchestrated_classify_server_url_cgap():

    with local_attrs(EnvUtils, **CGAP_SETTINGS_FOR_TESTING):

        assert classify_server_url("https://cgap.hms.harvard.edu/foo/bar") == {
            'kind': 'cgap',
            'environment': 'fourfront-cgap',
            'bucket_env': 'fourfront-cgap',
            'server_env': 'fourfront-cgap',
            'is_stg_or_prd': True,
            'public_name': 'cgap',
        }

        for env in ['cgapdev', 'cgapwolf']:
            url = f"http://{EnvUtils.FULL_ENV_PREFIX}{env}{EnvUtils.DEV_SUFFIX_FOR_TESTING}/foo/bar"
            assert classify_server_url(url) == {
                'kind': 'cgap',
                'environment': f"fourfront-{env}",
                'bucket_env': f"fourfront-{env}",
                'server_env': f"fourfront-{env}",
                'is_stg_or_prd': False,
                'public_name': None,
            }


@using_orchestrated_behavior()
def test_orchestrated_classify_server_url_fourfront():

    with stage_mirroring(enabled=True):

        with local_attrs(EnvUtils, **FOURFRONT_SETTINGS_FOR_TESTING):

            assert classify_server_url("https://data.4dnucleome.org/foo/bar") == {
                'kind': 'fourfront',
                'environment': 'fourfront-webprod',
                'bucket_env': 'fourfront-webprod',
                'server_env': 'fourfront-blue',
                'is_stg_or_prd': True,
                'public_name': 'data',
            }

            assert classify_server_url("https://staging.4dnucleome.org/foo/bar") == {
                'kind': 'fourfront',
                'environment': 'fourfront-webprod',
                'bucket_env': 'fourfront-webprod',
                'server_env': 'fourfront-green',
                'is_stg_or_prd': True,
                'public_name': 'staging',
            }

            assert classify_server_url(f"http://fourfront-blue{EnvUtils.DEV_SUFFIX_FOR_TESTING}/foo/bar") == {
                'kind': 'fourfront',
                'environment': 'fourfront-webprod',
                'bucket_env': 'fourfront-webprod',
                'server_env': 'fourfront-blue',
                'is_stg_or_prd': True,
                'public_name': 'data',
            }

            assert classify_server_url(f"http://fourfront-green{EnvUtils.DEV_SUFFIX_FOR_TESTING}/foo/bar") == {
                'kind': 'fourfront',
                'environment': 'fourfront-webprod',
                'bucket_env': 'fourfront-webprod',
                'server_env': 'fourfront-green',
                'is_stg_or_prd': True,
                'public_name': 'staging',
            }

            assert classify_server_url(f"http://fourfront-mastertest{EnvUtils.DEV_SUFFIX_FOR_TESTING}/foo/bar") == {
                'kind': 'fourfront',
                'environment': 'fourfront-mastertest',
                'bucket_env': 'fourfront-mastertest',
                'server_env': 'fourfront-mastertest',
                'is_stg_or_prd': False,
                'public_name': None,
            }


@using_orchestrated_behavior()
def test_orchestrated_classify_server_url_other():

    with raises_regexp(RuntimeError, "not a cgap server"):
        classify_server_url("http://google.com")  # raise_error=True is the default

    with raises_regexp(RuntimeError, "not a cgap server"):
        classify_server_url("http://google.com", raise_error=True)

    assert classify_server_url("http://google.com", raise_error=False) == {
        c.KIND: 'unknown',
        c.ENVIRONMENT: 'unknown',
        c.BUCKET_ENV: 'unknown',
        c.SERVER_ENV: 'unknown',
        c.IS_STG_OR_PRD: False,
        c.PUBLIC_NAME: None,
    }


# The function make_env_name_cfn_compatible has been removed because I think no one uses it. -kmp 15-May-2022
#
# @using_orchestrated_behavior()
# @pytest.mark.parametrize('env_name, cfn_id', [
#     ('acme-foo', 'acmefoo'),
#     ('foo-bar-baz', 'foobarbaz'),
#     ('cgap-mastertest', 'cgapmastertest'),
#     ('fourfront-cgap', 'fourfrontcgap'),
#     ('cgap-msa', 'cgapmsa'),
#     ('fourfrontmastertest', 'fourfrontmastertest')
# ])
# def test_orchestrated_make_env_name_cfn_compatible(env_name, cfn_id):
#     assert make_env_name_cfn_compatible(env_name) == cfn_id


@using_orchestrated_behavior()
def test_get_foursight_bucket():

    bucket_table = EnvUtils.FOURSIGHT_BUCKET_TABLE
    # Uncomment the following line to see the table we're working with.
    print(f"Testing get_foursight_bucket relative to: {json.dumps(bucket_table, indent=2)}")
    ignorable(json, bucket_table)  # Keeps lint tools from complaining when the above line is commented out.

    bucket = get_foursight_bucket(envname='acme-prd', stage='dev')
    assert bucket == 'acme-foursight-dev-prd'

    bucket = get_foursight_bucket(envname='acme-prd', stage='prod')
    assert bucket == 'acme-foursight-prod-prd'

    bucket = get_foursight_bucket(envname='acme-stg', stage='dev')
    assert bucket == 'acme-foursight-dev-stg'

    bucket = get_foursight_bucket(envname='acme-stg', stage='prod')
    assert bucket == 'acme-foursight-prod-stg'

    bucket = get_foursight_bucket(envname='acme-foo', stage='dev')
    assert bucket == 'acme-foursight-dev-other'

    bucket = get_foursight_bucket(envname='acme-foo', stage='prod')
    assert bucket == 'acme-foursight-prod-other'

    with local_attrs(EnvUtils, FOURSIGHT_BUCKET_TABLE="not-a-dict"):

        with EnvUtils.local_env_utils_for_testing():
            EnvUtils.FOURSIGHT_BUCKET_PREFIX = 'alpha-omega'
            EnvUtils.FOURSIGHT_BUCKET_TABLE = None
            EnvUtils.FULL_ENV_PREFIX = 'acme-'

            assert full_env_name(envname='acme-foo') == 'acme-foo'
            assert short_env_name(envname='acme-foo') == 'foo'

            assert full_env_name(envname='acme-stg') == 'acme-stg'
            assert short_env_name(envname='acme-stg') == 'stg'

            assert infer_foursight_from_env(envname='acme-foo') == 'foo'
            assert infer_foursight_from_env(envname='acme-stg') == 'stg'

            assert get_foursight_bucket(envname='acme-foo', stage='prod') == 'alpha-omega-prod-acme-foo'
            assert get_foursight_bucket(envname='acme-stg', stage='dev') == 'alpha-omega-dev-acme-stg'

            assert get_foursight_bucket(envname='acme-foo', stage='prod') == 'alpha-omega-prod-acme-foo'

            EnvUtils.FOURSIGHT_BUCKET_TABLE = None
            assert get_foursight_bucket(envname='acme-foo', stage='prod') == 'alpha-omega-prod-acme-foo'

            EnvUtils.FOURSIGHT_BUCKET_TABLE = {}
            assert get_foursight_bucket(envname='acme-foo', stage='prod') == 'alpha-omega-prod-acme-foo'


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_ff_get_env_real_url():

    # ===== Explicitly Defined URLs =====

    # By special name
    assert get_env_real_url('data') == 'https://genetics.example.com'
    assert get_env_real_url('staging') == 'https://stg.genetics.example.com'
    assert get_env_real_url('test') == 'https://testing.genetics.example.com'

    # By environment long name
    assert get_env_real_url('acme-prd') == 'https://genetics.example.com'
    assert get_env_real_url('acme-stg') == 'https://stg.genetics.example.com'
    assert get_env_real_url('acme-pubtest') == 'https://testing.genetics.example.com'

    # By environment short name
    assert get_env_real_url('prd') == 'https://genetics.example.com'
    assert get_env_real_url('stg') == 'https://stg.genetics.example.com'
    assert get_env_real_url('pubtest') == 'https://testing.genetics.example.com'

    # ==== Other URLs are built from DEV_SUFFIX =====

    dev_suffix = EnvUtils.DEV_ENV_DOMAIN_SUFFIX
    for env in ['acme-mastertest', 'acme-foo', "testing",
                # It doesn't work to add 'acme-' to the front of the special names
                'acme-data', 'acme-staging', 'acme-test']:
        # Note use of 'http' because Forfront prefers that.
        assert get_env_real_url(env) == f'http://{short_env_name(env)}{dev_suffix}'


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_CGAP_TESTING)
def test_cgap_get_env_real_url():

    # ===== Explicitly Defined URLs =====

    # By special name
    assert get_env_real_url('cgap') == 'https://cgap.genetics.example.com'
    assert get_env_real_url('stg') == 'https://staging.genetics.example.com'
    assert get_env_real_url('testing') == 'https://testing.genetics.example.com'

    # By environment long name
    assert get_env_real_url('acme-prd') == 'https://cgap.genetics.example.com'
    assert get_env_real_url('acme-stg') == 'https://staging.genetics.example.com'
    assert get_env_real_url('acme-pubtest') == 'https://testing.genetics.example.com'

    # By environment short name
    assert get_env_real_url('prd') == 'https://cgap.genetics.example.com'
    assert get_env_real_url('stg') == 'https://staging.genetics.example.com'
    assert get_env_real_url('pubtest') == 'https://testing.genetics.example.com'

    # ==== Other URLs are built from DEV_SUFFIX =====

    # These are not wired in, and end up defaulting Fourfront-style
    dev_suffix = EnvUtils.DEV_ENV_DOMAIN_SUFFIX
    for env in ['acme-mastertest', 'acme-foo', 'staging', 'test',
                # It doesn't work to add 'acme-' to the front of the special names, except 'stg' is actually also
                # a short name of an environment in this example, and so it does work to do that.
                # (We tested that above.)
                'acme-cgap', 'acme-testing']:
        # Note:
        #  * Uses 'https' uniformly for security reasons.
        #  * Uses full env name.
        assert get_env_real_url(env) == f'https://{env}{dev_suffix}'


def test_app_case():

    with local_attrs(EnvUtils, ORCHESTRATED_APP=APP_CGAP):
        assert EnvUtils.app_case(if_cgap='foo', if_fourfront='bar') == 'foo'

    with local_attrs(EnvUtils, ORCHESTRATED_APP=APP_FOURFRONT):
        assert EnvUtils.app_case(if_cgap='foo', if_fourfront='bar') == 'bar'

    with local_attrs(EnvUtils, ORCHESTRATED_APP='whatever'):
        with pytest.raises(ValueError):
            EnvUtils.app_case(if_cgap='foo', if_fourfront='bar')


def test_app_name():

    assert EnvUtils.app_name() == EnvUtils.ORCHESTRATED_APP

    with local_attrs(EnvUtils, ORCHESTRATED_APP=APP_CGAP):
        assert EnvUtils.app_name() == APP_CGAP

    with local_attrs(EnvUtils, ORCHESTRATED_APP=APP_FOURFRONT):
        assert EnvUtils.app_name() == APP_FOURFRONT

    with local_attrs(EnvUtils, ORCHESTRATED_APP='whatever'):
        assert EnvUtils.app_name() == 'whatever'


def test_get_config_ecosystem_from_s3():

    with mock.patch.object(EnvUtils, "_get_config_object_from_s3") as mock_get_config_object_from_s3:

        main_ecosystem = {"ecosystem": "blue"}

        blue_ecosystem = {"_ecosystem_name": "blue"}

        green_ecosystem = {"_ecosystem_name": "green"}

        cgap_foo = {
            "ff_env": "cgap-foo",
            "es": "http://es.etc",
            "fourfront": "http://fourfront.etc",
            "ecosystem": "main"
        }

        cgap_bar = {
            "ff_env": "cgap-bar",
            "es": "http://es.etc",
            "fourfront": "http://fourfront.etc"
        }

        bucket_for_testing = 'bucket-for-testing'

        cgap_ping = {
            "ff_env": "cgap-ping",
            "es": "http://es.etc",
            "fourfront": "http://fourfront.etc",
            "ecosystem": "ping"
        }

        cgap_pong = {
            "ff_env": "cgap-pong",
            "es": "http://es.etc",
            "fourfront": "http://fourfront.etc",
            "ecosystem": "pong"
        }

        ping_ecosystem = {"ecosystem": "pong"}
        pong_ecosystem = {"ecosystem": "ping"}

        circular_testing_bucket = 'circular-testing-bucket'

        envs = {
            bucket_for_testing: {
                "cgap-foo": cgap_foo,
                "cgap-bar": cgap_bar,
                "main.ecosystem": main_ecosystem,
                "blue.ecosystem": blue_ecosystem,
                "green.ecosystem": green_ecosystem
            },
            circular_testing_bucket: {
                "cgap-ping": cgap_ping,
                "cgap-pong": cgap_pong,
                "ping.ecosystem": ping_ecosystem,
                "pong.ecosystem": pong_ecosystem
            }
        }

        def mocked_get_config_object_from_s3(env_bucket, config_key):
            try:
                return envs[env_bucket][config_key]
            except Exception as e:
                # Typically an error raised due to boto3 S3 issues will end up getting caught and repackaged this way:
                raise EnvUtilsLoadError("Mocked bucket/key lookup failed.",
                                        env_bucket=env_bucket, config_key=config_key,
                                        encapsulated_error=e)

        mock_get_config_object_from_s3.side_effect = mocked_get_config_object_from_s3

        expected = cgap_bar
        actual = EnvUtils._get_config_ecosystem_from_s3(env_bucket=bucket_for_testing, config_key='cgap-bar')
        assert actual == expected

        expected = blue_ecosystem
        actual = EnvUtils._get_config_ecosystem_from_s3(env_bucket=bucket_for_testing, config_key='cgap-foo')
        assert actual == expected

        expected = blue_ecosystem
        actual = EnvUtils._get_config_ecosystem_from_s3(env_bucket=bucket_for_testing, config_key='main.ecosystem')
        assert actual == expected

        expected = blue_ecosystem
        actual = EnvUtils._get_config_ecosystem_from_s3(env_bucket=bucket_for_testing, config_key='blue.ecosystem')
        assert actual == expected

        expected = green_ecosystem
        actual = EnvUtils._get_config_ecosystem_from_s3(env_bucket=bucket_for_testing, config_key='green.ecosystem')
        assert actual == expected

        with pytest.raises(EnvUtilsLoadError):
            EnvUtils._get_config_ecosystem_from_s3(env_bucket=bucket_for_testing, config_key='missing')

        # Remaining tests test circularity.
        # We just stop at the point of being pointed back to something we've seen before.

        expected = pong_ecosystem
        actual = EnvUtils._get_config_ecosystem_from_s3(env_bucket=circular_testing_bucket, config_key='cgap-ping')
        assert actual == expected

        expected = ping_ecosystem
        actual = EnvUtils._get_config_ecosystem_from_s3(env_bucket=circular_testing_bucket, config_key='cgap-pong')
        assert actual == expected

        expected = pong_ecosystem
        actual = EnvUtils._get_config_ecosystem_from_s3(env_bucket=circular_testing_bucket, config_key='ping.ecosystem')
        assert actual == expected

        expected = ping_ecosystem
        actual = EnvUtils._get_config_ecosystem_from_s3(env_bucket=circular_testing_bucket, config_key='pong.ecosystem')
        assert actual == expected


def test_make_no_legacy():

    def foo(a, b, *, c):
        return [a, b, c]

    foo_prime = _make_no_legacy(foo, 'foo')

    with pytest.raises(NotImplementedError) as exc:
        foo_prime(3, 4, c=7)
    assert str(exc.value) == ("There is only an orchestrated version of foo, not a legacy version."
                              " args=(3, 4) kwargs={'c': 7}")


def test_set_declared_data_legacy():
    with local_attrs(LegacyController, LEGACY_DISPATCH_ENABLED=False):
        with pytest.raises(LegacyDispatchDisabled) as exc:
            EnvUtils.set_declared_data({'is_legacy': True})
        assert str(exc.value) == ('Attempt to use legacy operation set_declared_data'
                                  ' with args=None kwargs=None mode=load-env.')


def test_if_orchestrated_various_legacy_errors():

    def foo(x):
        return ['foo', x]

    def bar(x):
        return ['bar', x]

    def baz(x):
        if x == 99:
            raise UseLegacy()
        return ['baz', x]

    with local_attrs(LegacyController, LEGACY_DISPATCH_ENABLED=False):
        with pytest.raises(LegacyDispatchDisabled) as exc:
            if_orchestrated(use_legacy=True)(foo)
        # This error message could be better. The args aren't really involved. But it gets its point across
        # and anyway it should never happen. We're testing it just for coverage's sake. -kmp 25-Sep-2022
        assert str(exc.value) == "Attempt to use legacy operation foo with args=None kwargs=None mode=decorate."

    with local_attrs(LegacyController, LEGACY_DISPATCH_ENABLED=True):
        foo_prime = if_orchestrated(use_legacy=True)(foo)
        bar_prime = if_orchestrated(unimplemented=True)(bar)
        baz_prime = if_orchestrated(assumes_cgap=True)(baz)

        with local_attrs(LegacyController, LEGACY_DISPATCH_ENABLED=False):
            with pytest.raises(NotImplementedError) as exc:
                foo_prime(3)
            assert str(exc.value) == ("There is only an orchestrated version of foo,"
                                      " not a legacy version. args=(3,) kwargs={}")

            with pytest.raises(NotImplementedError) as exc:
                bar_prime(3)
            assert str(exc.value) == "Unimplemented: test.test_env_utils_orchestrated.bar"

            with local_attrs(EnvUtils, ORCHESTRATED_APP='cgap'):
                assert baz_prime(3) == ['baz', 3]
                with pytest.raises(LegacyDispatchDisabled) as exc:
                    baz_prime(99)
                assert str(exc.value) == "Attempt to use legacy operation baz with args=(99,) kwargs={} mode=raised."
                with local_attrs(LegacyController, LEGACY_DISPATCH_ENABLED=True):
                    with pytest.raises(NotImplementedError) as exc:
                        assert baz_prime(3) == ['baz', 3]
                        baz_prime(99)  # This will try to use the legacy version, which is enabled but doesn't exist.
                    assert str(exc.value) == ("There is only an orchestrated version of baz,"
                                              " not a legacy version. args=(99,) kwargs={}")
            with local_attrs(EnvUtils, ORCHESTRATED_APP='fourfront'):
                with pytest.raises(NotImplementedError) as exc:
                    baz_prime(3)
                assert 'Non-cgap applications are not supported.' in str(exc.value)

            with pytest.raises(LegacyDispatchDisabled) as exc:
                if_orchestrated(use_legacy=True)(legacy_blue_green_mirror_env)
            assert str(exc.value) == ("Attempt to use legacy operation blue_green_mirror_env"
                                      " with args=None kwargs=None mode=decorate.")

        bg = if_orchestrated()(legacy_blue_green_mirror_env)
        with local_attrs(EnvUtils, IS_LEGACY=True):
            assert bg('acme-green') == 'acme-blue'
            with local_attrs(LegacyController, LEGACY_DISPATCH_ENABLED=False):
                with pytest.raises(LegacyDispatchDisabled) as exc:
                    assert bg('acme-green') == 'acme-blue'
                assert str(exc.value) == ("Attempt to use legacy operation blue_green_mirror_env"
                                          " with args=('acme-green',) kwargs={} mode=dispatch.")


@using_orchestrated_behavior
def test_env_equals():
    assert env_equals('foobar', 'foobar');
    assert env_equals('acme-prd', 'cgap');
    assert env_equals('cgap', 'acme-prd');
    assert not env_equals('cgap', 'foobar');
    assert not env_equals('foobar', 'cgap');
