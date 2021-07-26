import functools
import os
import pytest

from dcicutils import env_utils
from dcicutils.env_utils import (
    is_stg_or_prd_env, is_cgap_env, is_fourfront_env, blue_green_mirror_env,
    get_mirror_env_from_context, is_test_env, is_hotseat_env, guess_mirror_env, get_standard_mirror_env,
    prod_bucket_env, public_url_mappings,
    infer_repo_from_env, data_set_for_env, get_bucket_env, infer_foursight_from_env,
    is_indexer_env, indexer_env_for_env, classify_server_url,
    full_env_name, full_cgap_env_name, full_fourfront_env_name, is_cgap_server, is_fourfront_server,
    make_env_name_cfn_compatible,
    # New support
    EnvUtils, p, c,
)
from dcicutils.misc_utils import decorator, local_attrs
from dcicutils.qa_utils import raises_regexp
from unittest import mock
from urllib.parse import urlparse


@decorator()
def using_orchestrated_behavior(data=None):
    def _decorate(fn):

        @functools.wraps(fn)
        def _wrapped(*args, **kwargs):
            old_data = EnvUtils.declared_data()
            try:
                EnvUtils.set_declared_data(data or EnvUtils.SAMPLE_TEMPLATE_FOR_CGAP_TESTING)
                return fn(*args, **kwargs)
            finally:
                EnvUtils.set_declared_data(old_data)

        return _wrapped

    return _decorate


@using_orchestrated_behavior()
def test_orchestrated_get_bucket_env():

    assert EnvUtils.PRD_ENV_NAME == 'acme-prd'
    assert EnvUtils.PRD_BUCKET == 'production-data'
    assert get_bucket_env(EnvUtils.PRD_ENV_NAME) == EnvUtils.PRD_BUCKET
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

        test_the_usual_scenario()  # The STG_ENV_NAME is ignored if STAGE_MIRRORING_ENABLED is False

        with local_attrs(env_utils, STAGE_MIRRORING_ENABLED=True):

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


@using_orchestrated_behavior()
def test_orchestrated_prod_bucket_env():

    assert EnvUtils.PRD_ENV_NAME == 'acme-prd'
    assert EnvUtils.PRD_BUCKET == 'production-data'
    assert prod_bucket_env(EnvUtils.PRD_ENV_NAME) == EnvUtils.PRD_BUCKET
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

        test_the_usual_scenario()  # The STG_ENV_NAME is ignored if STAGE_MIRRORING_ENABLED is False

        with local_attrs(env_utils, STAGE_MIRRORING_ENABLED=True):

            assert EnvUtils.STG_ENV_NAME == 'acme-stg'

            assert prod_bucket_env('acme-prd') == 'production-data'      # PRD_ENV_NAME
            assert prod_bucket_env('cgap') == 'production-data'          # in PUBLIC_URL_TABLE

            assert prod_bucket_env('acme-wolf') is None                  # normal env, just returns None
            assert prod_bucket_env('acme-foo') is None                   # normal env, just returns None
            assert prod_bucket_env('foo') is None                        # normal env, just returns None

            assert prod_bucket_env('acme-stg') == 'production-data'      # WIT mirroring enabled, this uses prod bucket
            assert prod_bucket_env('stg') == 'production-data'           # WITH mirroring enabled, this uses prod bucket


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

        # Setting EnvUtils.STG_ENV_NAME doesn't work unless STAGE_MIRRORING_ENABLED is enabled at top-level.
        assert data_set_for_env('acme-stg') is None
        assert data_set_for_env('stg') is None
        assert data_set_for_env('acme-stg', 'test') == 'test'
        assert data_set_for_env('stg', 'test') == 'test'

        with local_attrs(env_utils, STAGE_MIRRORING_ENABLED=True):

            assert data_set_for_env('acme-stg') == 'prod'
            assert data_set_for_env('stg') == 'prod'
            assert data_set_for_env('acme-stg', 'test') == 'prod'
            assert data_set_for_env('stg', 'test') == 'prod'


@using_orchestrated_behavior()
def test_orchestrated_public_url_mappings():

    sample_table_for_testing = EnvUtils.PUBLIC_URL_TABLE

    # This "test" is to show you what's there. Note that the URL doesn't have to have 'cgap' in its name.
    # For that matter, the key name doesn't have to be cgap either. But it should be PRD_ENV_NAME
    # or something in PUBLIC_URL_TABLE.
    public_name_1 = 'cgap'
    public_url_1 = 'https://genetics.example.com'
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
def test_orchestrated_is_cgap_server_for_cgap():

    assert is_cgap_server("localhost") is False
    assert is_cgap_server("localhost", allow_localhost=True) is True

    assert is_cgap_server("http://localhost") is False
    assert is_cgap_server("http://localhost", allow_localhost=True) is True

    assert is_cgap_server("https://localhost") is False
    assert is_cgap_server("https://localhost", allow_localhost=True) is True

    assert is_cgap_server("127.0.0.1") is False
    assert is_cgap_server("127.0.0.1", allow_localhost=True) is True

    assert is_cgap_server("http://127.0.0.1") is False
    assert is_cgap_server("http://127.0.0.1", allow_localhost=True) is True

    assert is_cgap_server("https://127.0.0.1") is False
    assert is_cgap_server("https://127.0.0.1", allow_localhost=True) is True

    assert is_cgap_server("genetics.example.com") is True

    assert is_cgap_server("https://genetics.example.com") is True

    assert is_cgap_server("http://genetics.example.com") is True
    assert is_cgap_server("http://genetics.example.com/") is True
    assert is_cgap_server("http://genetics.example.com/me") is True

    assert is_cgap_server("https://genetics.example.com") is True
    assert is_cgap_server("https://genetics.example.com/") is True
    assert is_cgap_server("https://genetics.example.com/me") is True

    assert is_cgap_server("example.com") is False
    assert is_cgap_server("https://example.com") is False

    with pytest.raises(ValueError):
        is_cgap_server(None)

    assert is_cgap_server("data.4dnucleome.org") is False             # Fourfront needs a separate orchestration
    assert is_cgap_server("http://data.4dnucleome.org") is False      # ditto
    assert is_cgap_server("https://data.4dnucleome.org") is False     # ditto

    assert is_cgap_server("https://staging.4dnucleome.org") is False  # ditto
    assert is_cgap_server("https://staging.4dnucleome.org") is False  # ditto
    assert is_cgap_server("https://staging.4dnucleome.org") is False  # ditto

    assert is_cgap_server("cgap.hms.harvard.edu") is False

    assert EnvUtils.DEV_ENV_DOMAIN_SUFFIX == ".abc123def456ghi789.us-east-1.rds.amazonaws.com"

    # An environment plus the suffix we require is the easy case here.
    assert is_cgap_server("acme-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is True

    # Extra middle components are allowed as we've presently implemented it.
    assert is_cgap_server("acme-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is True

    # Not all of the suffix is present here, so this will fail.
    assert is_cgap_server("acme-foo.us-east-1.rds.amazonaws.com") is False

    # Of course a just-plain-wrong suffix will fail.
    assert is_cgap_server("acme-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False  # wrong suffix

    # We're requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

    assert is_cgap_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_cgap_server("acme-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_cgap_server("acme-foo.us-west-1.rds.amazonaws.com") is False
    assert is_cgap_server("acme-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

    # Matching on an environment name requires the prefix (here we've declared "acme-")

    assert is_cgap_server("blah-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
    assert is_cgap_server("blah-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
    assert is_cgap_server("blah-foo.us-east-1.rds.amazonaws.com") is False
    assert is_cgap_server("blah-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

    # Also, we're again requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

    assert is_cgap_server("blah-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_cgap_server("blah-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_cgap_server("blah-foo.us-west-1.rds.amazonaws.com") is False
    assert is_cgap_server("blah-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

    with local_attrs(EnvUtils, DEV_ENV_DOMAIN_SUFFIX=".us-east-1.rds.amazonaws.com"):

        assert is_cgap_server("acme-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is True
        assert is_cgap_server("acme-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is True
        assert is_cgap_server("acme-foo.us-east-1.rds.amazonaws.com") is True
        assert is_cgap_server("acme-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is True

        # We're requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

        assert is_cgap_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("acme-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("acme-foo.us-west-1.rds.amazonaws.com") is False

        # Matching on an environment name requires the prefix (here we've declared "acme-")

        assert is_cgap_server("blah-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

        # Also, we're again requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

        assert is_cgap_server("blah-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

    with local_attrs(EnvUtils, DEV_ENV_DOMAIN_SUFFIX=".rds.amazonaws.com"):

        assert is_cgap_server("acme-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is True
        assert is_cgap_server("acme-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is True
        assert is_cgap_server("acme-foo.us-east-1.rds.amazonaws.com") is True
        assert is_cgap_server("acme-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is True

        # We're only requiring .rds.amazon.com, so even .us-west-1... will match.

        assert is_cgap_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is True
        assert is_cgap_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is True
        assert is_cgap_server("acme-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is True
        assert is_cgap_server("acme-foo.us-west-1.rds.amazonaws.com") is True

        # Matching on an environment name requires the prefix (here we've declared "acme-")

        assert is_cgap_server("blah-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

        # Also, we're again requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

        assert is_cgap_server("blah-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

        assert is_cgap_server("www.google.com") is False

        # Make sure we don't recognize the non-orchestrated ones.
        # It might seem like the cgap one should succeed, but really we're trying not to recognize "anything cgap-like"
        # but anything in my own cgap ecosystem. These are outside.


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_is_cgap_server_for_fourfront():

    assert is_cgap_server("localhost") is False
    assert is_cgap_server("localhost", allow_localhost=True) is False

    assert is_cgap_server("http://localhost") is False
    assert is_cgap_server("http://localhost", allow_localhost=True) is False

    assert is_cgap_server("https://localhost") is False
    assert is_cgap_server("https://localhost", allow_localhost=True) is False

    assert is_cgap_server("127.0.0.1") is False
    assert is_cgap_server("127.0.0.1", allow_localhost=True) is False

    assert is_cgap_server("http://127.0.0.1") is False
    assert is_cgap_server("http://127.0.0.1", allow_localhost=True) is False

    assert is_cgap_server("https://127.0.0.1") is False
    assert is_cgap_server("https://127.0.0.1", allow_localhost=True) is False

    assert is_cgap_server("genetics.example.com") is False

    assert is_cgap_server("https://genetics.example.com") is False

    assert is_cgap_server("http://genetics.example.com") is False
    assert is_cgap_server("http://genetics.example.com/") is False
    assert is_cgap_server("http://genetics.example.com/me") is False

    assert is_cgap_server("https://genetics.example.com") is False
    assert is_cgap_server("https://genetics.example.com/") is False
    assert is_cgap_server("https://genetics.example.com/me") is False

    assert is_cgap_server("example.com") is False
    assert is_cgap_server("https://example.com") is False

    with pytest.raises(ValueError):
        is_cgap_server(None)

    assert is_cgap_server("data.4dnucleome.org") is False             # Legacy Fourfront is not the orchestrated one
    assert is_cgap_server("http://data.4dnucleome.org") is False      # ditto
    assert is_cgap_server("https://data.4dnucleome.org") is False     # ditto

    assert is_cgap_server("https://staging.4dnucleome.org") is False  # ditto
    assert is_cgap_server("https://staging.4dnucleome.org") is False  # ditto
    assert is_cgap_server("https://staging.4dnucleome.org") is False  # ditto

    # NOTE: This last is actually a bug, but included here for reference.
    #       We're matching 'cgap' in part1 of the hostname, which is no worse than what used to happen in legacy cgap.
    #       But the cgap it's matching is in another domain. We don't presently enforce a domain suffix.
    #       -kmp 24-Jul-2021
    assert is_cgap_server("cgap.hms.harvard.edu") is False  # TODO: Fix this bug. See explanation above.

    assert EnvUtils.DEV_ENV_DOMAIN_SUFFIX == ".abc123def456ghi789.us-east-1.rds.amazonaws.com"

    # An environment plus the suffix we require is the easy case here.
    assert is_cgap_server("acme-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False

    # Extra middle components are allowed as we've presently implemented it.
    assert is_cgap_server("acme-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False

    # Not all of the suffix is present here, so this will fail.
    assert is_cgap_server("acme-foo.us-east-1.rds.amazonaws.com") is False

    # Of course a just-plain-wrong suffix will fail.
    assert is_cgap_server("acme-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False  # wrong suffix

    # We're requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

    assert is_cgap_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_cgap_server("acme-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_cgap_server("acme-foo.us-west-1.rds.amazonaws.com") is False
    assert is_cgap_server("acme-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

    # Matching on an environment name requires the prefix (here we've declared "acme-")

    assert is_cgap_server("blah-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
    assert is_cgap_server("blah-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
    assert is_cgap_server("blah-foo.us-east-1.rds.amazonaws.com") is False
    assert is_cgap_server("blah-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

    # Also, we're again requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

    assert is_cgap_server("blah-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_cgap_server("blah-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_cgap_server("blah-foo.us-west-1.rds.amazonaws.com") is False
    assert is_cgap_server("blah-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

    with local_attrs(EnvUtils, DEV_ENV_DOMAIN_SUFFIX=".us-east-1.rds.amazonaws.com"):

        assert is_cgap_server("acme-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("acme-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("acme-foo.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("acme-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

        # We're requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

        assert is_cgap_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("acme-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("acme-foo.us-west-1.rds.amazonaws.com") is False

        # Matching on an environment name requires the prefix (here we've declared "acme-")

        assert is_cgap_server("blah-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

        # Also, we're again requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

        assert is_cgap_server("blah-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

    with local_attrs(EnvUtils, DEV_ENV_DOMAIN_SUFFIX=".rds.amazonaws.com"):

        assert is_cgap_server("acme-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("acme-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("acme-foo.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("acme-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

        # We're only requiring .rds.amazon.com, so even .us-west-1... will match.

        assert is_cgap_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("acme-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("acme-foo.us-west-1.rds.amazonaws.com") is False

        # Matching on an environment name requires the prefix (here we've declared "acme-")

        assert is_cgap_server("blah-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.us-east-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

        # Also, we're again requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

        assert is_cgap_server("blah-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.us-west-1.rds.amazonaws.com") is False
        assert is_cgap_server("blah-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

        assert is_cgap_server("www.google.com") is False

        # Make sure we don't recognize the non-orchestrated ones.
        # It might seem like the cgap one should succeed, but really we're trying not to recognize "anything cgap-like"
        # but anything in my own cgap ecosystem. These are outside.


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_is_fourfront_server_for_fourfront():

    assert is_fourfront_server("localhost") is False
    assert is_fourfront_server("localhost", allow_localhost=True) is True

    assert is_fourfront_server("http://localhost") is False
    assert is_fourfront_server("http://localhost", allow_localhost=True) is True

    assert is_fourfront_server("https://localhost") is False
    assert is_fourfront_server("https://localhost", allow_localhost=True) is True

    assert is_fourfront_server("127.0.0.1") is False
    assert is_fourfront_server("127.0.0.1", allow_localhost=True) is True

    assert is_fourfront_server("http://127.0.0.1") is False
    assert is_fourfront_server("http://127.0.0.1", allow_localhost=True) is True

    assert is_fourfront_server("https://127.0.0.1") is False
    assert is_fourfront_server("https://127.0.0.1", allow_localhost=True) is True

    assert is_fourfront_server("genetics.example.com") is True

    assert is_fourfront_server("https://genetics.example.com") is True

    assert is_fourfront_server("http://genetics.example.com") is True
    assert is_fourfront_server("http://genetics.example.com/") is True
    assert is_fourfront_server("http://genetics.example.com/me") is True

    assert is_fourfront_server("https://genetics.example.com") is True
    assert is_fourfront_server("https://genetics.example.com/") is True
    assert is_fourfront_server("https://genetics.example.com/me") is True

    assert is_fourfront_server("example.com") is False
    assert is_fourfront_server("https://example.com") is False

    with pytest.raises(ValueError):
        is_fourfront_server(None)

    assert is_fourfront_server("data.4dnucleome.org") is False             # Fourfront needs a separate orchestration
    assert is_fourfront_server("http://data.4dnucleome.org") is False      # ditto
    assert is_fourfront_server("https://data.4dnucleome.org") is False     # ditto

    # NOTE: These only "succeed" (returning False) because we don't have mirroring on.

    assert is_fourfront_server("https://staging.4dnucleome.org") is False  # ditto
    assert is_fourfront_server("https://staging.4dnucleome.org") is False  # ditto
    assert is_fourfront_server("https://staging.4dnucleome.org") is False  # ditto

    with local_attrs(EnvUtils, STG_ENV_NAME='acme-stg'):

        assert is_fourfront_server("https://staging.4dnucleome.org") is False  # ditto
        assert is_fourfront_server("https://staging.4dnucleome.org") is False  # ditto
        assert is_fourfront_server("https://staging.4dnucleome.org") is False  # ditto

        with local_attrs(env_utils, STAGE_MIRRORING_ENABLED=True):

            assert is_fourfront_server("https://staging.4dnucleome.org") is False
            assert is_fourfront_server("https://staging.4dnucleome.org") is False
            assert is_fourfront_server("https://staging.4dnucleome.org") is False

    assert is_fourfront_server("cgap.hms.harvard.edu") is False

    assert EnvUtils.DEV_ENV_DOMAIN_SUFFIX == ".abc123def456ghi789.us-east-1.rds.amazonaws.com"

    # An environment plus the suffix we require is the easy case here.
    assert is_fourfront_server("acme-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is True

    # Extra middle components are allowed as we've presently implemented it.
    assert is_fourfront_server("acme-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is True

    # Not all of the suffix is present here, so this will fail.
    assert is_fourfront_server("acme-foo.us-east-1.rds.amazonaws.com") is False

    # Of course a just-plain-wrong suffix will fail.
    assert is_fourfront_server("acme-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False  # wrong suffix

    # We're requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

    assert is_fourfront_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_fourfront_server("acme-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_fourfront_server("acme-foo.us-west-1.rds.amazonaws.com") is False
    assert is_fourfront_server("acme-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

    # Matching on an environment name requires the prefix (here we've declared "acme-")

    assert is_fourfront_server("blah-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
    assert is_fourfront_server("blah-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
    assert is_fourfront_server("blah-foo.us-east-1.rds.amazonaws.com") is False
    assert is_fourfront_server("blah-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

    # Also, we're again requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

    assert is_fourfront_server("blah-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_fourfront_server("blah-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_fourfront_server("blah-foo.us-west-1.rds.amazonaws.com") is False
    assert is_fourfront_server("blah-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

    with local_attrs(EnvUtils, DEV_ENV_DOMAIN_SUFFIX=".us-east-1.rds.amazonaws.com"):

        assert is_fourfront_server("acme-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is True
        assert is_fourfront_server("acme-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is True
        assert is_fourfront_server("acme-foo.us-east-1.rds.amazonaws.com") is True
        assert is_fourfront_server("acme-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is True

        # We're requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

        assert is_fourfront_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("acme-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("acme-foo.us-west-1.rds.amazonaws.com") is False

        # Matching on an environment name requires the prefix (here we've declared "acme-")

        assert is_fourfront_server("blah-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

        # Also, we're again requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

        assert is_fourfront_server("blah-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

    with local_attrs(EnvUtils, DEV_ENV_DOMAIN_SUFFIX=".rds.amazonaws.com"):

        assert is_fourfront_server("acme-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is True
        assert is_fourfront_server("acme-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is True
        assert is_fourfront_server("acme-foo.us-east-1.rds.amazonaws.com") is True
        assert is_fourfront_server("acme-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is True

        # We're only requiring .rds.amazon.com, so even .us-west-1... will match.

        assert is_fourfront_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is True
        assert is_fourfront_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is True
        assert is_fourfront_server("acme-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is True
        assert is_fourfront_server("acme-foo.us-west-1.rds.amazonaws.com") is True

        # Matching on an environment name requires the prefix (here we've declared "acme-")

        assert is_fourfront_server("blah-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

        # Also, we're again requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

        assert is_fourfront_server("blah-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

        assert is_fourfront_server("www.google.com") is False

        # Make sure we don't recognize the non-orchestrated ones.
        # It might seem like the cgap one should succeed, but really we're trying not to recognize "anything cgap-like"
        # but anything in my own cgap ecosystem. These are outside.


@using_orchestrated_behavior()
def test_orchestrated_is_fourfront_server_for_cgap():

    assert is_fourfront_server("localhost") is False
    assert is_fourfront_server("localhost", allow_localhost=True) is False

    assert is_fourfront_server("http://localhost") is False
    assert is_fourfront_server("http://localhost", allow_localhost=True) is False

    assert is_fourfront_server("https://localhost") is False
    assert is_fourfront_server("https://localhost", allow_localhost=True) is False

    assert is_fourfront_server("127.0.0.1") is False
    assert is_fourfront_server("127.0.0.1", allow_localhost=True) is False

    assert is_fourfront_server("http://127.0.0.1") is False
    assert is_fourfront_server("http://127.0.0.1", allow_localhost=True) is False

    assert is_fourfront_server("https://127.0.0.1") is False
    assert is_fourfront_server("https://127.0.0.1", allow_localhost=True) is False

    assert is_fourfront_server("genetics.example.com") is False

    assert is_fourfront_server("https://genetics.example.com") is False

    assert is_fourfront_server("http://genetics.example.com") is False
    assert is_fourfront_server("http://genetics.example.com/") is False
    assert is_fourfront_server("http://genetics.example.com/me") is False

    assert is_fourfront_server("https://genetics.example.com") is False
    assert is_fourfront_server("https://genetics.example.com/") is False
    assert is_fourfront_server("https://genetics.example.com/me") is False

    assert is_fourfront_server("example.com") is False
    assert is_fourfront_server("https://example.com") is False

    with pytest.raises(ValueError):
        is_fourfront_server(None)

    assert is_fourfront_server("data.4dnucleome.org") is False             # Fourfront needs a separate orchestration
    assert is_fourfront_server("http://data.4dnucleome.org") is False      # ditto
    assert is_fourfront_server("https://data.4dnucleome.org") is False     # ditto

    assert is_fourfront_server("https://staging.4dnucleome.org") is False  # ditto
    assert is_fourfront_server("https://staging.4dnucleome.org") is False  # ditto
    assert is_fourfront_server("https://staging.4dnucleome.org") is False  # ditto

    # NOTE: This last is actually a bug, but included here for reference.
    #       We're matching 'cgap' in part1 of the hostname, which is no worse than what used to happen in legacy cgap.
    #       But the cgap it's matching is in another domain. We don't presently enforce a domain suffix.
    #       -kmp 24-Jul-2021
    assert is_fourfront_server("cgap.hms.harvard.edu") is False  # TODO: Fix this bug. See explanation above.

    assert EnvUtils.DEV_ENV_DOMAIN_SUFFIX == ".abc123def456ghi789.us-east-1.rds.amazonaws.com"

    # An environment plus the suffix we require is the easy case here.
    assert is_fourfront_server("acme-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False

    # Extra middle components are allowed as we've presently implemented it.
    assert is_fourfront_server("acme-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False

    # Not all of the suffix is present here, so this will fail.
    assert is_fourfront_server("acme-foo.us-east-1.rds.amazonaws.com") is False

    # Of course a just-plain-wrong suffix will fail.
    assert is_fourfront_server("acme-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False  # wrong suffix

    # We're requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

    assert is_fourfront_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_fourfront_server("acme-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_fourfront_server("acme-foo.us-west-1.rds.amazonaws.com") is False
    assert is_fourfront_server("acme-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

    # Matching on an environment name requires the prefix (here we've declared "acme-")

    assert is_fourfront_server("blah-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
    assert is_fourfront_server("blah-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
    assert is_fourfront_server("blah-foo.us-east-1.rds.amazonaws.com") is False
    assert is_fourfront_server("blah-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

    # Also, we're again requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

    assert is_fourfront_server("blah-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_fourfront_server("blah-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
    assert is_fourfront_server("blah-foo.us-west-1.rds.amazonaws.com") is False
    assert is_fourfront_server("blah-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

    with local_attrs(EnvUtils, DEV_ENV_DOMAIN_SUFFIX=".us-east-1.rds.amazonaws.com"):

        assert is_fourfront_server("acme-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("acme-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("acme-foo.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("acme-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

        # We're requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

        assert is_fourfront_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("acme-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("acme-foo.us-west-1.rds.amazonaws.com") is False

        # Matching on an environment name requires the prefix (here we've declared "acme-")

        assert is_fourfront_server("blah-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

        # Also, we're again requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

        assert is_fourfront_server("blah-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

    with local_attrs(EnvUtils, DEV_ENV_DOMAIN_SUFFIX=".rds.amazonaws.com"):

        assert is_fourfront_server("acme-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("acme-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("acme-foo.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("acme-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

        # We're only requiring .rds.amazon.com, so even .us-west-1... will match.

        assert is_fourfront_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("acme-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("acme-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("acme-foo.us-west-1.rds.amazonaws.com") is False

        # Matching on an environment name requires the prefix (here we've declared "acme-")

        assert is_fourfront_server("blah-foo.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.something.abc123def456ghi789.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.us-east-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.xxx123xxx456xxx789.us-east-1.rds.amazonaws.com") is False

        # Also, we're again requiring .us-east-1..., and these are .us-west-1..., so they will all fail.

        assert is_fourfront_server("blah-foo.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.something.abc123def456ghi789.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.us-west-1.rds.amazonaws.com") is False
        assert is_fourfront_server("blah-foo.xxx123xxx456xxx789.us-west-1.rds.amazonaws.com") is False

        assert is_fourfront_server("www.google.com") is False

        # Make sure we don't recognize the non-orchestrated ones.
        # It might seem like the cgap one should succeed, but really we're trying not to recognize "anything cgap-like"
        # but anything in my own cgap ecosystem. These are outside.


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

        with local_attrs(env_utils, STAGE_MIRRORING_ENABLED=True):

            assert is_stg_or_prd_env('acme-stg') is True


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_is_stg_or_prd_env_for_fourfront():

    assert is_stg_or_prd_env('data') is True
    assert is_stg_or_prd_env('cgap') is False
    assert is_stg_or_prd_env('acme-prd') is True
    assert is_stg_or_prd_env('acme-test') is False
    assert is_stg_or_prd_env('anything') is False

    assert is_stg_or_prd_env('acme-stg') is False

    with local_attrs(EnvUtils, STG_ENV_NAME='acme-stg'):

        assert is_stg_or_prd_env('acme-stg') is False

        with local_attrs(env_utils, STAGE_MIRRORING_ENABLED=True):

            assert is_stg_or_prd_env('acme-stg') is True


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

    with local_attrs(env_utils, STAGE_MIRRORING_ENABLED=True):
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

    with local_attrs(env_utils, STAGE_MIRRORING_ENABLED=True):
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

    with local_attrs(env_utils, STAGE_MIRRORING_ENABLED=True):
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

    with local_attrs(env_utils, STAGE_MIRRORING_ENABLED=True):
        with local_attrs(EnvUtils, STG_ENV_NAME='acme-stg'):

            with mock.patch.object(os, "environ", {'ENV_NAME': 'acme-stg', "MIRROR_ENV_NAME": 'bar'}):
                settings = {'env.name': 'acme-prd'}
                mirror = get_mirror_env_from_context(settings, allow_environ=True)
                assert mirror == 'bar'  # mirror explicitly declared, ignoring env name


def _test_get_standard_mirror_env_for_cgap(mirror_getter):

    for mirroring_enabled in [True, False]:
        with local_attrs(EnvUtils, STG_ENV_NAME='acme-stg'):
            with local_attrs(env_utils, STAGE_MIRRORING_ENABLED=mirroring_enabled):

                def expected_result(value):
                    return value if mirroring_enabled else None

                assert mirror_getter('acme-prd') == expected_result('acme-stg')
                assert mirror_getter('acme-stg') == expected_result('acme-prd')

                assert mirror_getter('cgap') == expected_result('stg')
                assert mirror_getter('stg') == expected_result('cgap')

                assert mirror_getter('acme-foo') is None


@using_orchestrated_behavior()
def test_orchestrated_get_standard_mirror_env_for_cgap():
    _test_get_standard_mirror_env_for_cgap(get_standard_mirror_env)


@using_orchestrated_behavior()
def test_orchestrated_guess_mirror_env_for_cgap():
    _test_get_standard_mirror_env_for_cgap(guess_mirror_env)


def _test_get_standard_mirror_env_for_fourfront(mirror_getter):

    for mirroring_enabled in [True, False]:
        with local_attrs(EnvUtils, STG_ENV_NAME='acme-stg'):
            with local_attrs(env_utils, STAGE_MIRRORING_ENABLED=mirroring_enabled):

                def expected_result(value):
                    return value if mirroring_enabled else None

                assert mirror_getter('acme-prd') == expected_result('acme-stg')
                assert mirror_getter('acme-stg') == expected_result('acme-prd')

                assert mirror_getter('data') == expected_result('staging')
                assert mirror_getter('staging') == expected_result('data')

                assert mirror_getter('acme-foo') is None


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_get_standard_mirror_env_for_fourfront():
    _test_get_standard_mirror_env_for_fourfront(get_standard_mirror_env)


@using_orchestrated_behavior(data=EnvUtils.SAMPLE_TEMPLATE_FOR_FOURFRONT_TESTING)
def test_orchestrated_guess_mirror_env_for_fourfront():
    _test_get_standard_mirror_env_for_fourfront(guess_mirror_env)


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
    FULL_ENV_PREFIX='fourfront-',
    DEV_ENV_DOMAIN_SUFFIX=EnvUtils.DEV_SUFFIX_FOR_TESTING,
    PRD_BUCKET='fourfront-cgap',
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
    FULL_ENV_PREFIX='fourfront-',
    DEV_ENV_DOMAIN_SUFFIX=EnvUtils.DEV_SUFFIX_FOR_TESTING,
    PRD_BUCKET='fourfront-webprod',
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
def test_orchestrated_infer_foursight_env():

    dev_suffix = EnvUtils.DEV_SUFFIX_FOR_TESTING

    class MockedRequest:
        def __init__(self, domain):
            self.domain = domain

    def mock_request(domain):  # build a dummy request with the 'domain' member, checked in the method
        return MockedRequest(domain)

    assert infer_foursight_from_env(mock_request('acme-mastertest' + dev_suffix), 'acme-mastertest') == 'mastertest'
    assert infer_foursight_from_env(mock_request('acme-webdev' + dev_suffix), 'acme-webdev') == 'webdev'
    assert infer_foursight_from_env(mock_request('acme-hotseat' + dev_suffix), 'acme-hotseat') == 'hotseat'

    with local_attrs(env_utils, STAGE_MIRRORING_ENABLED=True):

        with local_attrs(EnvUtils, **FOURFRONT_SETTINGS_FOR_TESTING):

            # (active) fourfront testing environments
            assert infer_foursight_from_env(mock_request('fourfront-mastertest' + dev_suffix),
                                            'fourfront-mastertest') == 'mastertest'
            assert infer_foursight_from_env(mock_request('fourfront-webdev' + dev_suffix),
                                            'fourfront-webdev') == 'webdev'
            assert infer_foursight_from_env(mock_request('fourfront-hotseat' + dev_suffix),
                                            'fourfront-hotseat') == 'hotseat'

            # (active) fourfront production environments
            assert (infer_foursight_from_env(mock_request(domain='data.4dnucleome.org'), 'fourfront-blue')
                    == 'data')
            assert (infer_foursight_from_env(mock_request(domain='data.4dnucleome.org'), 'fourfront-green')
                    == 'data')
            assert (infer_foursight_from_env(mock_request(domain='staging.4dnucleome.org'), 'fourfront-blue')
                    == 'staging')
            assert (infer_foursight_from_env(mock_request(domain='staging.4dnucleome.org'), 'fourfront-green')
                    == 'staging')

            # These next four are pathological and hopefully not used, but they illustrate that the domain dominates.
            # This does not illustrate intended use.
            assert (infer_foursight_from_env(mock_request(domain='data.4dnucleome.org'), 'data')
                    == 'data')
            assert (infer_foursight_from_env(mock_request(domain='data.4dnucleome.org'), 'staging')
                    == 'data')

            assert (infer_foursight_from_env(mock_request(domain='staging.4dnucleome.org'), 'data')
                    == 'staging')
            assert (infer_foursight_from_env(mock_request(domain='staging.4dnucleome.org'), 'staging')
                    == 'staging')

        # (active) cgap environments
        with local_attrs(EnvUtils, **CGAP_SETTINGS_FOR_TESTING):

            assert infer_foursight_from_env(mock_request('fourfront-cgapdev' + dev_suffix),
                                            'fourfront-cgapdev') == 'cgapdev'
            assert infer_foursight_from_env(mock_request('fourfront-cgaptest' + dev_suffix),
                                            'fourfront-cgaptest') == 'cgaptest'
            assert infer_foursight_from_env(mock_request('fourfront-cgapwolf' + dev_suffix),
                                            'fourfront-cgapwolf') == 'cgapwolf'
            assert infer_foursight_from_env(mock_request('fourfront-cgap' + dev_suffix),
                                            'fourfront-cgap') == 'cgap'

            assert infer_foursight_from_env(mock_request('cgap.hms.harvard.edu'), 'fourfront-cgap') == 'cgap'
            assert infer_foursight_from_env(mock_request('cgap.hms.harvard.edu'), 'cgap') == 'cgap'


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
def test_orchestrated_is_indexer_env():

    assert EnvUtils.INDEXER_ENV_NAME == 'acme-indexer'

    # This should be true for the indexer env, False for others
    assert is_indexer_env('acme-indexer') is True

    assert is_indexer_env('acme-prd') is False
    assert is_indexer_env('acme-test') is False
    assert is_indexer_env('acme-foo') is False
    assert is_indexer_env('pretty-much-anything') is False


@using_orchestrated_behavior()
def test_orchestrated_full_env_name():

    assert full_env_name('cgap') == 'acme-prd'
    assert full_env_name('acme-foo') == 'acme-foo'
    assert full_env_name('foo') == 'acme-foo'

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


@using_orchestrated_behavior()
def test_orchestrated_full_cgap_env_name_for_cgap():

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

    with local_attrs(env_utils, STAGE_MIRRORING_ENABLED=True):

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


@using_orchestrated_behavior()
@pytest.mark.parametrize('env_name, cfn_id', [
    ('acme-foo', 'acmefoo'),
    ('foo-bar-baz', 'foobarbaz'),
    ('cgap-mastertest', 'cgapmastertest'),
    ('fourfront-cgap', 'fourfrontcgap'),
    ('cgap-msa', 'cgapmsa'),
    ('fourfrontmastertest', 'fourfrontmastertest')
])
def test_orchestrated_make_env_name_cfn_compatible(env_name, cfn_id):
    assert make_env_name_cfn_compatible(env_name) == cfn_id
