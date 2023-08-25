import botocore.client
import botocore.exceptions
import contextlib
import datetime
import hashlib
import io
import json
import os
import pytest
import re
import requests

from dcicutils import s3_utils as s3_utils_module
from dcicutils.beanstalk_utils import get_beanstalk_real_url
from dcicutils.beanstalk_utils import compute_ff_prd_env, compute_cgap_prd_env, compute_cgap_stg_env
from dcicutils.common import LEGACY_GLOBAL_ENV_BUCKET
from dcicutils.env_manager import EnvManager
from dcicutils.env_utils import (
    get_standard_mirror_env, EnvUtils, short_env_name,
    is_stg_or_prd_env, full_env_name,
)
from dcicutils.env_utils_legacy import (
    FF_PUBLIC_URL_STG, FF_PUBLIC_URL_PRD,
     _CGAP_MGB_PUBLIC_URL_PRD,  # noQA - Yes, we do want to import a protected member (for testing)
)
from dcicutils.exceptions import SynonymousEnvironmentVariablesMismatched, CannotInferEnvFromManyGlobalEnvs
from dcicutils.ff_mocks import make_mock_es_url, make_mock_portal_url, mocked_s3utils
from dcicutils.misc_utils import ignored, ignorable, override_environ, exported, file_contents
from dcicutils.qa_utils import MockBoto3, MockResponse, known_bug_expected, MockBotoS3Client, MockFileSystem
from dcicutils.s3_utils import s3Utils, HealthPageKey
from requests.exceptions import ConnectionError
from typing import Optional, Callable, Dict
from unittest import mock
from .helpers import (
    using_fresh_ff_state_for_testing, using_fresh_cgap_state_for_testing, using_fresh_ff_deployed_state_for_testing,
)
from .test_ff_utils import mocked_s3utils_with_sse


# This was moved to ff_mocks.py, but some things may still import from here.
# Importing from here is deprecated. It should be imported from ff_Mocks going forward.
# Minimally this exported marker should be retained until major version 7 of dcicutils,
# though as a practical matter because we have to import that context manager here for testing,
# it's unlikely to break even after we remove this marker. -kmp 10-Feb-2023
exported(mocked_s3utils_with_sse)


@contextlib.contextmanager
def mocked_s3_integration(integrated_names=None, zip_suffix="", ffenv=None):
    """
    This does common setup of some mocks needed by zip testing.
    """

    zip_path_key = "zip_path" + zip_suffix
    zip_filename_key = "zip_filename" + zip_suffix

    b3 = MockBoto3()

    if not ffenv:
        ffenv = integrated_names['ffenv'] if integrated_names else None

    with mock.patch.object(s3_utils_module, "boto3", b3):

        s3_connection = s3Utils(env=ffenv)

        if integrated_names is not None:

            # Not needed when mocked.
            # s3_connection.s3_delete_dir(prefix)

            # In our mock, this won't exist already on S3 like in the integrated version of this test,
            # so we have to preload to our mock S3 manually. -kmp 13-Jan-2021
            s3_connection.s3.upload_file(Filename=integrated_names[zip_path_key],
                                         Bucket=s3_connection.outfile_bucket,
                                         Key=integrated_names[zip_filename_key])

            s3_connection.s3.put_object(Bucket=s3_connection.outfile_bucket,
                                        Key=integrated_names['filename'],
                                        Body=str.encode('thisisatest'))

        yield s3_connection


@pytest.mark.unit
def test_s3utils_constants():

    # This is a bit concrete, as tests go, but at last it will let us know if something changes. -kmp 22-Aug-2021

    assert s3Utils.SYS_BUCKET_SUFFIX == "system"
    assert s3Utils.OUTFILE_BUCKET_SUFFIX == "wfoutput"
    assert s3Utils.RAW_BUCKET_SUFFIX == "files"
    assert s3Utils.BLOB_BUCKET_SUFFIX == "blobs"
    assert s3Utils.METADATA_BUCKET_SUFFIX == "metadata-bundles"
    assert s3Utils.TIBANNA_OUTPUT_BUCKET_SUFFIX == 'tibanna-output'

    assert s3Utils.EB_PREFIX == "elasticbeanstalk"
    assert s3Utils.EB_AND_ENV_PREFIX == "elasticbeanstalk-%s-"

    assert s3Utils.SYS_BUCKET_TEMPLATE == "elasticbeanstalk-%s-system"
    assert s3Utils.OUTFILE_BUCKET_TEMPLATE == "elasticbeanstalk-%s-wfoutput"
    assert s3Utils.RAW_BUCKET_TEMPLATE == "elasticbeanstalk-%s-files"
    assert s3Utils.BLOB_BUCKET_TEMPLATE == "elasticbeanstalk-%s-blobs"
    assert s3Utils.METADATA_BUCKET_TEMPLATE == "elasticbeanstalk-%s-metadata-bundles"
    assert s3Utils.TIBANNA_OUTPUT_BUCKET_TEMPLATE == "tibanna-output"


@pytest.mark.skip("This test is obsolete and known to be broken.")
@pytest.mark.beanstalk_failure
@pytest.mark.integrated
@using_fresh_ff_state_for_testing()
def test_regression_s3_utils_short_name_c4_706():

    # TODO: This test is broken because it calls beanstalk_info which calls describe_beanstalk_environments.
    #       But even beyond that, we also have entries in GLOBAL_ENV_BUCKET=foursight-envs for both short and long
    #       names, and that will confuse the structure of this test. -kmp 11-May-2022

    # Environment long names work (at least in legacy CGAP)
    s3Utils(env="fourfront-mastertest")

    with known_bug_expected(jira_ticket="C4-706", fixed=True, error_class=botocore.exceptions.ClientError):
        # Sort names not allowed.
        s3Utils(env="mastertest")


def _notice_health_page_connection_problem_for_test(env):
    env_url = get_beanstalk_real_url(env)
    health_page_url = f"{env_url.rstrip('/')}/health?format=json"
    failure_message = f"Health page for {env} is unavailable at {health_page_url}, so test is being skipped."
    try:
        if requests.get(health_page_url, timeout=2).status_code == 200:
            return None
        else:
            return failure_message
    except ConnectionError:  # e.g., connection failure or timeout, probably host unavailable
        return failure_message


@pytest.mark.integrated
@pytest.mark.parametrize('ff_ordinary_envname', ['fourfront-mastertest', 'fourfront-webdev', 'fourfront-hotseat'])
@using_fresh_ff_state_for_testing()
def test_s3utils_creation_ff_ordinary(ff_ordinary_envname):
    with EnvUtils.local_env_utils_for_testing(global_env_bucket=LEGACY_GLOBAL_ENV_BUCKET, env_name=ff_ordinary_envname):
        problem = _notice_health_page_connection_problem_for_test(ff_ordinary_envname)
        if not problem:
            util = s3Utils(env=ff_ordinary_envname)
            assert util.sys_bucket == 'elasticbeanstalk-%s-system' % ff_ordinary_envname
        else:
            pytest.skip(problem)


@pytest.mark.skip("This test is obsolete and known to be broken.")
@pytest.mark.beanstalk_failure
@pytest.mark.integrated
@using_fresh_ff_state_for_testing()
def test_s3utils_creation_ff_stg():
    # TODO: I was never sure what this was testing, so it's fine if it doesn't run.
    #       The problem is that staging is not properly declared in foursight-envs for now, so this can't work.
    #   -kmp 11-May-2022
    print("In test_s3Utils_creation_ff_stg. It is now", str(datetime.datetime.now()))

    def test_stg(ff_staging_envname):
        util = s3Utils(env=ff_staging_envname)
        actual_props = {
            'sys_bucket': util.sys_bucket,
            'outfile_bucket': util.outfile_bucket,
            'raw_file_bucket': util.raw_file_bucket,
            'url': util.url,
        }
        assert actual_props == {
            # Change to containers meant using new fourfront-webprod2 pseudoenv.
            'sys_bucket': 'elasticbeanstalk-fourfront-webprod2-system',
            'outfile_bucket': 'elasticbeanstalk-fourfront-webprod-wfoutput',
            'raw_file_bucket': 'elasticbeanstalk-fourfront-webprod-files',
            'url': FF_PUBLIC_URL_STG,
        }

    test_stg('staging')
    # NOTE: These values should not be parameters because we don't know how long PyTest caches the
    #       parameter values before using them. By doing the test this way, we hold the value for as
    #       little time as possible, making it least risk of being stale. -kmp 10-Jul-2020
    prd_beanstalk_env = compute_ff_prd_env()
    stg_beanstalk_env = get_standard_mirror_env(prd_beanstalk_env)
    test_stg(stg_beanstalk_env)


@pytest.mark.skip("This test is obsolete and known to be broken.")
@pytest.mark.beanstalk_failure
@pytest.mark.integrated
@using_fresh_ff_state_for_testing()
def test_s3utils_creation_ff_prd():
    # TODO: I was never sure what this was testing, so it's fine if it doesn't run.
    #       The problem may be that data is not properly declared in foursight-envs for now.
    #       In any case, this will have to be looked at later.
    #   -kmp 11-May-2022
    print("In test_s3Utils_creation_ff_prd. It is now", str(datetime.datetime.now()))

    def test_prd(ff_production_envname):
        with EnvUtils.locally_declared_data_for_testing():
            EnvUtils.init(ff_production_envname)
            util = s3Utils(env=ff_production_envname)
            actual_props = {
                'sys_bucket': util.sys_bucket,
                'outfile_bucket': util.outfile_bucket,
                'raw_file_bucket': util.raw_file_bucket,
                'url': util.url,
            }
            assert actual_props == {
                # Change to containers meant using new fourfront-webprod2 pseudoenv.
                'sys_bucket': 'elasticbeanstalk-fourfront-webprod2-system',
                'outfile_bucket': 'elasticbeanstalk-fourfront-webprod-wfoutput',
                'raw_file_bucket': 'elasticbeanstalk-fourfront-webprod-files',
                'url': FF_PUBLIC_URL_PRD,
            }

    test_prd('data')
    # NOTE: These values should not be parameters because we don't know how long PyTest caches the
    #       parameter values before using them. By doing the test this way, we hold the value for as
    #       little time as possible, making it least risk of being stale. -kmp 10-Jul-2020
    prd_beanstalk_env = compute_ff_prd_env()
    test_prd(prd_beanstalk_env)


# cgap beanstalks have been discontinued. -kmp 18-Feb-2022
#
# @pytest.mark.integrated
# @pytest.mark.parametrize('cgap_ordinary_envname', ['fourfront-cgaptest', 'fourfront-cgapwolf'])
# @using_fresh_cgap_state()
# # 'fourfront-cgapdev' has been decommissioned.
# def test_s3utils_creation_cgap_ordinary(cgap_ordinary_envname):
#     util = s3Utils(env=cgap_ordinary_envname)
#     assert util.sys_bucket == 'elasticbeanstalk-%s-system' % cgap_ordinary_envname


@pytest.mark.skip("This test is obsolete and known to be broken.")
@pytest.mark.beanstalk_failure
@pytest.mark.integrated
@using_fresh_cgap_state_for_testing()
def test_s3utils_creation_cgap_prd():
    assert EnvUtils.PUBLIC_URL_TABLE is not None, "Something is not initialized."
    # TODO: I'm not sure what this is testing, so it's hard to rewrite
    #   But I fear this use of env 'data' implies the GA test environment has overbroad privilege.
    #   We should make this work without access to 'data'.
    #   -kmp 13-Jan-2021
    print("In test_s3Utils_creation_cgap_prd. It is now", str(datetime.datetime.now()))

    def test_prd(cgap_production_envname):
        util = s3Utils(env=cgap_production_envname)
        actual_props = {
            'sys_bucket': util.sys_bucket,
            'outfile_bucket': util.outfile_bucket,
            'raw_file_bucket': util.raw_file_bucket,
            'url': util.url,
        }
        assert actual_props == {
            'sys_bucket': 'elasticbeanstalk-fourfront-cgap-system',
            'outfile_bucket': 'elasticbeanstalk-fourfront-cgap-wfoutput',
            'raw_file_bucket': 'elasticbeanstalk-fourfront-cgap-files',
            'url': _CGAP_MGB_PUBLIC_URL_PRD,
        }

    test_prd('cgap')
    # NOTE: These values should not be parameters because we don't know how long PyTest caches the
    #       parameter values before using them. By doing the test this way, we hold the value for as
    #       little time as possible, making it least risk of being stale. -kmp 13-Jul-2020
    test_prd('fourfront-cgap')
    test_prd(compute_cgap_prd_env())  # Hopefully returns 'fourfront-cgap' but just in case we're into new naming


@pytest.mark.skip("This test is obsolete and known to be broken.")
@pytest.mark.beanstalk_failure
@pytest.mark.integrated
@using_fresh_cgap_state_for_testing()
def test_s3utils_creation_cgap_stg():
    # Not sure why this is failing, but staging is declared wrong. -kmp 11-May-2022
    print("In test_s3Utils_creation_cgap_prd. It is now", str(datetime.datetime.now()))
    # For now there is no CGAP stg...
    assert compute_cgap_stg_env() is None, "There seems to be a CGAP staging environment. Tests need updating."


def _check_portal_auth(*, portal_env: str, auth_getter: Callable, auth_kind: str, server_pattern: str,
                       other_getters: Optional[Dict[str, Optional[Callable]]] = None, require_key=True):

    print()

    other_getters = other_getters or {}

    def check_auth(kind, auth):
        assert isinstance(auth, dict), f"The {kind} for {portal_env!r} is not a dict."
        if require_key:
            assert auth.get('key'), f"The {kind} dict for {portal_env!r} is missing 'key' part."
        assert auth.get('secret'), f"The {kind} dict for {portal_env!r} is missing 'secret' part."
        assert auth.get('server'), f"The {kind} dict for {portal_env!r} is missing 'server' part."
        assert re.match(server_pattern, auth['server']), (
            f"The {kind} dict 'server' part does not match {server_pattern!r}")

    auth_kind = f"{portal_env} {auth_kind}"
    s3u = s3Utils(env=portal_env)

    auth = auth_getter(s3u)
    check_auth(auth_kind, auth)

    auth_seen = [(auth_kind, auth)]
    for other_auth_kind, other_auth_getter in other_getters.items():

        other_auth_kind = f"{portal_env} {other_auth_kind}"
        print(f"Checking {other_auth_kind}...")

        other_auth = other_auth_getter(s3u)
        check_auth(other_auth_kind, other_auth)

        for seen_auth_kind, seen_auth in auth_seen:

            print(f"Checking {other_auth_kind} against {seen_auth_kind}...")
            if 'key' in other_auth and 'key' in seen_auth:  # For some aux getters, key can be missing
                assert other_auth['key'] != seen_auth['key'], (
                    f"{other_auth_kind} and {seen_auth_kind} 'key' parts unexpectedly match.")
            assert other_auth['secret'] != seen_auth['secret'], (
                f"{other_auth_kind} and {seen_auth_kind} 'secret' parts unexpectedly match.")

        auth_seen.append((other_auth_kind, other_auth))


@pytest.mark.integrated
@pytest.mark.parametrize('portal_env', ['staging', 'data'])
@using_fresh_ff_state_for_testing()
def test_s3utils_get_access_keys(portal_env):
    server_pattern = f"https{'?' if portal_env == 'staging' else ''}://{portal_env}[.]4dnucleome[.]org"
    _check_portal_auth(portal_env=portal_env, auth_getter=s3Utils.get_access_keys, auth_kind="access_keys",
                       server_pattern=server_pattern,
                       other_getters={
                           "access_keys for tibanna": lambda s3u: s3u.get_access_keys('access_key_tibanna'),
                           "access keys for foursight": lambda s3u: s3u.get_access_keys('access_key_foursight'),
                       })

    # with EnvManager.global_env_bucket_named(LEGACY_GLOBAL_ENV_BUCKET):
    #     util = s3Utils(env='data')
    #     keys = util.get_access_keys()
    #     assert keys['server'] == 'https://data.4dnucleome.org'
    #     # make sure we have keys for foursight and tibanna as well
    #     keys_tb = util.get_access_keys('access_key_tibanna')
    #     assert keys_tb['key'] != keys['key']
    #     assert keys_tb['server'] == keys['server']
    #     keys_fs = util.get_access_keys('access_key_foursight')
    #     assert keys_fs['key'] != keys_tb['key'] != keys['key']
    #     assert keys_fs['server'] == keys['server']


@pytest.mark.integrated
@pytest.mark.parametrize('portal_env', ['staging', 'data'])
@using_fresh_ff_state_for_testing()
def test_s3utils_get_ff_key(portal_env):
    """Tests that the actual key stored on staging is in good form."""
    server_pattern = f"https{'?' if portal_env == 'staging' else ''}://{portal_env}[.]4dnucleome[.]org"
    _check_portal_auth(portal_env=portal_env, auth_getter=s3Utils.get_ff_key, auth_kind="ff_key",
                       server_pattern=server_pattern)

    # # TODO: I'm not sure what this is testing, so it's hard to rewrite
    # #   But I fear this use of env 'staging' implies the GA test environment has overbroad privilege.
    # #   We should make this work without access to 'staging'.
    # #   -kmp 13-Jan-2021
    # util = s3Utils(env='staging')
    # keys = util.get_ff_key()
    # # This is in transition. Eventually it will reliably be https.
    # assert re.match('https?://staging.4dnucleome.org', keys['server'])


@pytest.mark.integrated
@pytest.mark.parametrize('portal_env', ['staging', 'data'])
@using_fresh_ff_state_for_testing()
def test_s3utils_get_jupyterhub_key(portal_env):
    _check_portal_auth(portal_env=portal_env, auth_getter=s3Utils.get_jupyterhub_key, auth_kind="jupyterhub_key",
                       server_pattern='https://jupyter.4dnucleome.org', require_key=False)

    # # TODO: I'm not sure what this is testing, so it's hard to rewrite
    # #   But I fear this use of env 'data' implies the GA test environment has overbroad privilege.
    # #   We should make this work without access to 'data'.
    # #   -kmp 13-Jan-2021
    # util = s3Utils(env='data')
    # key = util.get_jupyterhub_key()
    # assert 'secret' in key
    # assert key['server'] == 'https://jupyter.4dnucleome.org'
    # for dict_key in ['key', 'secret', 'server']:
    #     assert key[dict_key]


@pytest.mark.integrated
@pytest.mark.parametrize('portal_env', ['staging', 'data'])
@using_fresh_ff_state_for_testing()
def test_s3utils_get_higlass_key(portal_env):
    _check_portal_auth(portal_env=portal_env, auth_getter=s3Utils.get_higlass_key, auth_kind="higlass_key",
                       server_pattern='https://higlass.4dnucleome.org')

    # # TODO: I'm not sure what this is testing, so it's hard to rewrite
    # #   But I fear this use of env 'staging' implies the GA test environment has overbroad privilege.
    # #   We should make this work without access to 'staging'.
    # #   -kmp 13-Jan-2021
    # util = s3Utils(env='staging')
    # key = util.get_higlass_key()
    # assert isinstance(key, dict)
    # assert len(key.keys()) == 3
    # for dict_key in ['key', 'secret', 'server']:
    #     assert key[dict_key]


@pytest.mark.integrated
@using_fresh_ff_state_for_testing()
def test_s3utils_get_google_key():
    s3u = s3Utils(env='staging')
    keys = s3u.get_google_key()
    assert isinstance(keys, dict)
    assert keys['type'] == 'service_account'
    assert keys["project_id"] == "fourfront-396315" # yes, this is a magic constant
    for dict_key in ['private_key_id', 'private_key', 'client_email', 'client_id', 'auth_uri', 'client_x509_cert_url']:
        assert keys[dict_key]


@pytest.mark.unit
@using_fresh_ff_state_for_testing()
def test_s3utils_get_access_keys_with_old_style_default():
    s3u = s3Utils(env='fourfront-mastertest')
    with mock.patch.object(s3u, "get_key") as mock_get_key:

        def mocked_get_key(keyfile_name):
            ignored(keyfile_name)
            key_wrapper = {'default': actual_key}
            return key_wrapper

        mock_get_key.side_effect = mocked_get_key

        actual_key = {'secret': 'some-secret', 'server': 'some-server'}

        with pytest.raises(ValueError):
            s3u.get_access_keys()  # ill-formed secret is missing key. portal keys are expected to have a key.

        key = s3u.get_access_keys(require_key=False)  # accesses to jupyterhub keys could need this
        assert key == actual_key

        actual_key = {'key': 'some-key', 'server': 'some-server'}

        with pytest.raises(ValueError):
            s3u.get_access_keys(require_key=False)  # require_key=False only protects against missing key, not secret

        actual_key = {'key': 'some-key', 'secret': 'some-secret', 'server': 'some-server'}

        key = s3u.get_access_keys()
        assert key == actual_key


@pytest.mark.unit
@using_fresh_ff_state_for_testing()
def test_s3utils_get_key_non_json_data():

    s3u = s3Utils(env='fourfront-mastertest')

    non_json_string = '1 { 2 3 >'

    with mock.patch.object(s3u.s3, "get_object") as mock_get_object:
        mock_get_object.return_value = {'Body': io.BytesIO(bytes(non_json_string, encoding='utf-8'))}
        assert s3u.get_key() == non_json_string

    with mock.patch.object(s3u.s3, "get_object") as mock_get_object:
        mock_get_object.return_value = {'Body': io.StringIO(non_json_string)}
        assert s3u.get_key() == non_json_string


@pytest.mark.unit
@using_fresh_ff_state_for_testing()
def test_s3utils_delete_key():

    sample_key_name = "--- reserved_key_name_for_unit_testing ---"

    util = s3Utils(env='fourfront-mastertest')

    with mock.patch.object(util.s3, "delete_object") as mock_delete_object:

        def make_mocked_delete_object(expected_bucket, expected_key):

            def mocked_delete_object(Bucket, Key):  # noQA - AWS chooses the arg names
                assert Bucket == expected_bucket
                assert Key == expected_key

            return mocked_delete_object

        mock_delete_object.side_effect = make_mocked_delete_object(expected_bucket=util.outfile_bucket,
                                                                   expected_key=sample_key_name)

        util.delete_key(sample_key_name)  # This won't err if everything went well

        assert mock_delete_object.call_count == 1

        explicit_bucket = '--- reserved_bucket_name_for_unit_testing ---'

        mock_delete_object.side_effect = make_mocked_delete_object(expected_bucket=explicit_bucket,
                                                                   expected_key=sample_key_name)

        util.delete_key(sample_key_name, bucket=explicit_bucket)

        assert mock_delete_object.call_count == 2


@pytest.mark.unit
@using_fresh_ff_state_for_testing()
def test_s3utils_s3_put_with_mock_boto_s3():

    mfs = MockFileSystem()

    with mfs.mock_exists_open_remove():
        with mocked_s3utils():

            s3u = s3Utils(env='fourfront-mastertest')
            assert isinstance(s3u, s3Utils)
            assert isinstance(s3u.s3, MockBotoS3Client)
            s3 = s3u.s3

            some_key = 'some-key.json'
            some_file = f'downloaded-{some_key}'

            for item in [{'a': 1, 'b': 2}, "some string"]:
                for i, content_type in enumerate(['text/plain', 'application/json']):
                    for acl in [None, 'some-acl']:
                        print(f"Case {i} using item={item!r} content_type={content_type}")
                        item_to_etag = json.dumps(item) if isinstance(item, dict) else item
                        expected_etag = f'"{hashlib.md5(item_to_etag.encode("utf-8")).hexdigest()}"'
                        print(f"expected_etag={expected_etag}")
                        expected_result = {
                            "Body": item,
                            "Bucket": s3u.outfile_bucket,
                            "Key": some_key,
                            "ContentType": content_type,
                        }
                        if acl:
                            expected_result['ACL'] = acl
                        print(f"expected_result={expected_result}")
                        actual_result = s3u.s3_put(item, upload_key=some_key)
                        print(f"actual_result={actual_result}")
                        # assert actual_result == expected_result
                        assert actual_result['ETag'] == expected_etag
                        s3.download_file(Bucket=s3u.outfile_bucket, Key=some_key, Filename=some_file)
                        expected_file_contents = item_to_etag
                        print(f"Expected file contents: {expected_file_contents!r}")
                        actual_file_contents = file_contents(some_file)
                        print(f"Actual file contents: {actual_file_contents!r}")
                        assert actual_file_contents == expected_file_contents


@pytest.mark.unit
@using_fresh_ff_state_for_testing()
def test_s3utils_s3_put():

    util = s3Utils(env='fourfront-mastertest')

    some_content_type = "text/plain"
    with mock.patch("mimetypes.guess_type") as mock_guess_type:
        mock_guess_type.return_value = [some_content_type]
        with mock.patch.object(util.s3, "put_object") as mock_put_object:
            def mocked_put_object(**kwargs):
                return kwargs
            mock_put_object.side_effect = mocked_put_object
            item = {'a': 1, 'b': 2}
            some_key = 'some-key'
            assert util.s3_put(item, upload_key=some_key) == {
                "Body": json.dumps(item),
                "Bucket": util.outfile_bucket,
                "Key": some_key,
                "ContentType": some_content_type,
            }
            some_acl = 'some-acl'
            assert util.s3_put(item, upload_key=some_key, acl=some_acl) == {
                "Body": json.dumps(item),
                "Bucket": util.outfile_bucket,
                "Key": some_key,
                "ContentType": some_content_type,
                "ACL": some_acl,
            }


@pytest.mark.unit
@using_fresh_ff_state_for_testing()
def test_s3utils_s3_put_secret():

    util = s3Utils(env='fourfront-mastertest')
    standard_algorithm = "AES256"
    environmental_key = 'environmental-key'
    with override_environ(S3_ENCRYPT_KEY=environmental_key):
        with mock.patch.object(util.s3, "put_object") as mock_put_object:
            def mocked_put_object(**kwargs):
                return kwargs
            mock_put_object.side_effect = mocked_put_object
            item = {'a': 1, 'b': 2}
            some_key = 'some-key'
            some_secret = 'some-secret'
            assert util.s3_put_secret(item, keyname=some_key) == {
                "Body": json.dumps(item),
                "Bucket": util.sys_bucket,
                "Key": some_key,
                "SSECustomerKey": environmental_key,
                "SSECustomerAlgorithm": standard_algorithm,
            }
            some_bucket = 'some-bucket'
            assert util.s3_put_secret(item, keyname=some_key, bucket=some_bucket) == {
                "Body": json.dumps(item),
                "Bucket": some_bucket,
                "Key": some_key,
                "SSECustomerKey": environmental_key,
                "SSECustomerAlgorithm": standard_algorithm,
            }
            assert util.s3_put_secret(item, keyname=some_key, secret=some_secret) == {
                "Body": json.dumps(item),
                "Bucket": util.sys_bucket,
                "Key": some_key,
                "SSECustomerKey": some_secret,
                "SSECustomerAlgorithm": standard_algorithm,
            }


@pytest.mark.skip("This test is obsolete and known to be broken.")
@pytest.mark.beanstalk_failure
@pytest.mark.integratedx
@using_fresh_ff_state_for_testing()
def test_does_key_exist_integrated():
    """ Use staging to check for non-existant key """
    # TODO: One problem is that staging is not properly declared in foursight-envs for now.
    #   -kmp 11-May-2022
    util = s3Utils(env='staging')
    assert not util.does_key_exist('not_a_key')


@pytest.mark.skip("This test is obsolete and known to be broken.")
@pytest.mark.beanstalk_failure
@pytest.mark.unit
@using_fresh_ff_state_for_testing()
def test_does_key_exist_unit(integrated_names):
    """ Use staging to check for non-existant key """
    # TODO: One problem is that staging is not properly declared in foursight-envs for now.
    #   -kmp 11-May-2022
    with mocked_s3_integration(integrated_names=integrated_names) as s3_connection:
        assert not s3_connection.does_key_exist('not_a_key')


@pytest.mark.integratedx
@using_fresh_ff_state_for_testing()
def test_read_s3_integrated(integrated_s3_info):
    read = integrated_s3_info['s3Obj'].read_s3(integrated_s3_info['filename'])
    assert read.strip() == b'thisisatest'


@pytest.mark.unit
@using_fresh_ff_state_for_testing()
def test_read_s3_unit(integrated_names):
    # This unit test needs work, but its corresponding integration test works.
    # with mocked_s3_integration(integrated_names=integrated_names) as s3_connection:
    ffenv = integrated_names['ffenv'] if integrated_names else None
    filename = integrated_names['filename']
    bucket = s3Utils.OUTFILE_BUCKET_TEMPLATE % ffenv
    pseudo_filename = f"{bucket}/{filename}"
    file_content = "this is a unit test."
    with mocked_s3utils_with_sse(beanstalks=[ffenv], require_sse=False,
                                 files={pseudo_filename: file_content}):
        s3_connection = s3Utils(env=ffenv)
        read = s3_connection.read_s3(filename)
        assert read.strip() == file_content.encode('utf-8')


@pytest.mark.integratedx
@using_fresh_ff_state_for_testing()
def test_get_file_size_integrated(integrated_s3_info):
    size = integrated_s3_info['s3Obj'].get_file_size(integrated_s3_info['filename'])
    assert size == 11
    with pytest.raises(Exception) as exec_info:
        integrated_s3_info['s3Obj'].get_file_size('not_a_file')
    assert 'not found' in str(exec_info.value)


@pytest.mark.unit
@using_fresh_ff_state_for_testing()
def test_get_file_size_unit(integrated_names):

    # This unit test needs work, but its corresponding integration test works.
    # with mocked_s3_integration(integrated_names=integrated_names) as s3_connection:
    ffenv = integrated_names['ffenv'] if integrated_names else None
    filename = integrated_names['filename']
    bucket = s3Utils.OUTFILE_BUCKET_TEMPLATE % ffenv
    pseudo_filename = f"{bucket}/{filename}"
    file_content = "this is a unit test."
    with mocked_s3utils_with_sse(beanstalks=[ffenv], require_sse=False,
                                 files={pseudo_filename: file_content}):
        s3_connection = s3Utils(env=ffenv)

        size = s3_connection.get_file_size(integrated_names['filename'])
        assert size == len(file_content)
        with pytest.raises(Exception) as exec_info:
            s3_connection.get_file_size('not_a_file')
        assert 'not found' in str(exec_info.value)


@pytest.mark.integratedx
@using_fresh_ff_state_for_testing()
def test_size_integrated(integrated_s3_info):
    """ Get size of non-existent, real bucket """
    bucket = integrated_s3_info['s3Obj'].sys_bucket
    sz = integrated_s3_info['s3Obj'].size(bucket)
    assert sz > 0
    with pytest.raises(Exception) as exec_info:
        integrated_s3_info['s3Obj'].size('not_a_bucket')
    assert 'NoSuchBucket' in str(exec_info.value)


@pytest.mark.unit
@using_fresh_ff_state_for_testing()
def test_size_unit(integrated_names):
    """ Get size of non-existent, real bucket """

    # This unit test needs work, but its corresponding integration test works.
    # with mocked_s3_integration(integrated_names=integrated_names) as s3_connection:
    ffenv = integrated_names['ffenv'] if integrated_names else None

    with mocked_s3utils_with_sse(beanstalks=[ffenv], require_sse=False):
        s3_connection = s3Utils(env=ffenv)

        bucket = s3_connection.sys_bucket

        # # Because this is a mock, the set of objects will be empty, so let's initialize it.
        s3_connection.s3.put_object(Bucket=bucket, Key="a.txt", Body=b'apple')
        s3_connection.s3.put_object(Bucket=bucket, Key="b.txt", Body=b'orange, banana')
        s3_connection.s3.put_object(Bucket=bucket, Key="c.txt", Body=b'papaya')

        # When buckets exist, we expect no error
        sz = s3_connection.size(bucket)
        assert sz == 3, "Expected exactly 3 files in the mocked bucket, but got %s" % sz

        # When bucket doesn't exist, we expect an error
        with pytest.raises(Exception, match='.*NoSuchBucket.*') as exec_info:
            ignorable(exec_info)
            s3_connection.size('not_a_bucket')


@pytest.mark.integratedx
@using_fresh_ff_state_for_testing()
def test_get_file_size_in_gb_integrated(integrated_s3_info):

    s3_connection = integrated_s3_info['s3Obj']

    size = s3_connection.get_file_size(integrated_s3_info['filename'],
                                       add_gb=2, size_in_gb=True)
    assert int(size) == 2


@pytest.mark.unit
@using_fresh_ff_state_for_testing()
def test_get_file_size_in_gb_unit(integrated_names):

    # This unit test needs work, but its corresponding integration test works.
    # with mocked_s3_integration(integrated_names=integrated_names) as s3_connection:
    ffenv = integrated_names['ffenv'] if integrated_names else None
    filename = integrated_names['filename']
    bucket = s3Utils.OUTFILE_BUCKET_TEMPLATE % ffenv
    pseudo_filename = f"{bucket}/{filename}"
    file_content = "this is a unit test."
    with mocked_s3utils_with_sse(beanstalks=[ffenv], require_sse=False,
                                 files={pseudo_filename: file_content}):
        s3_connection = s3Utils(env=ffenv)

        size = s3_connection.get_file_size(integrated_names['filename'],
                                           add_gb=2, size_in_gb=True)
        assert int(size) == 2


@pytest.mark.integratedx
@using_fresh_ff_state_for_testing()
def test_read_s3_zip_integrated(integrated_s3_info):
    filename = integrated_s3_info['zip_filename']
    files = integrated_s3_info['s3Obj'].read_s3_zipfile(filename, ['summary.txt', 'fastqc_data.txt'])
    assert files['summary.txt']
    assert files['fastqc_data.txt']
    assert files['summary.txt'].startswith(b'PASS')


@pytest.mark.unit
@using_fresh_ff_state_for_testing()
def test_read_s3_zip_unit(integrated_names):

    # with mocked_s3_integration(integrated_names=integrated_names) as s3_connection:
    ffenv = integrated_names['ffenv'] if integrated_names else None

    with mocked_s3utils_with_sse(beanstalks=[ffenv],
                                 # TODO (C4-819): This and other tests that are uploading files to S3
                                 #                are not passing KMS arguments. Test needs to be upgraded.
                                 #                But also need to figure out if that's a bug in test or
                                 #                underlying API.  -kmp 13-May-2022
                                 require_sse=False):
        s3_connection = s3Utils(env=ffenv)

        # In our mock, this won't exist already on S3 like in the integrated version of this test,
        # so we have to preload to our mock S3 manually. -kmp 13-Jan-2021
        s3_connection.s3.upload_file(Filename=integrated_names['zip_path'],
                                     Bucket=s3_connection.outfile_bucket,
                                     Key=integrated_names['zip_filename'])

        zip_filename = integrated_names['zip_filename']
        files = s3_connection.read_s3_zipfile(zip_filename, ['summary.txt', 'fastqc_data.txt'])
        assert files['summary.txt']
        assert files['fastqc_data.txt']
        assert files['summary.txt'].startswith(b'PASS')
        assert files['fastqc_data.txt'].startswith(b'##FastQC')


@pytest.mark.integratedx
@pytest.mark.parametrize("suffix, expected_report", [("", "fastqc_report.html"), ("2", "qc_report.html")])
@using_fresh_ff_state_for_testing()
def test_unzip_s3_to_s3_integrated(integrated_s3_info, suffix, expected_report):
    """test for unzip_s3_to_s3 with case where there is a basdir"""

    zip_filename_key = "zip_filename" + suffix

    prefix = '__test_data/extracted'
    filename = integrated_s3_info[zip_filename_key]
    s3_connection = integrated_s3_info['s3Obj']

    # start with a clean test space
    s3_connection.s3_delete_dir(prefix)

    # ensure this thing was deleted
    # if no files there will be no Contents in response
    objs = s3_connection.s3_read_dir(prefix)
    assert not objs.get('Contents')

    # now copy to that dir we just deleted
    ret_files = s3_connection.unzip_s3_to_s3(filename, prefix)
    assert ret_files[expected_report]['s3key'].startswith("https://s3.amazonaws.com")
    assert ret_files[expected_report]['s3key'].endswith(expected_report)

    objs = s3_connection.s3_read_dir(prefix)
    assert objs.get('Contents')


@pytest.mark.unit
@pytest.mark.parametrize("suffix, expected_report", [("", "fastqc_report.html"), ("2", "qc_report.html")])
@using_fresh_ff_state_for_testing()
def test_unzip_s3_to_s3_unit(integrated_names, suffix, expected_report):
    """test for unzip_s3_to_s3 with case where there is no basdir"""

    # with mocked_s3_integration(integrated_names=integrated_names) as s3_connection:
    ffenv = integrated_names['ffenv'] if integrated_names else None

    with mocked_s3utils_with_sse(beanstalks=[ffenv], require_sse=False):
        s3_connection = s3Utils(env=ffenv)

        # In our mock, this won't exist already on S3 like in the integrated version of this test,
        # so we have to preload to our mock S3 manually. -kmp 13-Jan-2021
        s3_connection.s3.upload_file(Filename=integrated_names['zip_path' + suffix],
                                     Bucket=s3_connection.outfile_bucket,
                                     Key=integrated_names['zip_filename' + suffix])

        zip_filename_key = "zip_filename" + suffix
        prefix = '__test_data/extracted'
        filename = integrated_names[zip_filename_key]

        # ensure this thing was deleted
        # if no files there will be no Contents in response
        objs = s3_connection.s3_read_dir(prefix)
        assert not objs.get('Contents')

        # now copy to that dir we just deleted
        ret_files = s3_connection.unzip_s3_to_s3(filename, prefix)
        assert ret_files[expected_report]['s3key'].startswith("https://s3.amazonaws.com")
        assert ret_files[expected_report]['s3key'].endswith(expected_report)

        objs = s3_connection.s3_read_dir(prefix)
        assert objs.get('Contents')


@pytest.mark.integratedx
@using_fresh_ff_state_for_testing()
def test_unzip_s3_to_s3_store_results_integrated(integrated_s3_info):
    """test for unzip_s3_to_s3 with case where there is a basdir and store_results=False"""
    prefix = '__test_data/extracted'
    filename = integrated_s3_info['zip_filename']
    s3_connection = integrated_s3_info['s3Obj']

    s3_connection.s3_delete_dir(prefix)

    # ensure this thing was deleted
    # if no files there will be no Contents in response
    objs = s3_connection.s3_read_dir(prefix)
    assert not objs.get('Contents')

    # now copy to that dir we just deleted
    ret_files = s3_connection.unzip_s3_to_s3(filename, prefix, store_results=False)
    assert len(ret_files) == 0  # no returned content

    objs = s3_connection.s3_read_dir(prefix)
    assert objs.get('Contents')


@pytest.mark.unit
@using_fresh_ff_state_for_testing()
def test_unzip_s3_to_s3_store_results_unit(integrated_names):
    """test for unzip_s3_to_s3 with case where there is a basdir and store_results=False"""

    # with mocked_s3_integration(integrated_names=integrated_names) as s3_connection:
    ffenv = integrated_names['ffenv'] if integrated_names else None

    with mocked_s3utils_with_sse(beanstalks=[ffenv], require_sse=False):
        s3_connection = s3Utils(env=ffenv)

        # In our mock, this won't exist already on S3 like in the integrated version of this test,
        # so we have to preload to our mock S3 manually. -kmp 13-Jan-2021
        s3_connection.s3.upload_file(Filename=integrated_names['zip_path'],
                                     Bucket=s3_connection.outfile_bucket,
                                     Key=integrated_names['zip_filename'])

        zip_filename_key = "zip_filename"
        prefix = '__test_data/extracted'
        filename = integrated_names[zip_filename_key]

        # ensure this thing was deleted
        # if no files there will be no Contents in response
        objs = s3_connection.s3_read_dir(prefix)
        assert not objs.get('Contents')

        # now copy to that dir we just deleted
        ret_files = s3_connection.unzip_s3_to_s3(filename, prefix, store_results=False)
        assert len(ret_files) == 0  # no returned content

        objs = s3_connection.s3_read_dir(prefix)
        assert objs.get('Contents')


@using_fresh_ff_state_for_testing()
def test_s3_utils_legacy_behavior():
    # From https://hms-dbmi.atlassian.net/browse/C4-674

    outfile_bucket = 'my-outfile-bucket'
    sys_bucket = 'my-system-bucket'
    raw_file_bucket = 'my-raw_file-bucket'

    def test_it():

        # As long as sys_bucket= is given in the s3Utils() call, it will just fill the slots
        # with given values and won't try to do anything smart.

        s = s3Utils(outfile_bucket, sys_bucket, raw_file_bucket)
        assert s.outfile_bucket == outfile_bucket
        assert s.sys_bucket == sys_bucket
        assert s.raw_file_bucket == raw_file_bucket
        assert s.blob_bucket is None
        assert s.metadata_bucket is None
        assert s.tibanna_cwls_bucket is None
        assert s.tibanna_output_bucket is None

        s = s3Utils(sys_bucket=sys_bucket)
        assert s.outfile_bucket is None
        assert s.sys_bucket == sys_bucket
        assert s.raw_file_bucket is None
        assert s.blob_bucket is None
        assert s.metadata_bucket is None
        assert s.tibanna_cwls_bucket is None
        assert s.tibanna_output_bucket is None

    test_it()

    # Test that certain legacy behavior is unperturbed by GLOBAL_ENV_BUCKET (or its older name, GLOBAL_BUCKET_ENV)
    with override_environ(GLOBAL_BUCKET_ENV='should-be-unused',
                          GLOBAL_ENV_BUCKET='should-be-unused'):
        test_it()


@using_fresh_ff_state_for_testing()
def test_s3_utils_buckets_modern():

    env_name = 'fourfront-foo'

    with mocked_s3utils_with_sse():  # mock.patch("boto3.client"):

        s = s3Utils(env=env_name)

        assert s.outfile_bucket != 'the-output-file-bucket'
        assert s.sys_bucket != 'the-system-bucket'
        assert s.raw_file_bucket != 'the-raw-file-bucket'
        assert s.blob_bucket != 'the-blob-bucket'
        assert s.metadata_bucket != 'the-metadata-bundles-bucket'
        assert s.tibanna_cwls_bucket != 'the-tibanna-cwls-bucket'
        assert s.tibanna_output_bucket != 'the-tibanna-output-bucket'

        assert s.outfile_bucket == f'elasticbeanstalk-{env_name}-wfoutput'
        assert s.sys_bucket == f'elasticbeanstalk-{env_name}-system'
        assert s.raw_file_bucket == f'elasticbeanstalk-{env_name}-files'
        assert s.blob_bucket == f'elasticbeanstalk-{env_name}-blobs'
        assert s.metadata_bucket == f'elasticbeanstalk-{env_name}-metadata-bundles'
        assert s.tibanna_cwls_bucket == 'tibanna-cwls'
        assert s.tibanna_output_bucket == 'tibanna-output'

        assert s.s3_encrypt_key_id is None

        es_url = make_mock_es_url(env_name)

        with mock.patch.object(s3_utils_module.EnvManager, "fetch_health_page_json") as mock_fetch:
            def mocked_fetch_health_page_json(url):
                # Should match fourfront-foo-xxxx.some-aws-domain or foo.some-hms-domain
                assert any(re.match(f'https?://{e}[.-].*', url) for e in [env_name, short_env_name(env_name)])
                return {
                    HealthPageKey.ELASTICSEARCH: es_url,  # es_server_short,
                    HealthPageKey.SYSTEM_BUCKET: "the-system-bucket",
                    HealthPageKey.PROCESSED_FILE_BUCKET: "the-output-file-bucket",
                    HealthPageKey.FILE_UPLOAD_BUCKET: "the-raw-file-bucket",
                    HealthPageKey.BLOB_BUCKET: "the-blob-bucket",
                    HealthPageKey.METADATA_BUNDLES_BUCKET: "the-metadata-bundles-bucket",
                    HealthPageKey.TIBANNA_CWLS_BUCKET: "the-tibanna-cwls-bucket",
                    HealthPageKey.TIBANNA_OUTPUT_BUCKET: "the-tibanna-output-bucket",
                    HealthPageKey.S3_ENCRYPT_KEY_ID: "my-encrypt-key",
                    HealthPageKey.ENV_BUCKET: "fourfront-foo-foursight-envs",
                    HealthPageKey.ENV_ECOSYSTEM: "secondary",
                    HealthPageKey.ENV_NAME: "fourfront-foo",
                    HealthPageKey.BEANSTALK_ENV: "fourfront-foo",
                }
            mock_fetch.side_effect = mocked_fetch_health_page_json

            s = s3Utils(env=env_name)

            assert s.outfile_bucket == 'the-output-file-bucket'
            assert s.sys_bucket == 'the-system-bucket'
            assert s.raw_file_bucket == 'the-raw-file-bucket'
            assert s.blob_bucket == 'the-blob-bucket'
            assert s.metadata_bucket == 'the-metadata-bundles-bucket'
            assert s.tibanna_cwls_bucket == 'the-tibanna-cwls-bucket'
            assert s.tibanna_output_bucket == 'the-tibanna-output-bucket'

            assert s.s3_encrypt_key_id == 'my-encrypt-key'

            e = s.env_manager

            assert e.s3 == s.s3
            # This mock is not elaborate enough for testing how e.portal_url is set up.
            # assert e.portal_url = ...
            assert e.es_url == es_url  # es_server_https
            assert e.env_name == env_name


C4_853_FIX_INFO = {
    'fourfront-production-blue':
        {'metadata_fix': False, 'tibanna_fix': False},
    'fourfront-production-green':
        {'metadata_fix': True, 'tibanna_fix': False},
    'data':
        {'metadata_fix': True, 'tibanna_fix': False},
    'staging':
        {'metadata_fix': False, 'tibanna_fix': False},

    'fourfront-mastertest':
        {'metadata_fix': True, 'tibanna_fix': False},
    'mastertest':
        {'metadata_fix': True, 'tibanna_fix': False},

    'fourfront-webdev':
        {'metadata_fix': True, 'tibanna_fix': False},
    'webdev':
        {'metadata_fix': True, 'tibanna_fix': False},

    'fourfront-hotseat':
        {'metadata_fix': True, 'tibanna_fix': True},
    'hotseat':
        {'metadata_fix': True, 'tibanna_fix': True},
}


@pytest.mark.parametrize('env_name', [
    'fourfront-mastertest', 'fourfront-webdev', 'fourfront-hotseat', 'mastertest', 'webdev', 'hotseat'])
@using_fresh_ff_deployed_state_for_testing()
def test_s3_utils_buckets_ff_live_ecosystem_not_production(env_name):
    print()  # Start on fresh line
    print("=" * 80)
    print(f"env_name={env_name}")
    print("=" * 80)
    s3u = s3Utils(env=env_name)

    full_env = full_env_name(env_name)

    print(f"s3u.sys_bucket={s3u.sys_bucket}")
    assert s3u.sys_bucket == f'elasticbeanstalk-{full_env}-system'

    print(f"s3u.outfile_bucket={s3u.outfile_bucket}")
    assert s3u.outfile_bucket == f'elasticbeanstalk-{full_env}-wfoutput'
    print(f"s3u.raw_file_bucket={s3u.raw_file_bucket}")
    assert s3u.raw_file_bucket == f'elasticbeanstalk-{full_env}-files'
    print(f"s3u.blob_bucket={s3u.blob_bucket}")
    assert s3u.blob_bucket == f'elasticbeanstalk-{full_env}-blobs'
    assert s3u.tibanna_output_bucket == 'tibanna-output'

    assert s3u.s3_encrypt_key_id is None
    assert isinstance(s3u.env_manager, EnvManager)
    assert isinstance(s3u.env_manager.s3, botocore.client.BaseClient)  # It's hard to test for S3 specifically
    es_url = s3u.env_manager.es_url
    print(f"Checking {es_url} ...")
    # NOTE: The right answers differ here from production, but as long as something approximately like
    #       the short name is in there, that's enough.
    names_part = _make_similar_names_alternation(env_name)
    pattern = f"https://vpc-[eo]s-.*({names_part}).*[.]amazonaws[.]com:443"
    print(f"pattern={pattern}")
    assert es_url and re.match(pattern, es_url)
    assert s3u.env_manager.env_name == full_env


@pytest.mark.xfail(reason="awaiting deployment transition of ecosystem software")
@pytest.mark.parametrize('env_name', [
    'fourfront-mastertest', 'fourfront-webdev', 'fourfront-hotseat', 'mastertest', 'webdev', 'hotseat'])
@using_fresh_ff_deployed_state_for_testing()
def test_s3_utils_buckets_ff_live_ecosystem_not_production_transition(env_name):
    fix_info = C4_853_FIX_INFO[env_name]

    print()  # Start on fresh line
    print("=" * 80)
    print(f"env_name={env_name}")
    print("=" * 80)
    s3u = s3Utils(env=env_name)

    full_env = full_env_name(env_name)

    with known_bug_expected(jira_ticket="C4-853", fixed=fix_info['metadata_fix'], error_class=AssertionError):
        print(f"s3u.metadata_bucket={s3u.metadata_bucket}")
        assert s3u.metadata_bucket == f'elasticbeanstalk-{full_env}-metadata-bundles'

    with known_bug_expected(jira_ticket="C4-853", fixed=fix_info['tibanna_fix'], error_class=AssertionError):
        assert s3u.tibanna_cwls_bucket == 'tibanna-cwls'


def _make_similar_names_alternation(env_name):
    # e.g.,
    #  _make_similar_names_alternation('production')   # if the full_env_name is fourfront-production
    #  returns
    #    pro?d(uction)?.green|fourfront.pro?d(uction)?.green
    return "|".join([(x.replace('-', '.')  # match any char where a "-" is in the env name
                      .replace('production', 'pro?d(uction)?')  # allow abbreviating production
                      .replace('development', 'dev(elopment)?'))  # allow abbreviating development
                     for x in [short_env_name(env_name), full_env_name(env_name)]])


@pytest.mark.parametrize('env_name', [
    'fourfront-production-blue', 'fourfront-production-green',
    'data', 'staging'])
@using_fresh_ff_deployed_state_for_testing()
def test_s3_utils_buckets_ff_live_ecosystem_production(env_name):
    print()  # Start on fresh line
    print("=" * 80)
    print(f"env_name={env_name}")
    print("=" * 80)
    s3u = s3Utils(env=env_name)

    s3_sys_env_token = 'fourfront-webprod2'  # Shared by data and staging for sys bucket

    print(f"s3u.sys_bucket={s3u.sys_bucket}")
    assert s3u.sys_bucket == f'elasticbeanstalk-{s3_sys_env_token}-system'

    s3_env_token = 'fourfront-webprod'  # Shared by data and staging for other than sys bucket

    print(f"s3u.outfile_bucket={s3u.outfile_bucket}")
    assert s3u.outfile_bucket == f'elasticbeanstalk-{s3_env_token}-wfoutput'
    print(f"s3u.raw_file_bucket={s3u.raw_file_bucket}")
    assert s3u.raw_file_bucket == f'elasticbeanstalk-{s3_env_token}-files'
    print(f"s3u.blob_bucket={s3u.blob_bucket}")
    assert s3u.blob_bucket == f'elasticbeanstalk-{s3_env_token}-blobs'

    assert s3u.s3_encrypt_key_id is None
    assert isinstance(s3u.env_manager, EnvManager)
    assert isinstance(s3u.env_manager.s3, botocore.client.BaseClient)  # It's hard to test for S3 specifically
    es_url = s3u.env_manager.es_url
    print(f"Checking {es_url}...")
    # tokenify(full_env_name(env_name)) matches better, but as long as short env name is there, it's enough.
    names_part = _make_similar_names_alternation(env_name)
    pattern = f"https://vpc-[eo]s-.*({names_part}).*[.]amazonaws[.]com:443"
    print(f"pattern={pattern}")
    assert es_url and re.match(pattern, es_url)
    assert is_stg_or_prd_env(s3u.env_manager.env_name)


@pytest.mark.xfail(reason="awaiting deployment transition of ecosystem software")
@pytest.mark.parametrize('env_name', [
    'fourfront-production-blue', 'fourfront-production-green',
    'data', 'staging'])
@using_fresh_ff_deployed_state_for_testing()
def test_s3_utils_buckets_ff_live_ecosystem_production_transition(env_name):
    fix_info = C4_853_FIX_INFO[env_name]

    s3_env_token = 'fourfront-webprod'  # Shared by data and staging for other than sys bucket

    print()  # Start on fresh line
    print("=" * 80)
    print(f"env_name={env_name}")
    print("=" * 80)
    s3u = s3Utils(env=env_name)
    with known_bug_expected(jira_ticket="C4-853", fixed=fix_info['metadata_fix'], error_class=AssertionError):
        print(f"s3u.metadata_bucket={s3u.metadata_bucket}")
        assert s3u.metadata_bucket == f'elasticbeanstalk-{s3_env_token}-metadata-bundles'

    with known_bug_expected(jira_ticket="C4-853", fixed=fix_info['tibanna_fix'], error_class=AssertionError):
        assert s3u.tibanna_cwls_bucket == 'tibanna-cwls'
    assert s3u.tibanna_output_bucket == 'tibanna-output'


@using_fresh_ff_state_for_testing()
def test_s3_utils_environment_variable_use():

    with pytest.raises(SynonymousEnvironmentVariablesMismatched):

        with override_environ(GLOBAL_BUCKET_ENV='should-be-unused',
                              GLOBAL_ENV_BUCKET='inconsistently-unused'):

            # If we do the simple-minded version of this, the environment variable doesn't matter
            s3Utils(sys_bucket='foo')

            with pytest.raises(SynonymousEnvironmentVariablesMismatched):
                # If we don't initialize the sys_bucket, we have to go through the smart protocols
                # and expect environment variables to be in order.
                s3Utils()


@using_fresh_ff_state_for_testing()
def test_s3_utils_verify_and_get_env_config():

    with mock.patch.object(EnvManager, "verify_and_get_env_config") as mock_implementation:

        def mocked_implementation(s3_client, global_bucket, env):
            assert s3_client == 'dummy-s3'
            assert global_bucket == 'dummy-bucket'
            assert env == 'dummy-env'

        mock_implementation.side_effect = mocked_implementation

        s3Utils.verify_and_get_env_config(s3_client='dummy-s3', global_bucket='dummy-bucket', env='dummy-env')
        s3Utils.verify_and_get_env_config(env='dummy-env', s3_client='dummy-s3', global_bucket='dummy-bucket')
        s3Utils.verify_and_get_env_config('dummy-s3', 'dummy-bucket', 'dummy-env')


@using_fresh_ff_state_for_testing()
def test_s3_utils_fetch_health_page_json():

    with mock.patch.object(EnvManager, "fetch_health_page_json") as mock_implementation:

        def mocked_implementation(url, use_urllib):
            assert url == 'dummy-url'
            assert use_urllib == 'dummy-use-urllib'

        mock_implementation.side_effect = mocked_implementation

        s3Utils.fetch_health_page_json(url='dummy-url', use_urllib='dummy-use-urllib')
        s3Utils.fetch_health_page_json(use_urllib='dummy-use-urllib', url='dummy-url')
        s3Utils.fetch_health_page_json('dummy-url', 'dummy-use-urllib')


@using_fresh_ff_state_for_testing()
def test_env_manager_fetch_health_page_json():

    sample_health_page = {"mocked": "health-page"}

    class MockHelper:

        def __init__(self):
            self.used_mocked_get = False
            self.used_mocked_urlopen = False

        def mocked_get(self, url):
            assert url.endswith("/health?format=json")
            self.used_mocked_get = True
            return MockResponse(json=sample_health_page)

        def mocked_urlopen(self, url):
            assert url.endswith("/health?format=json")
            self.used_mocked_urlopen = True
            return io.BytesIO(json.dumps(sample_health_page).encode('utf-8'))

    with mock.patch("requests.get") as mock_get:
        with mock.patch("urllib.request.urlopen") as mock_urlopen:

            helper = MockHelper()
            mock_get.side_effect = helper.mocked_get
            mock_urlopen.side_effect = helper.mocked_urlopen

            assert EnvManager.fetch_health_page_json("http://something/health?format=json",
                                                     use_urllib=False) == sample_health_page
            # We always use urllib now.
            assert helper.used_mocked_get is False
            assert helper.used_mocked_urlopen is True

            helper = MockHelper()
            mock_get.side_effect = helper.mocked_get
            mock_urlopen.side_effect = helper.mocked_urlopen

            assert EnvManager.fetch_health_page_json("http://something/health?format=json",
                                                     use_urllib=True) == sample_health_page
            # We always use urllib now.
            assert helper.used_mocked_get is False
            assert helper.used_mocked_urlopen is True


@using_fresh_ff_state_for_testing()
def test_env_manager():

    test_env = 'fourfront-foo'
    test_env2 = 'fourfront-another-plausible-env'

    # This tests that with no env_name argument, we can figure out there's only one environment
    with mocked_s3utils_with_sse(beanstalks=[test_env]):
        with EnvManager.global_env_bucket_named(name='global-env-1'):
            with pytest.raises(Exception):
                EnvManager()

        env_mgr = EnvManager()

        assert env_mgr.portal_url == make_mock_portal_url(test_env)
        assert env_mgr.es_url == make_mock_es_url(test_env)
        assert env_mgr.env_name == test_env

    # This tests that additional ecosystems do not confuse env defaulting
    with mocked_s3utils_with_sse(beanstalks=[test_env, 'foo.ecosystem']):
        with EnvManager.global_env_bucket_named(name='global-env-1'):
            with pytest.raises(Exception):
                EnvManager()

        env_mgr = EnvManager()

        assert env_mgr.portal_url == make_mock_portal_url(test_env)
        assert env_mgr.es_url == make_mock_es_url(test_env)
        assert env_mgr.env_name == test_env

    # This tests that we notice a legit ambiguity in environment names
    with mocked_s3utils_with_sse(beanstalks=[test_env, 'another-possible-env']):
        with pytest.raises(CannotInferEnvFromManyGlobalEnvs):
            EnvManager()  # can't tell which environment

    # This tests that we can overcome a legit ambiguity
    with mocked_s3utils_with_sse(beanstalks=[test_env, test_env2]):
        with EnvManager.global_env_bucket_named(name='global-env-1'):
            with pytest.raises(Exception):
                EnvManager()

        env_mgr = EnvManager(env_name=test_env)

        assert env_mgr.portal_url == make_mock_portal_url(test_env)
        assert env_mgr.es_url == make_mock_es_url(test_env)
        assert env_mgr.env_name == test_env

        env_mgr = EnvManager(env_name=test_env2)

        assert env_mgr.portal_url == make_mock_portal_url(test_env2)
        assert env_mgr.es_url == make_mock_es_url(test_env2)
        assert env_mgr.env_name == test_env2


@using_fresh_ff_state_for_testing()
def test_env_manager_verify_and_get_env_config():

    test_env = 'fourfront-foo'

    with mocked_s3utils_with_sse(beanstalks=[test_env]) as boto3:

        config = EnvManager.verify_and_get_env_config(s3_client=boto3.client('s3'),
                                                      global_bucket=LEGACY_GLOBAL_ENV_BUCKET,
                                                      env=test_env)

        assert config['fourfront'] == make_mock_portal_url(test_env)
        assert config['es'] == make_mock_es_url(test_env)
        assert config['ff_env'] == test_env

        config = EnvManager.verify_and_get_env_config(s3_client=boto3.client('s3'),
                                                      global_bucket=LEGACY_GLOBAL_ENV_BUCKET,
                                                      # env will default because we have only one
                                                      env=None
                                                      )

        assert config['fourfront'] == make_mock_portal_url(test_env)
        assert config['es'] == make_mock_es_url(test_env)
        assert config['ff_env'] == test_env


@using_fresh_ff_state_for_testing()
def test_env_manager_compose():

    test_env = 'fourfront-foo'

    test_portal = make_mock_portal_url(test_env)
    test_es = make_mock_es_url(test_env)

    with mocked_s3utils_with_sse(beanstalks=[test_env]) as boto3:

        config = EnvManager.verify_and_get_env_config(s3_client=boto3.client('s3'),
                                                      global_bucket=LEGACY_GLOBAL_ENV_BUCKET,
                                                      env=test_env)

        assert config['fourfront'] == test_portal
        assert config['es'] == test_es
        assert config['ff_env'] == test_env

        env_manager_from_desc = EnvManager.compose(s3=boto3.client('s3'),
                                                   portal_url=test_portal,
                                                   es_url=test_es,
                                                   env_name=test_env)

        assert env_manager_from_desc.env_description['ff_env'] == test_env
        assert env_manager_from_desc.env_description['fourfront'] == test_portal
        assert env_manager_from_desc.env_description['es'] == test_es


@using_fresh_ff_state_for_testing()
def test_env_manager_global_env_bucket_name():

    # Now that we're containerized, there is always something in GLOBAL_ENV_BUCKET
    #
    # # These tests expect to be run in an environment that does not have these buckets bound globally.
    # assert os.environ.get('GLOBAL_ENV_BUCKET') is None
    # assert os.environ.get('GLOBAL_BUCKET_ENV') is None
    #
    # This is the rewrite:

    global_env_bucket_for_testing = os.environ.get('GLOBAL_ENV_BUCKET')
    assert global_env_bucket_for_testing == LEGACY_GLOBAL_ENV_BUCKET
    global_bucket_env_for_testing = os.environ.get('GLOBAL_BUCKET_ENV')
    assert global_bucket_env_for_testing is None or global_bucket_env_for_testing == global_env_bucket_for_testing

    with EnvManager.global_env_bucket_named(name='foo'):

        # Make sure we picked mock values that are different than the global settings.
        assert os.environ.get('GLOBAL_ENV_BUCKET') != global_env_bucket_for_testing
        assert os.environ.get('GLOBAL_BUCKET_ENV') != global_bucket_env_for_testing

        # Beyond here we're clear to test with our mock values.
        assert os.environ.get('GLOBAL_ENV_BUCKET') == 'foo'
        assert os.environ.get('GLOBAL_BUCKET_ENV') == 'foo'
        assert EnvManager.global_env_bucket_name() == 'foo'

        with override_environ(GLOBAL_BUCKET_ENV='bar'):

            assert os.environ.get('GLOBAL_ENV_BUCKET') == 'foo'
            assert os.environ.get('GLOBAL_BUCKET_ENV') == 'bar'
            with pytest.raises(SynonymousEnvironmentVariablesMismatched):
                EnvManager.global_env_bucket_name()

            with override_environ(GLOBAL_ENV_BUCKET='bar'):

                assert os.environ.get('GLOBAL_ENV_BUCKET') == 'bar'
                assert os.environ.get('GLOBAL_BUCKET_ENV') == 'bar'
                assert EnvManager.global_env_bucket_name() == 'bar'

        with override_environ(GLOBAL_ENV_BUCKET='bar'):

            assert os.environ.get('GLOBAL_ENV_BUCKET') == 'bar'
            assert os.environ.get('GLOBAL_BUCKET_ENV') == 'foo'
            with pytest.raises(SynonymousEnvironmentVariablesMismatched):
                EnvManager.global_env_bucket_name()

            with override_environ(GLOBAL_BUCKET_ENV='bar'):

                assert os.environ.get('GLOBAL_ENV_BUCKET') == 'bar'
                assert os.environ.get('GLOBAL_BUCKET_ENV') == 'bar'
                assert EnvManager.global_env_bucket_name() == 'bar'


@using_fresh_ff_state_for_testing()
def test_get_and_set_object_tags():

    mock_boto3 = MockBoto3()

    bucket = 'sample-bucket'
    key = 'sample-key'

    with mock.patch.object(s3_utils_module, "boto3", mock_boto3):

        s3u = s3Utils(sys_bucket='irrelevant')

        s3 = s3u.s3

        assert isinstance(s3, MockBotoS3Client)

        # An S3 file must exist for us to manipulate its tags
        s3.create_object_for_testing("irrelevant", Bucket=bucket, Key=key)

        actual = s3u.get_object_tags(key=key, bucket=bucket)
        expected = []
        # print(f"actual={actual} expected={expected}")
        assert actual == expected, f"Got {actual} but expected {expected}"

        s3u.set_object_tags(key=key, bucket=bucket, tags=[{'Key': 'a', 'Value': 'alpha'}])

        actual = s3u.get_object_tags(key=key, bucket=bucket)
        expected = [{'Key': 'a', 'Value': 'alpha'}]
        # print(f"actual={actual} expected={expected}")
        assert actual == expected, f"Got {actual} but expected {expected}"

        s3u.set_object_tags(key=key, bucket=bucket, tags=[{'Key': 'b', 'Value': 'beta'}])

        actual = s3u.get_object_tags(key=key, bucket=bucket)
        expected = [{'Key': 'a', 'Value': 'alpha'}, {'Key': 'b', 'Value': 'beta'}]
        # print(f"actual={actual} expected={expected}")
        assert actual == expected, f"Got {actual} but expected {expected}"

        s3u.set_object_tags(key=key, bucket=bucket, tags=[{'Key': 'a', 'Value': 'alpha'}], merge_existing_tags=False)

        actual = s3u.get_object_tags(key=key, bucket=bucket)
        expected = [{'Key': 'a', 'Value': 'alpha'}]
        # print(f"actual={actual} expected={expected}")
        assert actual == expected, f"Got {actual} but expected {expected}"

        s3u.set_object_tags(key=key, bucket=bucket, tags=[{'Key': 'b', 'Value': 'bravo'}], merge_existing_tags=True)

        actual = s3u.get_object_tags(key=key, bucket=bucket)
        expected = [{'Key': 'a', 'Value': 'alpha'}, {'Key': 'b', 'Value': 'bravo'}]
        # print(f"actual={actual} expected={expected}")
        assert actual == expected, f"Got {actual} but expected {expected}"

        s3u.set_object_tag(key=key, bucket=bucket, tag_key="a", tag_value="alpha_2")

        actual = s3u.get_object_tags(key=key, bucket=bucket)
        expected = [{'Key': 'a', 'Value': 'alpha_2'}, {'Key': 'b', 'Value': 'bravo'}]
        # print(f"actual={actual} expected={expected}")
        assert actual == expected, f"Got {actual} but expected {expected}"

        s3u.set_object_tag(key=key, bucket=bucket, tag_key="c", tag_value="gamma")

        actual = s3u.get_object_tags(key=key, bucket=bucket)
        expected = [{'Key': 'a', 'Value': 'alpha_2'}, {'Key': 'b', 'Value': 'bravo'}, {'Key': 'c', 'Value': 'gamma'}]
        # print(f"actual={actual} expected={expected}")
        assert actual == expected, f"Got {actual} but expected {expected}"
