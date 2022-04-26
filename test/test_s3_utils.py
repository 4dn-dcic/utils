import contextlib
import datetime
import io
import json
import os
import pytest
import requests

from botocore.exceptions import ClientError
from dcicutils import s3_utils as s3_utils_module, beanstalk_utils
from dcicutils.beanstalk_utils import compute_ff_prd_env, compute_cgap_prd_env, compute_cgap_stg_env
from dcicutils.env_utils import (
    get_standard_mirror_env,
    FF_PUBLIC_URL_STG, FF_PUBLIC_URL_PRD, _CGAP_MGB_PUBLIC_URL_PRD
)
from dcicutils.exceptions import SynonymousEnvironmentVariablesMismatched, CannotInferEnvFromManyGlobalEnvs
from dcicutils.misc_utils import ignored, ignorable
from dcicutils.qa_utils import override_environ, MockBoto3, MockBotoS3Client, MockResponse, known_bug_expected
from dcicutils.s3_utils import s3Utils, EnvManager
from unittest import mock


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
            # so we have to pre-load to our mock S3 manually. -kmp 13-Jan-2021
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


@pytest.mark.integrated
def test_regression_s3_utils_short_name_c4_706():

    # Environment long names work (at least in legacy CGAP)
    s3Utils(env="fourfront-mastertest")

    with known_bug_expected(jira_ticket="C4-706", fixed=True, error_class=ClientError):
        # Sort names not allowed.
        s3Utils(env="mastertest")


def _env_is_up_and_healthy(env):
    env_url = beanstalk_utils.get_beanstalk_real_url(env)
    health_page_url = f"{env_url}/health?format=json"
    return requests.get(health_page_url).status_code == 200


@pytest.mark.integrated
@pytest.mark.parametrize('ff_ordinary_envname', ['fourfront-mastertest', 'fourfront-webdev', 'fourfront-hotseat'])
def test_s3utils_creation_ff_ordinary(ff_ordinary_envname):
    if _env_is_up_and_healthy(ff_ordinary_envname):
        util = s3Utils(env=ff_ordinary_envname)
        assert util.sys_bucket == 'elasticbeanstalk-%s-system' % ff_ordinary_envname
    else:
        pytest.skip(f"Health page for {ff_ordinary_envname} is unavailable, so test is being skipped.")


@pytest.mark.integrated
def test_s3utils_creation_ff_stg():
    # TODO: I'm not sure what this is testing, so it's hard to rewrite
    #   But I fear this use of env 'staging' implies the GA test environment has overbroad privilege.
    #   We should make this work without access to 'staging'.
    #   -kmp 13-Jan-2021
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
            'sys_bucket': 'elasticbeanstalk-fourfront-webprod-system',
            'outfile_bucket': 'elasticbeanstalk-fourfront-webprod-wfoutput',
            'raw_file_bucket': 'elasticbeanstalk-fourfront-webprod-files',
            'url': FF_PUBLIC_URL_STG,
        }

    test_stg('staging')
    # NOTE: These values should not be parameters because we don't know how long PyTest caches the
    #       parameter values before using them. By doing the test this way, we hold the value for as
    #       little time as possible, making it least risk of being stale. -kmp 10-Jul-2020
    stg_beanstalk_env = get_standard_mirror_env(compute_ff_prd_env())
    test_stg(stg_beanstalk_env)


@pytest.mark.integrated
def test_s3utils_creation_ff_prd():
    # TODO: I'm not sure what this is testing, so it's hard to rewrite
    #   But I fear this use of env 'data' implies the GA test environment has overbroad privilege.
    #   We should make this work without access to 'data'.
    #   -kmp 13-Jan-2021
    print("In test_s3Utils_creation_ff_prd. It is now", str(datetime.datetime.now()))

    def test_prd(ff_production_envname):
        util = s3Utils(env=ff_production_envname)
        actual_props = {
            'sys_bucket': util.sys_bucket,
            'outfile_bucket': util.outfile_bucket,
            'raw_file_bucket': util.raw_file_bucket,
            'url': util.url,
        }
        assert actual_props == {
            'sys_bucket': 'elasticbeanstalk-fourfront-webprod-system',
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
# # 'fourfront-cgapdev' has been decommissioned.
# def test_s3utils_creation_cgap_ordinary(cgap_ordinary_envname):
#     util = s3Utils(env=cgap_ordinary_envname)
#     assert util.sys_bucket == 'elasticbeanstalk-%s-system' % cgap_ordinary_envname


@pytest.mark.integrated
def test_s3utils_creation_cgap_prd():
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


@pytest.mark.integrated
def test_s3utils_creation_cgap_stg():
    print("In test_s3Utils_creation_cgap_prd. It is now", str(datetime.datetime.now()))
    # For now there is no CGAP stg...
    assert compute_cgap_stg_env() is None, "There seems to be a CGAP staging environment. Tests need updating."


@pytest.mark.integrated
def test_s3utils_get_keys_for_data():
    util = s3Utils(env='data')
    keys = util.get_access_keys()
    assert keys['server'] == 'https://data.4dnucleome.org'
    # make sure we have keys for foursight and tibanna as well
    keys_tb = util.get_access_keys('access_key_tibanna')
    assert keys_tb['key'] != keys['key']
    assert keys_tb['server'] == keys['server']
    keys_fs = util.get_access_keys('access_key_foursight')
    assert keys_fs['key'] != keys_tb['key'] != keys['key']
    assert keys_fs['server'] == keys['server']


@pytest.mark.integrated
def test_s3utils_get_keys_for_staging():
    # TODO: I'm not sure what this is testing, so it's hard to rewrite
    #   But I fear this use of env 'staging' implies the GA test environment has overbroad privilege.
    #   We should make this work without access to 'staging'.
    #   -kmp 13-Jan-2021
    util = s3Utils(env='staging')
    keys = util.get_ff_key()
    assert keys['server'] == 'http://staging.4dnucleome.org'


@pytest.mark.integrated
def test_s3utils_get_jupyterhub_key():
    # TODO: I'm not sure what this is testing, so it's hard to rewrite
    #   But I fear this use of env 'data' implies the GA test environment has overbroad privilege.
    #   We should make this work without access to 'data'.
    #   -kmp 13-Jan-2021
    util = s3Utils(env='data')
    key = util.get_jupyterhub_key()
    assert 'secret' in key
    assert key['server'] == 'https://jupyter.4dnucleome.org'


@pytest.mark.integrated
def test_s3utils_get_higlass_key_integrated():
    # TODO: I'm not sure what this is testing, so it's hard to rewrite
    #   But I fear this use of env 'staging' implies the GA test environment has overbroad privilege.
    #   We should make this work without access to 'staging'.
    #   -kmp 13-Jan-2021
    util = s3Utils(env='staging')
    keys = util.get_higlass_key()
    assert isinstance(keys, dict)
    assert 3 == len(keys.keys())


def test_s3utils_get_google_key():
    util = s3Utils(env='staging')
    keys = util.get_google_key()
    assert isinstance(keys, dict)
    assert keys['type'] == 'service_account'
    assert keys["project_id"] == "fourdn-fourfront"
    for dict_key in ['private_key_id', 'private_key', 'client_email', 'client_id', 'auth_uri', 'client_x509_cert_url']:
        assert keys[dict_key]


@pytest.mark.unit
def test_s3utils_get_access_keys_with_old_style_default():
    util = s3Utils(env='fourfront-mastertest')
    with mock.patch.object(util, "get_key") as mock_get_key:
        actual_key = {'key': 'some-key', 'server': 'some-server'}

        def mocked_get_key(keyfile_name):
            ignored(keyfile_name)
            key_wrapper = {'default': actual_key}
            return key_wrapper

        mock_get_key.side_effect = mocked_get_key
        key = util.get_access_keys()
        assert key == actual_key


@pytest.mark.unit
def test_s3utils_get_key_non_json_data():

    util = s3Utils(env='fourfront-mastertest')

    non_json_string = '1 { 2 3 >'

    with mock.patch.object(util.s3, "get_object") as mock_get_object:
        mock_get_object.return_value = {'Body': io.BytesIO(bytes(non_json_string, encoding='utf-8'))}
        assert util.get_key() == non_json_string

    with mock.patch.object(util.s3, "get_object") as mock_get_object:
        mock_get_object.return_value = {'Body': io.StringIO(non_json_string)}
        assert util.get_key() == non_json_string


@pytest.mark.unit
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
                "Body": item,
                "Bucket": util.outfile_bucket,
                "Key": some_key,
                "ContentType": some_content_type,
            }
            some_acl = 'some-acl'
            assert util.s3_put(item, upload_key=some_key, acl=some_acl) == {
                "Body": item,
                "Bucket": util.outfile_bucket,
                "Key": some_key,
                "ContentType": some_content_type,
                "ACL": some_acl,
            }


@pytest.mark.unit
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
                "Body": item,
                "Bucket": util.sys_bucket,
                "Key": some_key,
                "SSECustomerKey": environmental_key,
                "SSECustomerAlgorithm": standard_algorithm,
            }
            some_bucket = 'some-bucket'
            assert util.s3_put_secret(item, keyname=some_key, bucket=some_bucket) == {
                "Body": item,
                "Bucket": some_bucket,
                "Key": some_key,
                "SSECustomerKey": environmental_key,
                "SSECustomerAlgorithm": standard_algorithm,
            }
            assert util.s3_put_secret(item, keyname=some_key, secret=some_secret) == {
                "Body": item,
                "Bucket": util.sys_bucket,
                "Key": some_key,
                "SSECustomerKey": some_secret,
                "SSECustomerAlgorithm": standard_algorithm,
            }


@pytest.mark.integratedx
def test_does_key_exist_integrated():
    """ Use staging to check for non-existant key """
    util = s3Utils(env='staging')
    assert not util.does_key_exist('not_a_key')


@pytest.mark.unit
def test_does_key_exist_unit(integrated_names):
    """ Use staging to check for non-existant key """

    with mocked_s3_integration(integrated_names=integrated_names) as s3_connection:

        assert not s3_connection.does_key_exist('not_a_key')


@pytest.mark.integratedx
def test_read_s3_integrated(integrated_s3_info):
    read = integrated_s3_info['s3Obj'].read_s3(integrated_s3_info['filename'])
    assert read.strip() == b'thisisatest'


@pytest.mark.unit
def test_read_s3_unit(integrated_names):

    with mocked_s3_integration(integrated_names=integrated_names) as s3_connection:

        read = s3_connection.read_s3(integrated_names['filename'])
        assert read.strip() == b'thisisatest'


@pytest.mark.integratedx
def test_get_file_size_integrated(integrated_s3_info):
    size = integrated_s3_info['s3Obj'].get_file_size(integrated_s3_info['filename'])
    assert size == 11
    with pytest.raises(Exception) as exec_info:
        integrated_s3_info['s3Obj'].get_file_size('not_a_file')
    assert 'not found' in str(exec_info.value)


@pytest.mark.unit
def test_get_file_size_unit(integrated_names):

    with mocked_s3_integration(integrated_names=integrated_names) as s3_connection:

        size = s3_connection.get_file_size(integrated_names['filename'])
        assert size == 11
        with pytest.raises(Exception) as exec_info:
            s3_connection.get_file_size('not_a_file')
        assert 'not found' in str(exec_info.value)


@pytest.mark.integratedx
def test_size_integrated(integrated_s3_info):
    """ Get size of non-existent, real bucket """
    bucket = integrated_s3_info['s3Obj'].sys_bucket
    sz = integrated_s3_info['s3Obj'].size(bucket)
    assert sz > 0
    with pytest.raises(Exception) as exec_info:
        integrated_s3_info['s3Obj'].size('not_a_bucket')
    assert 'NoSuchBucket' in str(exec_info.value)


@pytest.mark.unit
def test_size_unit(integrated_names):
    """ Get size of non-existent, real bucket """

    with mocked_s3_integration(integrated_names=integrated_names) as s3_connection:

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
def test_get_file_size_in_gb_integrated(integrated_s3_info):

    s3_connection = integrated_s3_info['s3Obj']

    size = s3_connection.get_file_size(integrated_s3_info['filename'],
                                       add_gb=2, size_in_gb=True)
    assert int(size) == 2


@pytest.mark.unit
def test_get_file_size_in_gb_unit(integrated_names):

    with mocked_s3_integration(integrated_names=integrated_names) as s3_connection:

        size = s3_connection.get_file_size(integrated_names['filename'],
                                           add_gb=2, size_in_gb=True)
        assert int(size) == 2


@pytest.mark.integratedx
def test_read_s3_zip_integrated(integrated_s3_info):
    filename = integrated_s3_info['zip_filename']
    files = integrated_s3_info['s3Obj'].read_s3_zipfile(filename, ['summary.txt', 'fastqc_data.txt'])
    assert files['summary.txt']
    assert files['fastqc_data.txt']
    assert files['summary.txt'].startswith(b'PASS')


@pytest.mark.unit
def test_read_s3_zip_unit(integrated_names):

    with mocked_s3_integration(integrated_names=integrated_names) as s3_connection:

        filename = integrated_names['zip_filename']
        files = s3_connection.read_s3_zipfile(filename, ['summary.txt', 'fastqc_data.txt'])
        assert files['summary.txt']
        assert files['fastqc_data.txt']
        assert files['summary.txt'].startswith(b'PASS')


@pytest.mark.integratedx
@pytest.mark.parametrize("suffix, expected_report", [("", "fastqc_report.html"), ("2", "qc_report.html")])
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
def test_unzip_s3_to_s3_unit(integrated_names, suffix, expected_report):
    """test for unzip_s3_to_s3 with case where there is no basdir"""

    with mocked_s3_integration(integrated_names=integrated_names, zip_suffix=suffix) as s3_connection:

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
def test_unzip_s3_to_s3_store_results_unit(integrated_names):
    """test for unzip_s3_to_s3 with case where there is a basdir and store_results=False"""

    with mocked_s3_integration(integrated_names=integrated_names) as s3_connection:

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


def test_s3_utils_buckets_modern():

    env_name = 'fourfront-cgapfoo'

    es_server_short = "some-es-server.com:443"
    es_server_https = "https://some-es-server.com:443"

    with mock.patch("boto3.client"):
        with mock.patch.object(s3_utils_module.EnvManager, "fetch_health_page_json") as mock_fetch:
            mock_fetch.return_value = {
                "elasticsearch": es_server_short,
                "system_bucket": "the-system-bucket",
                "processed_file_bucket": "the-output-file-bucket",
                "file_upload_bucket": "the-raw-file-bucket",
                "blob-bucket": "the-blob-bucket",
                "metadata_bundles_bucket": "the-metadata-bundles-bucket",
                "tibanna_cwls_bucket": "the-tibanna-cwls-bucket",
                "tibanna_output_bucket": "the-tibanna-output-bucket",
                "s3_encrypt_key_id": "my-encrypt-key",
            }
            s = s3Utils(env=env_name)
            assert s.outfile_bucket != 'the-output-file-bucket'
            assert s.sys_bucket != 'the-system-bucket'
            assert s.raw_file_bucket != 'the-raw-file-bucket'
            assert s.blob_bucket != 'the-blog-bucket'
            assert s.metadata_bucket != 'the-metadata-bundles-bucket'
            assert s.tibanna_cwls_bucket != 'the-tibanna-cwls-bucket'
            assert s.tibanna_output_bucket != 'the-tibanna-output-bucket'

            assert s.outfile_bucket == 'elasticbeanstalk-fourfront-cgapfoo-wfoutput'
            assert s.sys_bucket == 'elasticbeanstalk-fourfront-cgapfoo-system'
            assert s.raw_file_bucket == 'elasticbeanstalk-fourfront-cgapfoo-files'
            assert s.blob_bucket == 'elasticbeanstalk-fourfront-cgapfoo-blobs'
            assert s.metadata_bucket == 'elasticbeanstalk-fourfront-cgapfoo-metadata-bundles'
            assert s.tibanna_cwls_bucket == 'tibanna-cwls'
            assert s.tibanna_output_bucket == 'tibanna-output'

            assert s.s3_encrypt_key_id == 'my-encrypt-key'

            e = s.env_manager

            assert e.s3 == s.s3
            # This mock is not elaborate enough for testing how e.portal_url is set up.
            # assert e.portal_url = ...
            assert e.es_url == es_server_https
            assert e.env_name == env_name


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


def test_s3_utils_fetch_health_page_json():

    with mock.patch.object(EnvManager, "fetch_health_page_json") as mock_implementation:

        def mocked_implementation(url, use_urllib):
            assert url == 'dummy-url'
            assert use_urllib == 'dummy-use-urllib'

        mock_implementation.side_effect = mocked_implementation

        s3Utils.fetch_health_page_json(url='dummy-url', use_urllib='dummy-use-urllib')
        s3Utils.fetch_health_page_json(use_urllib='dummy-use-urllib', url='dummy-url')
        s3Utils.fetch_health_page_json('dummy-url', 'dummy-use-urllib')


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


def test_env_manager():

    class MyS3(MockBotoS3Client):
        MOCK_STATIC_FILES = {
            # Bucket 'global-env-1'
            'global-env-1/cgap-footest':
                '{"fourfront": "http://portal", "es": "http://es", "ff_env": "cgap-footest"}',
            # Bucket 'global-env-2'
            'global-env-2/cgap-footest':
                '{"fourfront": "http://portal-foo", "es": "http://es-foo", "ff_env": "cgap-footest"}',
            'global-env-2/cgap-bartest':
                '{"fourfront": "http://portal-bar", "es": "http://es-bar", "ff_env": "cgap-bartest"}',
        }

    with mock.patch.object(s3_utils_module, "boto3", MockBoto3(s3=MyS3)):

        my_s3 = s3_utils_module.boto3.client('s3')

        with EnvManager.global_env_bucket_named(name='global-env-1'):

            e = EnvManager(s3=my_s3)
            assert e.portal_url == "http://portal"
            assert e.es_url == "http://es"
            assert e.env_name == "cgap-footest"


def test_env_manager_verify_and_get_env_config():

    class MyS3(MockBotoS3Client):
        MOCK_STATIC_FILES = {
            # Bucket 'global-env-1'
            'global-env-1/cgap-footest':
                '{"fourfront": "http://portal", "es": "http://es", "ff_env": "cgap-footest"}',
            # Bucket 'global-env-2'
            'global-env-2/cgap-footest':
                '{"fourfront": "http://portal-foo", "es": "http://es-foo", "ff_env": "cgap-footest"}',
            'global-env-2/cgap-bartest':
                '{"fourfront": "http://portal-bar", "es": "http://es-bar", "ff_env": "cgap-bartest"}',
        }

    with mock.patch.object(s3_utils_module, "boto3", MockBoto3(s3=MyS3)):

        my_s3 = s3_utils_module.boto3.client('s3')

        # Note here we specified the env explicitly.
        config = EnvManager.verify_and_get_env_config(s3_client=my_s3, global_bucket='global-env-1', env='cgap-footest')

        assert config['fourfront'] == 'http://portal'
        assert config['es'] == 'http://es'
        assert config['ff_env'] == 'cgap-footest'

        env_manager_from_desc = EnvManager.compose(portal_url='http://portal',
                                                   es_url="http://es",
                                                   env_name='cgap-footest',
                                                   s3=my_s3)

        assert env_manager_from_desc.env_description == config
        assert env_manager_from_desc.env_description['fourfront'] == 'http://portal'
        assert env_manager_from_desc.env_description['es'] == 'http://es'
        assert env_manager_from_desc.env_description['ff_env'] == 'cgap-footest'
        assert env_manager_from_desc.portal_url == 'http://portal'
        assert env_manager_from_desc.es_url == 'http://es'
        assert env_manager_from_desc.env_name == 'cgap-footest'

        config = EnvManager.verify_and_get_env_config(s3_client=my_s3, global_bucket='global-env-1',
                                                      # Env unspecified, but there's only one, so it'll be inferred.
                                                      env=None)

        assert config['fourfront'] == 'http://portal'
        assert config['es'] == 'http://es'
        assert config['ff_env'] == 'cgap-footest'

        # The next tests are similar to the above, but in an S3 global bucket env (global-env-2) that has more than
        # one environment, so the env cannot default.

        config = EnvManager.verify_and_get_env_config(s3_client=my_s3, global_bucket='global-env-2', env='cgap-footest')

        assert config['fourfront'] == 'http://portal-foo'
        assert config['es'] == 'http://es-foo'
        assert config['ff_env'] == 'cgap-footest'

        config = EnvManager.verify_and_get_env_config(s3_client=my_s3, global_bucket='global-env-2', env='cgap-bartest')

        assert config['fourfront'] == 'http://portal-bar'
        assert config['es'] == 'http://es-bar'
        assert config['ff_env'] == 'cgap-bartest'

        with pytest.raises(CannotInferEnvFromManyGlobalEnvs):
            EnvManager.verify_and_get_env_config(s3_client=my_s3, global_bucket='global-env-2',
                                                 # Env unspecified, but alas ambiguous, so no defaulting can occur.
                                                 env=None)


def test_env_manager_global_env_bucket_name():

    # These tests expect to be run in an environment that does not have these buckets bound globally.
    assert os.environ.get('GLOBAL_ENV_BUCKET') is None
    assert os.environ.get('GLOBAL_BUCKET_ENV') is None

    with EnvManager.global_env_bucket_named(name='foo'):

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
