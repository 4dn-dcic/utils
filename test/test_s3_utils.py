import contextlib
import datetime
import io
import pytest

from dcicutils import s3_utils as s3_utils_module
from dcicutils.misc_utils import ignored, ignorable
from dcicutils.qa_utils import override_environ, MockBoto3
from dcicutils.s3_utils import s3Utils
from dcicutils.beanstalk_utils import compute_ff_prd_env, compute_cgap_prd_env, compute_cgap_stg_env
from dcicutils.env_utils import get_standard_mirror_env, FF_PUBLIC_URL_STG, FF_PUBLIC_URL_PRD, CGAP_PUBLIC_URL_PRD
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


@pytest.mark.integrated
@pytest.mark.parametrize('ff_ordinary_envname', ['fourfront-mastertest', 'fourfront-webdev', 'fourfront-hotseat'])
def test_s3utils_creation_ff_ordinary(ff_ordinary_envname):
    util = s3Utils(env=ff_ordinary_envname)
    assert util.sys_bucket == 'elasticbeanstalk-%s-system' % ff_ordinary_envname


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


@pytest.mark.integrated
@pytest.mark.parametrize('cgap_ordinary_envname', ['fourfront-cgaptest', 'fourfront-cgapdev', 'fourfront-cgapwolf'])
def test_s3utils_creation_cgap_ordinary(cgap_ordinary_envname):
    util = s3Utils(env=cgap_ordinary_envname)
    assert util.sys_bucket == 'elasticbeanstalk-%s-system' % cgap_ordinary_envname


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
            'url': CGAP_PUBLIC_URL_PRD,
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
def test_s3utils_get_jupyterhub_key(basestring):
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


# From https://hms-dbmi.atlassian.net/browse/C4-674
# To be a viable test, this will need some mocking.
#
# import os
#
# def test_s3_utils_legacy_behavior():
#     os.environ['GLOBAL_BUCKET_ENV'] = os.environ['GLOBAL_ENV_BUCKET'] = 'foursight-cgap-mastertest-envs'
#     s3Utils('application-cgap-mastertest-wfout',
#             'application-cgap-mastertest-wfout',
#             'application-cgap-mastertest-wfout')
