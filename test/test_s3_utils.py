import datetime
import io
import pytest

from dcicutils.qa_utils import ignored, override_environ
from dcicutils.s3_utils import s3Utils
from dcicutils.beanstalk_utils import compute_ff_prd_env, compute_cgap_prd_env, compute_cgap_stg_env
from dcicutils.env_utils import get_standard_mirror_env, FF_PUBLIC_URL_STG, FF_PUBLIC_URL_PRD, CGAP_PUBLIC_URL_PRD
from unittest import mock


@pytest.mark.parametrize('ff_ordinary_envname', ['fourfront-mastertest', 'fourfront-webdev', 'fourfront-hotseat'])
def test_s3Utils_creation(ff_ordinary_envname):
    util = s3Utils(env=ff_ordinary_envname)
    assert util.sys_bucket == 'elasticbeanstalk-%s-system' % ff_ordinary_envname


def test_s3Utils_creation_ff_stg():
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


def test_s3Utils_creation_ff_prd():
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


def test_s3Utils_creation_cgap_prd():
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


def test_s3Utils_creation_cgap_stg():
    print("In test_s3Utils_creation_cgap_prd. It is now", str(datetime.datetime.now()))
    # For now there is no CGAP stg...
    assert compute_cgap_stg_env() is None, "There seems to be a CGAP staging environment. Tests need updating."


@pytest.mark.parametrize('ordinary_envname', ['fourfront-mastertest', 'fourfront-webdev',
                                              'fourfront-cgaptest', 'fourfront-cgapdev', 'fourfront-cgapwolf'])
def test_s3Utils_creation(ordinary_envname):
    util = s3Utils(env=ordinary_envname)
    assert util.sys_bucket == 'elasticbeanstalk-%s-system' % ordinary_envname


def test_s3Utils_get_keys_for_data():
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


def test_s3Utils_get_keys_for_staging():
    util = s3Utils(env='staging')
    keys = util.get_ff_key()
    assert keys['server'] == 'http://staging.4dnucleome.org'


def test_s3Utils_get_jupyterhub_key(basestring):
    util = s3Utils(env='data')
    key = util.get_jupyterhub_key()
    assert 'secret' in key
    assert key['server'] == 'https://jupyter.4dnucleome.org'


def test_s3Utils_get_higlass_key():
    util = s3Utils(env='staging')
    keys = util.get_higlass_key()
    assert isinstance(keys, dict)
    assert 3 == len(keys.keys())


def test_s3Utils_get_google_key():
    util = s3Utils(env='staging')
    keys = util.get_google_key()
    assert isinstance(keys, dict)
    assert keys['type'] == 'service_account'
    assert keys["project_id"] == "fourdn-fourfront"
    for dict_key in ['private_key_id', 'private_key', 'client_email', 'client_id', 'auth_uri', 'client_x509_cert_url']:
        assert keys[dict_key]


def test_s3Utils_get_access_keys_with_old_style_default():
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


def test_s3Utils_get_key_non_json_data():

    util = s3Utils(env='fourfront-mastertest')

    non_json_string = '1 { 2 3 >'

    with mock.patch.object(util.s3, "get_object") as mock_get_object:
        mock_get_object.return_value = {'Body': io.BytesIO(bytes(non_json_string, encoding='utf-8'))}
        assert util.get_key() == non_json_string

    with mock.patch.object(util.s3, "get_object") as mock_get_object:
        mock_get_object.return_value = {'Body': io.StringIO(non_json_string)}
        assert util.get_key() == non_json_string


def test_s3Utils_delete_key():

    sample_key_name = "--- reserved_key_name_for_unit_testing ---"

    util = s3Utils(env='fourfront-mastertest')

    with mock.patch.object(util.s3, "delete_object") as mock_delete_object:

        def make_mocked_delete_object(expected_bucket, expected_key):

            def mocked_delete_object(Bucket, Key):
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


def test_s3Utils_s3_put():

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


def test_s3Utils_s3_put_secret():

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


def test_does_key_exist():
    """ Use staging to check for non-existant key """
    util = s3Utils(env='staging')
    assert not util.does_key_exist('not_a_key')


def test_read_s3(integrated_s3_info):
    read = integrated_s3_info['s3Obj'].read_s3(integrated_s3_info['filename'])
    assert read.strip() == b'thisisatest'


def test_get_file_size(integrated_s3_info):
    size = integrated_s3_info['s3Obj'].get_file_size(integrated_s3_info['filename'])
    assert size == 11
    with pytest.raises(Exception) as exec_info:
        integrated_s3_info['s3Obj'].get_file_size('not_a_file')
    assert 'not found' in str(exec_info.value)


def test_size(integrated_s3_info):
    """ Get size of non-existent, real bucket """
    bucket = integrated_s3_info['s3Obj'].sys_bucket
    sz = integrated_s3_info['s3Obj'].size(bucket)
    assert sz > 0
    with pytest.raises(Exception) as exec_info:
        integrated_s3_info['s3Obj'].size('not_a_bucket')
    assert 'NoSuchBucket' in str(exec_info.value)


def test_get_file_size_in_bg(integrated_s3_info):
    size = integrated_s3_info['s3Obj'].get_file_size(integrated_s3_info['filename'],
                                                     add_gb=2, size_in_gb=True)
    assert int(size) == 2


def test_read_s3_zip(integrated_s3_info):
    filename = integrated_s3_info['zip_filename']
    files = integrated_s3_info['s3Obj'].read_s3_zipfile(filename, ['summary.txt', 'fastqc_data.txt'])
    assert files['summary.txt']
    assert files['fastqc_data.txt']
    assert files['summary.txt'].startswith(b'PASS')


def test_unzip_s3_to_s3(integrated_s3_info):
    """test for unzip_s3_to_s3 with case where there is a basdir"""

    prefix = '__test_data/extracted'
    filename = integrated_s3_info['zip_filename']
    s3_connection = integrated_s3_info['s3Obj']

    # start with a clean test space
    s3_connection.s3_delete_dir(prefix)

    # ensure this thing was deleted
    # if no files there will be no Contents in response
    objs = s3_connection.s3_read_dir(prefix)
    assert not objs.get('Contents')

    # now copy to that dir we just deleted
    ret_files = s3_connection.unzip_s3_to_s3(filename, prefix)
    assert ret_files['fastqc_report.html']['s3key'].startswith("https://s3.amazonaws.com")

    objs = s3_connection.s3_read_dir(prefix)
    assert objs.get('Contents')


def test_unzip_s3_to_s3_2(integrated_s3_info):
    """test for unzip_s3_to_s3 with case where there is no basdir"""

    prefix = '__test_data/extracted'
    filename = integrated_s3_info['zip_filename2']
    s3_connection = integrated_s3_info['s3Obj']

    s3_connection.s3_delete_dir(prefix)

    # ensure this thing was deleted
    # if no files there will be no Contents in response
    objs = s3_connection.s3_read_dir(prefix)
    assert not objs.get('Contents')

    # now copy to that dir we just deleted
    ret_files = s3_connection.unzip_s3_to_s3(filename, prefix)
    assert ret_files['qc_report.html']['s3key'].startswith("https://s3.amazonaws.com")
    assert ret_files['qc_report.html']['s3key'].endswith("qc_report.html")

    objs = s3_connection.s3_read_dir(prefix)
    assert objs.get('Contents')


def test_unzip_s3_to_s3_store_results(integrated_s3_info):
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
