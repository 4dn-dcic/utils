import datetime
import pytest

from dcicutils.s3_utils import s3Utils
from dcicutils.beanstalk_utils import compute_ff_prd_env
from dcicutils.env_utils import get_standard_mirror_env


@pytest.mark.parametrize('ff_ordinary_envname', ['fourfront-mastertest', 'fourfront-webdev', 'fourfront-hotseat'])
def test_s3Utils_creation(ff_ordinary_envname):
    util = s3Utils(env=ff_ordinary_envname)
    assert util.sys_bucket == 'elasticbeanstalk-%s-system' % ff_ordinary_envname


def test_s3Utils_creation_staging():
    print("In test_s3Utils_creation_staging. It is now", str(datetime.datetime.now()))
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
            'url': 'http://staging.4dnucleome.org',
        }
    test_stg('staging')
    # NOTE: These values should not be parameters because we don't know how long PyTest caches the
    #       parameter values before using them. By doing the test this way, we hold the value for as
    #       little time as possible, making it least risk of being stale. -kmp 10-Jul-2020
    stg_beanstalk_env = get_standard_mirror_env(compute_ff_prd_env())
    test_stg(stg_beanstalk_env)


def test_s3Utils_creation_data():
    print("In test_s3Utils_creation_data. It is now", str(datetime.datetime.now()))
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
            'url': 'https://data.4dnucleome.org',
        }
    test_prd('data')
    # NOTE: These values should not be parameters because we don't know how long PyTest caches the
    #       parameter values before using them. By doing the test this way, we hold the value for as
    #       little time as possible, making it least risk of being stale. -kmp 10-Jul-2020
    prd_beanstalk_env = compute_ff_prd_env()
    test_prd(prd_beanstalk_env)


def test_s3Utils_creation_cgap():
    print("In test_s3Utils_creation_staging. It is now", str(datetime.datetime.now()))
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
            'url': 'https://cgap.hms.harvard.edu',
        }
    test_prd('cgap')
    test_prd('fourfront-cgap')


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
    '''test for unzip_s3_to_s3 with case where there is a basdir'''
    prefix = '__test_data/extracted'
    filename = integrated_s3_info['zip_filename']
    integrated_s3_info['s3Obj'].s3_delete_dir(prefix)

    # ensure this thing was deleted
    # if no files there will be no Contents in response
    objs = integrated_s3_info['s3Obj'].s3_read_dir(prefix)
    assert [] == objs.get('Contents', [])

    # now copy to that dir we just deleted
    ret_files = integrated_s3_info['s3Obj'].unzip_s3_to_s3(filename, prefix)
    assert ret_files['fastqc_report.html']['s3key'].startswith("https://s3.amazonaws.com")

    objs = integrated_s3_info['s3Obj'].s3_read_dir(prefix)
    assert objs.get('Contents', None)


def test_unzip_s3_to_s3_2(integrated_s3_info):
    '''test for unzip_s3_to_s3 with case where there is no basdir'''
    prefix = '__test_data/extracted'
    filename = integrated_s3_info['zip_filename2']
    integrated_s3_info['s3Obj'].s3_delete_dir(prefix)

    # ensure this thing was deleted
    # if no files there will be no Contents in response
    objs = integrated_s3_info['s3Obj'].s3_read_dir(prefix)
    assert [] == objs.get('Contents', [])

    # now copy to that dir we just deleted
    ret_files = integrated_s3_info['s3Obj'].unzip_s3_to_s3(filename, prefix)
    assert ret_files['qc_report.html']['s3key'].startswith("https://s3.amazonaws.com")
    assert ret_files['qc_report.html']['s3key'].endswith("qc_report.html")

    objs = integrated_s3_info['s3Obj'].s3_read_dir(prefix)
    assert objs.get('Contents', None)
