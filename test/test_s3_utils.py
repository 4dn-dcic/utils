from dcicutils.s3_utils import s3Utils


def test_s3Utils_creation():
    util = s3Utils(env='fourfront-mastertest')
    assert util.sys_bucket == 'elasticbeanstalk-fourfront-mastertest-system'


def test_s3Utils_creation_staging():
    util = s3Utils(env='staging')
    assert util.sys_bucket == 'elasticbeanstalk-fourfront-webprod-system'
    assert util.outfile_bucket == 'elasticbeanstalk-fourfront-webprod-wfoutput'
    assert util.raw_file_bucket == 'elasticbeanstalk-fourfront-webprod-files'
    assert util.url == 'http://staging.4dnucleome.org'


def test_s3Utils_creation_data():
    util = s3Utils(env='data')
    assert util.sys_bucket == 'elasticbeanstalk-fourfront-webprod-system'
    assert util.outfile_bucket == 'elasticbeanstalk-fourfront-webprod-wfoutput'
    assert util.raw_file_bucket == 'elasticbeanstalk-fourfront-webprod-files'
    assert util.url == 'https://data.4dnucleome.org'


def test_s3Utils_get_keys_for_data():
    util = s3Utils(env='data')
    keys = util.get_access_keys()
    assert keys['server'] == 'https://data.4dnucleome.org'


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


def test_read_s3(integrated_s3_info):
    read = integrated_s3_info['s3Obj'].read_s3(integrated_s3_info['filename'])
    assert read.strip() == b'thisisatest'


def test_get_file_size(integrated_s3_info):
    size = integrated_s3_info['s3Obj'].get_file_size(integrated_s3_info['filename'])
    assert size == 12


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
    prefix = '__test_data/extracted'
    filename = integrated_s3_info['zip_filename']
    integrated_s3_info['s3Obj'].s3_delete_dir(prefix)

    # ensure this thing was deleted
    # if no files there will be no Contents in response
    objs = integrated_s3_info['s3Obj'].s3_read_dir(prefix)
    assert [] == objs.get('Contents', [])

    # now copy to that dir we just deleted
    retfile_list = ['summary.txt', 'fastqc_data.txt', 'fastqc_report.html']
    ret_files = integrated_s3_info['s3Obj'].unzip_s3_to_s3(filename, prefix, retfile_list)
    assert 3 == len(ret_files.keys())
    assert ret_files['fastqc_report.html']['s3key'].startswith("https://s3.amazonaws.com")

    objs = integrated_s3_info['s3Obj'].s3_read_dir(prefix)
    assert objs.get('Contents', None)
