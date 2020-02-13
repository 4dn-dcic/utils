# flake8: noqa
import pytest
import os
from dcicutils.s3_utils import s3Utils
from dcicutils.ff_utils import authorized_request

# this is the ff_env we use for integrated tests
INTEGRATED_ENV = 'fourfront-mastertest'


@pytest.fixture(scope='session')
def basestring():
    try:
        basestring = basestring
    except NameError:
        basestring = str
    return basestring


@pytest.fixture(scope='session')
def integrated_ff():
    """
    Object that contains keys and ff_env for integrated environment
    """
    integrated = {}
    s3 = s3Utils(env=INTEGRATED_ENV)
    integrated['ff_key'] = s3.get_access_keys()
    integrated['higlass_key'] = s3.get_higlass_key()
    integrated['ff_env'] = INTEGRATED_ENV
    # XXX: Read from config? Somewhere else?
    integrated['es_url'] = 'https://search-fourfront-mastertest-wusehbixktyxtbagz5wzefffp4.us-east-1.es.amazonaws.com'
    # do this to make sure env is up (will error if not)
    res = authorized_request(integrated['ff_key']['server'], auth=integrated['ff_key'])
    if res.status_code != 200:
        raise Exception('Environment %s is not ready for integrated status. Requesting '
                        'the homepage gave status of: %s' % (INTEGRATED_ENV, res.status_code))
    return integrated


@pytest.fixture(scope='session')
def integrated_s3_info():
    """
    Ensure the test files are present in the s3 sys bucket of the integrated
    environment (probably 'fourfront-mastertest') and return some info on them
    """
    test_filename = '__test_data/test_file.txt'
    zip_filename = '__test_data/fastqc_report.zip'
    s3Obj = s3Utils(env=INTEGRATED_ENV)
    # for now, always upload these files
    s3Obj.s3.put_object(Bucket=s3Obj.sys_bucket, Key=test_filename,
                          Body=str.encode('thisisatest'))
    zip_path = os.path.join('test', 'data_files', zip_filename.split('/')[-1])
    s3Obj.s3.upload_file(Filename=str(zip_path), Bucket=s3Obj.sys_bucket, Key=zip_filename)

    return {'s3Obj': s3Obj, 'filename': test_filename, 'zip_filename': zip_filename}
