# flake8: noqa
import boto3
import pytest
import os
import requests
import time

from dcicutils.misc_utils import check_true
from dcicutils.s3_utils import s3Utils
from dcicutils.ff_utils import authorized_request, get_health_page
from dcicutils.beanstalk_utils import describe_beanstalk_environments, REGION
from .conftest_settings import TEST_DIR


def _discover_es_health(envname):
    try:
        eb_client = boto3.client('elasticbeanstalk', region_name=REGION)
        # Calling describe_beanstalk_environments is pretty much the same as doing eb_client.describe_environments(...)
        # except it's robust against AWS throttling us for calling it too often.
        envs = describe_beanstalk_environments(eb_client, EnvironmentNames=[envname])['Environments']
        for env in envs:
            if env.get('EnvironmentName') == envname:
                cname = env.get('CNAME')
                # TODO: It would be nice if we were using https: for everything. -kmp 14-Aug-2020
                res = requests.get("http://%s/health?format=json" % cname)
                health_json = res.json()
                return health_json
    except Exception as e:
        raise RuntimeError("Unable to discover elasticsearch info for %s:\n%s: %s" % (envname, e.__class__.__name__, e))


def _discover_es_info(envname):
    discovered_health_json = _discover_es_health(envname)
    time.sleep(1)  # Reduce throttling risk
    ff_health_json = get_health_page(ff_env=envname)
    # Consistency check that both utilities are returning the same info.
    assert discovered_health_json['beanstalk_env'] == ff_health_json['beanstalk_env']
    assert discovered_health_json['elasticsearch'] == ff_health_json['elasticsearch']
    assert discovered_health_json['namespace'] == ff_health_json['namespace']
    # This should be all we actually need:
    return discovered_health_json['elasticsearch'], discovered_health_json['namespace']


# XXX: Refactor to config
INTEGRATED_ENV = 'fourfront-mastertest'
# This used to be:
# INTEGRATED_ES = 'https://search-fourfront-mastertest-wusehbixktyxtbagz5wzefffp4.us-east-1.es.amazonaws.com'
# but it changes too much, so now we discover it from the 'elasticsearch' and 'namespace' parts of health page...
INTEGRATED_ES, INTEGRATED_ES_NAMESPACE = _discover_es_info(INTEGRATED_ENV)

# We _think_ these are always the same, but maybe not. Perhaps worth noting if/when they diverge.
check_true(INTEGRATED_ENV == INTEGRATED_ES_NAMESPACE, "INTEGRATED_ENV and INTEGRATED_ES_NAMESPACE are not the same.")


@pytest.fixture(scope='session')
def basestring():
    try:
        basestring = basestring  # noQA - PyCharm static analysis doesn't understand this Python 2.7 compatibility issue
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
    integrated['es_url'] = INTEGRATED_ES
    # do this to make sure env is up (will error if not)
    res = authorized_request(integrated['ff_key']['server'],  # noQA - PyCharm fears the ['server'] part won't be there.
                             auth=integrated['ff_key'])
    if res.status_code != 200:
        raise Exception('Environment %s is not ready for integrated status. Requesting '
                        'the homepage gave status of: %s' % (INTEGRATED_ENV, res.status_code))
    return integrated


@pytest.fixture(scope='session')
def integrated_names():

    test_filename = '__test_data/test_file.txt'

    zip_filename = '__test_data/fastqc_report.zip'
    zip_filename2 = '__test_data/madqc_report.zip'

    zip_path = os.path.join(TEST_DIR, 'data_files', os.path.basename(zip_filename))
    zip_path2 = os.path.join(TEST_DIR, 'data_files', os.path.basename(zip_filename2))

    return {
        'ffenv': INTEGRATED_ENV,
        'filename': test_filename,
        # short filenames or s3 key names (informally, s3 filenames)
        'zip_filename': zip_filename,
        'zip_filename2': zip_filename2,
        # actual local filenames where the data should be
        'zip_path': zip_path,
        'zip_path2': zip_path2,
    }


@pytest.fixture(scope='session')
def integrated_s3_info(integrated_names):
    """
    Ensure the test files are present in the s3 sys bucket of the integrated
    environment (probably 'fourfront-mastertest') and return some info on them
    """
    test_filename = integrated_names['filename']

    s3_obj = s3Utils(env=INTEGRATED_ENV)
    # for now, always upload these files
    s3_obj.s3.put_object(Bucket=s3_obj.outfile_bucket, Key=test_filename,
                         Body=str.encode('thisisatest'))
    s3_obj.s3.upload_file(Filename=integrated_names['zip_path'],
                          Bucket=s3_obj.outfile_bucket,
                          Key=integrated_names['zip_filename'])
    s3_obj.s3.upload_file(Filename=integrated_names['zip_path2'],
                          Bucket=s3_obj.outfile_bucket,
                          Key=integrated_names['zip_filename2'])

    return {
        's3Obj': s3_obj,
        'filename': test_filename,
        'zip_filename': integrated_names['zip_filename'],
        'zip_filename2': integrated_names['zip_filename2'],
    }
