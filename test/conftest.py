# flake8: noqa
import pytest
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
    # do this to make sure env is up (will error if not)
    res = authorized_request(integrated['ff_key']['server'], auth=integrated['ff_key'])
    if res.status_code != 200:
        raise Exception('Environment %s is not ready for integrated status. Requesting '
                        'the homepage gave status of: %s' % (INTEGRATED_ENV, res.status_code))
    return integrated

   
@pytest.fixture(scope='session')
def used_env():
    return 'fourfront-webdev'


@pytest.fixture(scope='session')
def s3_utils(used_env):
    return s3Utils(env=used_env)
