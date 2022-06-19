import os
import pytest
import requests

from dcicutils.common import LEGACY_GLOBAL_ENV_BUCKET
from dcicutils.env_utils import EnvUtils
from dcicutils.ff_utils import authorized_request
from dcicutils.lang_utils import conjoined_list
from dcicutils.s3_utils import s3Utils
from .conftest_settings import TEST_DIR, INTEGRATED_ENV, INTEGRATED_ENV_INDEX_NAMESPACE, INTEGRATED_ENV_PORTAL_URL


def _portal_health_get(namespace, portal_url, key):
    healh_json_url = f"{portal_url}/health?format=json"
    response = requests.get(healh_json_url)
    health_json = response.json()
    assert health_json['namespace'] == namespace  # check we're talking to proper host
    return health_json[key]


# We used to wire in this URL, but it's better to discover it dynamically
# so that it can change.
# INTEGRATED_ES = _discover_es_url_from_boto3_eb_metadata(INTEGRATED_ENV)

INTEGRATED_ES = _portal_health_get(portal_url=INTEGRATED_ENV_PORTAL_URL,
                                   namespace=INTEGRATED_ENV_INDEX_NAMESPACE,
                                   key="elasticsearch")

os.environ['GLOBAL_ENV_BUCKET'] = LEGACY_GLOBAL_ENV_BUCKET
os.environ['ENV_NAME'] = INTEGRATED_ENV

EnvUtils.init(force=True)  # This would be a good time to force EnvUtils to synchronize with the real environment


class IntegratedFixture:
    """
    A class that implements the integrated_ff fixture.
    Implementing this as a class assures that the initialization is done at toplevel before any mocking occurs.
    """

    S3_CLIENT = s3Utils(env=INTEGRATED_ENV)
    ENV_NAME = INTEGRATED_ENV
    ENV_INDEX_NAMESPACE = INTEGRATED_ENV_INDEX_NAMESPACE
    ES_URL = INTEGRATED_ES
    PORTAL_ACCESS_KEY = S3_CLIENT.get_access_keys()
    HIGLASS_ACCESS_KEY = S3_CLIENT.get_higlass_key()

    INTEGRATED_FF_ITEMS = {
        'ff_key': PORTAL_ACCESS_KEY,
        'higlass_key': HIGLASS_ACCESS_KEY,
        'ff_env': ENV_NAME,
        'ff_env_index_namespace': ENV_INDEX_NAMESPACE,
        'es_url': ES_URL,
    }

    @classmethod
    def verify_portal_access(cls, portal_access_key):
        response = authorized_request(
            portal_access_key['server'],
            auth=portal_access_key)
        if response.status_code != 200:
            raise Exception(f'Environment {cls.ENV_NAME} is not ready for integrated status.'
                            f' Requesting the homepage gave status of: {response.status_code}')

    def __init__(self, name):
        self.name = name

    def __str__(self):
        """
        Print is object as a dictionary with credentials (entries with 'key' in their name) redacted.
        A dictionary pseudo-element 'self' describes this object itself.
        """
        entries = ', '.join([f'{key!r}: {"<redacted>" if "key" in key else repr(self.INTEGRATED_FF_ITEMS[key])}'
                               for key in self.INTEGRATED_FF_ITEMS])
        return f"{{'self': <{self.__class__.__name__} {self.name!r} {id(self)}>, {entries}}}"

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name!r})"

    def __getitem__(self, item):
        """
        Allows objects of this class to be treated as a dictionary proxy. See cls.INTEGRATED_FF_ITEMS.
        The advantage of this is that when this dictionary shows up in stack traces, its contents won't be visible.
        That, in turn, means that access keys won't get logged in GA (Github Actions).
        """
        if item == 'self':  # In case someone accesses the 'self' key we print in the __str__ method.
            return self
        assert item in self.INTEGRATED_FF_ITEMS, (
            f"The integrated_ff fixture has no key named {item}."
            f" Valid keys are {conjoined_list(list(self.INTEGRATED_FF_ITEMS.keys()) + ['self'])}")
        return self.INTEGRATED_FF_ITEMS[item]

    def portal_access_key(self, s3_client=None):
        s3_client = s3_client or self.S3_CLIENT
        return s3_client.get_access_keys()

    def higlass_access_key(self, s3_client=None):
        s3_client = s3_client or self.S3_CLIENT
        return s3_client.get_higlass_key()


IntegratedFixture.verify_portal_access(IntegratedFixture.PORTAL_ACCESS_KEY)


@pytest.fixture(scope='session')
def integrated_ff():
    """
    Object that contains keys and ff_env for integrated environment
    """
    return IntegratedFixture('integrated_ff')


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

    s3_obj = IntegratedFixture.S3_CLIENT
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
