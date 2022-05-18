import contextlib
import json
import os
import re

from dcicutils.env_utils import EnvUtils
from dcicutils.misc_utils import ignored, override_environ, remove_prefix
from dcicutils.qa_utils import (
    MockBoto3, MockBotoElasticBeanstalkClient, MockBotoS3Client,
    make_mock_beanstalk, make_mock_beanstalk_cname, make_mock_beanstalk_environment_variables,
)
from dcicutils.s3_utils import EnvManager
from unittest import mock
from . import beanstalk_utils, ff_utils, s3_utils, env_utils, env_base  # , base


_MOCK_APPLICATION_NAME = "4dn-web"
_MOCK_SERVICE_USERNAME = 'service-accout-name'
_MOCK_SERVICE_PASSWORD = 'service-accout-pw'
_MOCK_APPLICATION_OPTIONS_PARTIAL = (
    f"MOCK_USERNAME={_MOCK_SERVICE_USERNAME},MOCK_PASSWORD={_MOCK_SERVICE_PASSWORD},ENV_NAME="
)


class MockBoto4DNLegacyElasticBeanstalkClient(MockBotoElasticBeanstalkClient):  # noQA - missing some abstract methods

    DEFAULT_MOCKED_BEANSTALKS = [
        make_mock_beanstalk("fourfront-cgapdev"),
        make_mock_beanstalk("fourfront-cgapwolf"),
        make_mock_beanstalk("fourfront-cgap"),
        make_mock_beanstalk("fourfront-cgaptest"),
        make_mock_beanstalk("fourfront-webdev"),
        make_mock_beanstalk("fourfront-hotseat"),
        make_mock_beanstalk("fourfront-mastertest"),
        make_mock_beanstalk("fourfront-green", cname="fourfront-green.us-east-1.elasticbeanstalk.com"),
        make_mock_beanstalk("fourfront-blue"),
    ]

    MOCK_APPLICATION_NAME = _MOCK_APPLICATION_NAME
    MOCK_SERVICE_USERNAME = _MOCK_SERVICE_USERNAME
    MOCK_SERVICE_PASSWORD = _MOCK_SERVICE_PASSWORD

    DEFAULT_MOCKED_CONFIGURATION_SETTINGS = [
        {
            "ApplicationName": _MOCK_APPLICATION_NAME,
            "EnvironmentName": env_name,
            "DeploymentStatus": "deployed",
            "OptionSettings": make_mock_beanstalk_environment_variables(_MOCK_APPLICATION_OPTIONS_PARTIAL + env_name),
        }
        for env_name in [beanstalk["EnvironmentName"] for beanstalk in DEFAULT_MOCKED_BEANSTALKS]
    ]

    @classmethod
    def all_legacy_beanstalk_names(cls):
        return [beanstalk["EnvironmentName"] for beanstalk in cls.DEFAULT_MOCKED_BEANSTALKS]


def make_mock_boto_eb_client_class(beanstalks):

    class MockBotoBeanstalkClient(MockBotoElasticBeanstalkClient):  # noQA - missing some abstract methods

        DEFAULT_MOCKED_BEANSTALKS = list(map(make_mock_beanstalk, beanstalks))

        DEFAULT_MOCKED_CONFIGURATION_SETTINGS = [
            {
                "ApplicationName": _MOCK_APPLICATION_NAME,
                "EnvironmentName": env_name,
                "DeploymentStatus": "deployed",
                "OptionSettings": make_mock_beanstalk_environment_variables(_MOCK_APPLICATION_OPTIONS_PARTIAL
                                                                            + env_name),
            }
            for env_name in [beanstalk["EnvironmentName"] for beanstalk in DEFAULT_MOCKED_BEANSTALKS]
        ]

    return MockBotoBeanstalkClient


def make_mock_es_url(env_name):
    return f"http://{env_name}.es.mocked-fourfront.org"


def make_mock_portal_url(env_name):
    return f"http://{make_mock_beanstalk_cname(env_name)}"


def make_mock_health_page(env_name):
    return {
        s3_utils.HealthPageKey.BEANSTALK_ENV: env_name,
        s3_utils.HealthPageKey.ELASTICSEARCH: make_mock_es_url(env_name),
        s3_utils.HealthPageKey.SYSTEM_BUCKET: s3_utils.s3Utils.SYS_BUCKET_TEMPLATE % env_name,
        s3_utils.HealthPageKey.PROCESSED_FILE_BUCKET: s3_utils.s3Utils.OUTFILE_BUCKET_TEMPLATE % env_name,
        s3_utils.HealthPageKey.FILE_UPLOAD_BUCKET: s3_utils.s3Utils.RAW_BUCKET_TEMPLATE % env_name,
        s3_utils.HealthPageKey.BLOB_BUCKET: s3_utils.s3Utils.BLOB_BUCKET_TEMPLATE % env_name,
        s3_utils.HealthPageKey.METADATA_BUNDLES_BUCKET: s3_utils.s3Utils.METADATA_BUCKET_TEMPLATE % env_name,
        s3_utils.HealthPageKey.TIBANNA_CWLS_BUCKET: s3_utils.s3Utils.TIBANNA_CWLS_BUCKET_TEMPLATE,      # no env_name
        s3_utils.HealthPageKey.TIBANNA_OUTPUT_BUCKET: s3_utils.s3Utils.TIBANNA_OUTPUT_BUCKET_TEMPLATE,  # no env_name
    }


@contextlib.contextmanager
def mocked_s3utils(environments=None, require_sse=False, other_access_key_names=None):
    """
    This context manager sets up a mock version of boto3 for use by s3_utils and ff_utils during the context
    of its test. It also sets up the S3_ENCRYPT_KEY environment variable with a sample value for testing,
    and it sets up a set of mocked beanstalks for fourfront-foo and fourfront-bar, so that s3_utils will not
    get confused when it does discovery operations to find them.
    """
    if environments is None:
        environments = TestScenarios.DEFAULT_BEANSTALKS
    # First we make a mocked boto3 that will use an S3 mock with mock server side encryption.
    s3_class = (make_mock_boto_s3_with_sse(beanstalks=environments, other_access_key_names=other_access_key_names)
                if require_sse
                else MockBotoS3Client)
    mock_boto3 = MockBoto3(s3=s3_class,
                           elasticbeanstalk=make_mock_boto_eb_client_class(beanstalks=environments))
    s3_client = mock_boto3.client('s3')  # This creates the s3 file system
    assert isinstance(s3_client, s3_class)
    for environment in environments:
        record = {
            EnvManager.LEGACY_PORTAL_URL_KEY: make_mock_portal_url(environment),
            EnvManager.LEGACY_ES_URL_KEY: make_mock_es_url(environment),
            EnvManager.LEGACY_ENV_NAME_KEY: environment
        }
        record_string = json.dumps(record)
        s3_client.s3_files.files[f"foursight-envs/{environment}"] = bytes(record_string.encode('utf-8'))
    # Now we arrange that s3_utils, ff_utils, etc. modules share the illusion that our mock IS the boto3 library
    with mock.patch.object(s3_utils, "boto3", mock_boto3):
        with mock.patch.object(ff_utils, "boto3", mock_boto3):
            with mock.patch.object(beanstalk_utils, "boto3", mock_boto3):
                with mock.patch.object(env_utils, "boto3", mock_boto3):
                    with mock.patch.object(env_base, "boto3", mock_boto3):
                        with mock.patch.object(s3_utils.EnvManager, "fetch_health_page_json") as mock_fetcher:
                            # This is all that's needed for s3Utils to initialize an EnvManager.
                            # We might have to add more later.
                            def mocked_fetch_health_page_json(url, use_urllib=True):
                                ignored(use_urllib)  # we don't test this
                                m = re.match(r'.*(fourfront-[a-z0-9-]+)(?:[.]|$)', url)
                                if m:
                                    env_name = m.group(1)
                                    return make_mock_health_page(env_name)
                                else:
                                    raise NotImplementedError(f"Mock can't handle URL: {url}")

                            mock_fetcher.side_effect = mocked_fetch_health_page_json
                            # The mocked encrypt key is expected by various tools in the s3_utils module
                            # to be supplied as an environment variable (i.e., in os.environ), so this
                            # sets up that environment variable.
                            if require_sse:
                                with override_environ(S3_ENCRYPT_KEY=s3_class.SSE_ENCRYPT_KEY):
                                    with EnvUtils.local_env_utils(global_env_bucket=os.environ.get('GLOBAL_ENV_BUCKET'),
                                                                  env_name=environments[0]
                                                                  if environments else
                                                                  os.environ.get('ENV_NAME')):
                                        yield mock_boto3
                            else:
                                with EnvUtils.local_env_utils(global_env_bucket=os.environ.get('GLOBAL_ENV_BUCKET'),
                                                              env_name=os.environ.get('ENV_NAME')):
                                    yield mock_boto3


# Here we set up some variables, auxiliary functions, and mocks containing common values needed for testing
# of the next several functions so that the functions don't have to set them up over
# and over again in each test.

class TestScenarios:

    DEFAULT_BEANSTALKS = ['fourfront-foo', 'fourfront-bar']

    @classmethod
    def mocked_auth_key(cls, env):
        short_env = remove_prefix("fourfront-", env)
        return f"{short_env}key"

    @classmethod
    def mocked_auth_secret(cls, env):
        short_env = remove_prefix("fourfront-", env)
        return f"{short_env}secret"

    @classmethod
    def mocked_auth_server(cls, env):
        return f"http://{make_mock_beanstalk_cname(env)}/"

    @classmethod
    def mocked_auth_key_secret_tuple(cls, env):
        return (cls.mocked_auth_key(env),
                cls.mocked_auth_secret(env))

    @classmethod
    def mocked_auth_key_secret_dict(cls, env):
        return {
            'key': cls.mocked_auth_key(env),
            'secret': cls.mocked_auth_secret(env),
        }

    @classmethod
    def mocked_auth_key_secret_server_dict(cls, env):
        return {
            'key': cls.mocked_auth_key(env),
            'secret': cls.mocked_auth_secret(env),
            'server': cls.mocked_auth_server(env),
        }

    some_server = 'http://localhost:8000'
    some_auth_key, some_auth_secret = some_auth_tuple = ('mykey', 'mysecret')
    some_badly_formed_auth_tuple = ('mykey', 'mysecret', 'other-junk')  # contains an extra element
    some_auth_dict = {'key': some_auth_key, 'secret': some_auth_secret}
    some_auth_dict_with_server = {'key': some_auth_key, 'secret': some_auth_secret, 'server': some_server}
    some_badly_formed_auth_dict = {'kee': some_auth_key, 'secret': some_auth_secret}  # 'key' is misspelled

    foo_env = 'fourfront-foo'
    foo_env_auth_key, foo_env_auth_secret = foo_env_auth_tuple = ('fookey', 'foosecret')
    foo_env_auth_dict = {
        'key': foo_env_auth_key,  # mocked_auth_key('fourfront-foo')
        'secret': foo_env_auth_secret  # mocked_auth_secret('fourfront-foo')
    }

    FOURFRONT_FOO_HEALTH_PAGE = make_mock_health_page('fourfront-foo')

    bar_env = 'fourfront-bar'
    bar_env_auth_key, bar_env_auth_secret = bar_env_auth_tuple = ('barkey', 'barsecret')
    bar_env_url = f"http://{make_mock_beanstalk_cname('fourfront-bar')}/"
    bar_env_url_trimmed = bar_env_url.rstrip('/')
    bar_env_auth_dict = {
        'key': bar_env_auth_key,
        'secret': bar_env_auth_secret,
        'server': bar_env_url,
    }
    bar_env_default_auth_dict = {'default': bar_env_auth_dict}

    FOURFRONT_BAR_HEALTH_PAGE = make_mock_health_page('fourfront-bar')


SSE_CUSTOMER_KEY = 'SSECustomerKey'
SSE_CUSTOMER_ALGORITHM = 'SSECustomerAlgorithm'


def _access_key_home(bs_env, access_key_name):
    """
    Constructs the location in our S3 mock where the stashed access keys live for a given bs_env.

    In our MockBotoS3, buckets and keys in s3 are represented as bucket/key in a MockFileSystem M
    that is used by the S3 mock to hold its contents. So buckets are just the toplevel folders in M,
    and keys are below that, and to preset a mock with contents for a given bucket and key, one just
    creates bucket/key as a filename in M.  This function, _access_key_home will return the appropriate
    filename for such a mock file system to contain the access keys for a given bs_env.
    """
    return os.path.join(s3_utils.s3Utils.SYS_BUCKET_TEMPLATE % bs_env, access_key_name)


def make_mock_boto_s3_with_sse(beanstalks=None, other_access_key_names=None):

    access_key_names = [s3_utils.s3Utils.ACCESS_KEYS_S3_KEY] + (other_access_key_names or [])

    if beanstalks is None:
        beanstalks = TestScenarios.DEFAULT_BEANSTALKS

    class MockBotoS3WithSSE(MockBotoS3Client):
        """
        This is a specialized mock for a boto3 S3 client that does server-side encryption (SSE).
        We don't actually test that encryption is done, since that would be done by boto3, but we set up so that
        that calls to any methods on the mock use certain required keyword arguments beyond the ordinary methods
        that do the ordinary work of the method. e.g., for this mock definition:

            def upload_file(self, Filename, Bucket, Key, **kwargs)
                ...

        if the call does not look like:

            upload_File(Filename=..., Bucket=..., Key=...,
                        SSECustomerKey='shazam', SSECustomerAlgorithm='AES256')

        where the mock value 'shazam' is a wired value supplied in calls to the mock and 'AES256' is a constant we
        expect is consistently used throughout this code base, then an error is raised in testing. The intent is to
        make sure the calls are done with a certain degree of consistency.
        """

        SSE_ENCRYPT_KEY = 'shazam'

        MOCK_REQUIRED_ARGUMENTS = {
            # Our s3 mock for this test must check that all API calls pass these required arguments.
            SSE_CUSTOMER_KEY: SSE_ENCRYPT_KEY,
            SSE_CUSTOMER_ALGORITHM: "AES256"
        }

        MOCK_STATIC_FILES = {
            # Our s3 mock presumes access keys are stashed in a well-known key
            # in the system bucket corresponding to the given beanstalk.
            _access_key_home(env, access_key_name=access_key_name):
                json.dumps(TestScenarios.mocked_auth_key_secret_server_dict(env))
            for env in beanstalks
            for access_key_name in access_key_names
        }

        def check_for_kwargs_required_by_mock(self, operation, Bucket, Key, **kwargs):
            if Bucket == 'foursight-envs':
                return  # This bucket does not care about SSE arguments
            super().check_for_kwargs_required_by_mock(operation=operation, Bucket=Bucket, Key=Key, **kwargs)

    return MockBotoS3WithSSE


# MockBotoS3WithSSE = make_mock_boto_s3_with_sse()
