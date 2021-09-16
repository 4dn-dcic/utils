import contextlib
import json
import os
import re


from dcicutils.misc_utils import ignored, override_environ
from dcicutils.qa_utils import (
    MockBoto3, MockBotoElasticBeanstalkClient, MockBotoS3Client,
    make_mock_beanstalk, make_mock_beanstalk_cname, make_mock_beanstalk_environment_variables,
)
from unittest import mock
from . import beanstalk_utils, ff_utils, s3_utils


_MOCK_APPLICATION_NAME = "4dn-web"
_MOCK_SERVICE_USERNAME = 'service-accout-name'
_MOCK_SERVICE_PASSWORD = 'service-accout-pw'
_MOCK_APPLICATION_OPTIONS_PARTIAL = (
    f"MOCK_USERNAME={_MOCK_SERVICE_USERNAME},MOCK_PASSWORD={_MOCK_SERVICE_PASSWORD},ENV_NAME="
)


class MockBoto4DNLegacyElasticBeanstalkClient(MockBotoElasticBeanstalkClient):

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

    class MockBotoBeanstalkClient(MockBotoElasticBeanstalkClient):

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


def make_mock_health_page(env_name):
    return {
        s3_utils.HealthPageKey.BEANSTALK_ENV: env_name,
        s3_utils.HealthPageKey.ELASTICSEARCH: f"http://{env_name}.elasticsearch.whatever",
    }


@contextlib.contextmanager
def mocked_s3utils(beanstalks=None, require_sse=False):
    """
    This context manager sets up a mock version of boto3 for use by s3_utils and ff_utils during the context
    of its test. It also sets up the S3_ENCRYPT_KEY environment variable with a sample value for testing,
    and it sets up a set of mocked beanstalks for fourfront-foo and fourfront-bar, so that s3_utils will not
    get confused when it does discovery operations to find them.
    """
    # First we make a mocked boto3 that will use an S3 mock with mock server side encryption.
    s3_class = MockBotoS3WithSSE if require_sse else MockBotoS3Client
    mock_boto3 = (MockBoto3(s3=s3_class,
                            elasticbeanstalk=make_mock_boto_eb_client_class(beanstalks=beanstalks))
                  if require_sse
                  else MockBoto3)
    # Now we arrange that both s3_utils and ff_utils modules share the illusion that our mock IS the boto3 library
    with mock.patch.object(s3_utils, "boto3", mock_boto3):
        with mock.patch.object(ff_utils, "boto3", mock_boto3):
            with mock.patch.object(beanstalk_utils, "boto3", mock_boto3):
                with mock.patch.object(s3_utils.EnvManager, "fetch_health_page_json") as mock_fetch_health_page_json:

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

                    mock_fetch_health_page_json.side_effect = mocked_fetch_health_page_json
                    # The mocked encrypt key is expected by various tools in the s3_utils module to be supplied
                    # as an environment variable (i.e., in os.environ), so this sets up that environment variable.
                    with override_environ(S3_ENCRYPT_KEY=MockBotoS3WithSSE.SSE_ENCRYPT_KEY):
                        yield


# Here we set up some variables, auxiliary functions, and mocks containing common values needed for testing
# of the next several functions so that the functions don't have to set them up over
# and over again in each test.

class TestScenarios:

    some_server = 'http://localhost:8000'
    some_auth_key, some_auth_secret = some_auth_tuple = ('mykey', 'mysecret')
    some_badly_formed_auth_tuple = ('mykey', 'mysecret', 'other-junk')  # contains an extra element
    some_auth_dict = {'key': some_auth_key, 'secret': some_auth_secret}
    some_auth_dict_with_server = {'key': some_auth_key, 'secret': some_auth_secret, 'server': some_server}
    some_badly_formed_auth_dict = {'kee': some_auth_key, 'secret': some_auth_secret}  # 'key' is misspelled

    foo_env = 'fourfront-foo'
    foo_env_auth_key, foo_env_auth_secret = foo_env_auth_tuple = ('fookey', 'foosecret')
    foo_env_auth_dict = {
        'key': foo_env_auth_key,
        'secret': foo_env_auth_secret
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


def _access_key_home(bs_env):
    """
    Constructs the location in our S3 mock where the stashed access keys live for a given bs_env.

    In our MockBotoS3, buckets and keys in s3 are represented as bucket/key in a MockFileSystem M
    that is used by the S3 mock to hold its contents. So buckets are just the toplevel folders in M,
    and keys are below that, and to preset a mock with contents for a given bucket and key, one just
    creates bucket/key as a filename in M.  This function, _access_key_home will return the appropriate
    filename for such a mock file system to contain the access keys for a given bs_env.
    """
    return os.path.join(s3_utils.s3Utils.SYS_BUCKET_TEMPLATE % bs_env, s3_utils.s3Utils.ACCESS_KEYS_S3_KEY)


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
        _access_key_home(TestScenarios.foo_env): json.dumps(TestScenarios.foo_env_auth_dict),
        _access_key_home(TestScenarios.bar_env): json.dumps(TestScenarios.bar_env_default_auth_dict)
    }
