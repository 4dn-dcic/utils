import contextlib
import datetime
import io
import json
import math
import os
import re
import requests
import time

from dcicutils.env_utils import EnvUtils, full_env_name, is_stg_or_prd_env
from dcicutils.ff_utils import authorized_request
from dcicutils.lang_utils import disjoined_list, conjoined_list
from dcicutils.misc_utils import ignored, override_environ, remove_prefix, full_class_name, PRINT, environ_bool
from dcicutils.qa_utils import (
    MockBoto3, MockBotoElasticBeanstalkClient, MockBotoS3Client, ControlledTime, MockResponse,
    make_mock_beanstalk, make_mock_beanstalk_cname, make_mock_beanstalk_environment_variables,
)
from dcicutils.s3_utils import EnvManager
from unittest import mock
from . import beanstalk_utils, ff_utils, s3_utils, env_utils, env_base, env_manager
from .common import LEGACY_GLOBAL_ENV_BUCKET


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
    protocol = 'https' if is_stg_or_prd_env(env_name) else 'http'
    result = f"{protocol}://{make_mock_beanstalk_cname(env_name)}"
    return result


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

    def write_config(config_name, record):
        record_string = json.dumps(record)
        s3_client.s3_files.files[f"{LEGACY_GLOBAL_ENV_BUCKET}/{config_name}"] = bytes(record_string.encode('utf-8'))

    ecosystem_file = "main.ecosystem"
    for environment in environments:
        record = {
            EnvManager.LEGACY_PORTAL_URL_KEY: make_mock_portal_url(environment),
            EnvManager.LEGACY_ES_URL_KEY: make_mock_es_url(environment),
            EnvManager.LEGACY_ENV_NAME_KEY: environment,
            "ecosystem": ecosystem_file
        }
        write_config(environment, record)
    write_config(ecosystem_file, {"is_legacy": True})
    # Now we arrange that s3_utils, ff_utils, etc. modules share the illusion that our mock IS the boto3 library
    with mock.patch.object(s3_utils, "boto3", mock_boto3):
        with mock.patch.object(ff_utils, "boto3", mock_boto3):
            with mock.patch.object(beanstalk_utils, "boto3", mock_boto3):
                with mock.patch.object(env_utils, "boto3", mock_boto3):
                    with mock.patch.object(env_base, "boto3", mock_boto3):
                        with mock.patch.object(env_manager, "boto3", mock_boto3):
                            with mock.patch.object(s3_utils.EnvManager, "fetch_health_page_json") as mock_fetcher:
                                # This is all that's needed for s3Utils to initialize an EnvManager.
                                # We might have to add more later.
                                def mocked_fetch_health_page_json(url, use_urllib=True):
                                    ignored(use_urllib)  # we don't test this
                                    m = re.match(r'.*(fourfront-[a-z0-9-]+)(?:[.]|$)', url)
                                    if m:
                                        env_name = m.group(1)  # we found it with a fourfront-prefix, so use as is
                                        return make_mock_health_page(env_name)
                                    m = re.match(r'(?:https?://)?([a-z0-9-]+)'
                                                 r'(?:[.](4dnucleome[.]org|hms.harvard.edu)([/].*)|$)', url)
                                    if m:
                                        env_name = full_env_name(m.group(1))  # no fourfront- prefix, so add one
                                        return make_mock_health_page(env_name)
                                    raise NotImplementedError(f"Mock can't handle URL: {url}")

                                mock_fetcher.side_effect = mocked_fetch_health_page_json
                                # The mocked encrypt key is expected by various tools in the s3_utils module
                                # to be supplied as an environment variable (i.e., in os.environ), so this
                                # sets up that environment variable.
                                if require_sse:
                                    with override_environ(S3_ENCRYPT_KEY=s3_class.SSE_ENCRYPT_KEY):
                                        with EnvUtils.local_env_utils_for_testing(
                                                global_env_bucket=os.environ.get('GLOBAL_ENV_BUCKET'),
                                                env_name=(
                                                        environments[0]
                                                        if environments else
                                                        os.environ.get('ENV_NAME'))):
                                            yield mock_boto3
                                else:
                                    with EnvUtils.local_env_utils_for_testing(
                                            global_env_bucket=os.environ.get('GLOBAL_ENV_BUCKET'),
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
            if Bucket == LEGACY_GLOBAL_ENV_BUCKET:
                return  # This bucket does not care about SSE arguments
            super().check_for_kwargs_required_by_mock(operation=operation, Bucket=Bucket, Key=Key, **kwargs)

    return MockBotoS3WithSSE


# MockBotoS3WithSSE = make_mock_boto_s3_with_sse()


REQUESTS_KEYS = list(ff_utils.REQUESTS_VERBS.keys())


@contextlib.contextmanager
def mocked_authorized_requests(**mocks):
    for key in mocks:
        assert key in REQUESTS_KEYS, (f"The string {key} does not name a requests verb."
                                      f" Each key must be {disjoined_list(REQUESTS_KEYS)}.")
    mocked = ff_utils.REQUESTS_VERBS.copy()

    def make_disabled(verb):
        def _disabled(url, *args, **kwargs):
            ignored(args, kwargs)
            raise AssertionError(f"Attempt to {verb} {url}, which is disabled in the mock.")
        return _disabled

    with mock.patch.object(ff_utils, 'REQUESTS_VERBS', mocked):
        for key, val in mocked.items():
            mocked[key] = make_disabled(key)
        for key, val in mocks.items():
            mocked[key] = mocks.get(key) or val
        yield


@contextlib.contextmanager
def controlled_time_mocking(enabled=True):
    dt = ControlledTime()
    if enabled:
        with mock.patch.object(datetime, "datetime", dt):
            with mock.patch.object(requests.sessions, "preferred_clock", dt.time):
                with mock.patch.object(time, "sleep", dt.sleep):
                    with mock.patch.object(time, "time", dt.time):
                        yield dt
    else:
        # This returns the datetime object, but doesn't set it up as a mock, so it's harmless.
        # But it means the caller can still do dt.sleep() operations.
        yield dt


_MYDIR = os.path.dirname(__file__)
_TEST_DIR = os.path.join(os.path.dirname(_MYDIR), "test")


class TestRecorder:
    """
    This allows the web request part of an integration test to be run in a mode where it makes a recording
    that can be played back as an integration test. (Note that this does not mock other elements like s3
    because it's assumed the web request hides all of that.)

    This would replace something that might be written as:

        @pytest.mark.integrated
        def test_something(integrated_ff):
            ...the meat of the test...

    with:

        @pytest.mark.recordable
        @pytest.mark.integratedx
        def test_something(integrated_ff):
            with TestRecorder().recorded_requests('test_something', integrated_ff):
                # Call common subroutine shared by integrated and unit test
                check_something(integrated_ff)

        @pytest.mark.unit
        def test_post_delete_purge_links_metadata_unit():
            with TestRecorder().replayed_requests('test_post_delete_purge_links_metadata') as mocked_integrated_ff:
                # Call common subroutine shared by integrated and unit test
                check_post_delete_purge_links_metadata(mocked_integrated_ff)

        def check_something(integrated_ff):  # Not a test but a common subroutine
            ... the meat of the test ...

    The integration test must be run once by doing something like:

        $ RECORDING_ENABLED=TRUE pytest -vv -m recordable

    This will create the recordings. After that, one can invoke tests in the normal way, but the recorded test
    will be used. Be sure to check in the recording.
    """

    __test__ = False  # This declaration asserts to PyTest that this is not a test case.

    RECORDING_ENABLED = environ_bool("RECORDING_ENABLED")

    DEFAULT_RECORDINGS_DIR = os.path.abspath(os.path.join(_TEST_DIR, "recordings"))

    REAL_REQUEST_VERBS = ff_utils.REQUESTS_VERBS.copy()

    def __init__(self, recordings_dir=None):
        self.recordings_dir = recordings_dir or self.DEFAULT_RECORDINGS_DIR
        self.recording_enabled = self.RECORDING_ENABLED

    @contextlib.contextmanager
    def disable_recording(self):
        old_enabled = self.recording_enabled
        try:
            self.recording_enabled = False
            yield
        finally:
            self.recording_enabled = old_enabled

    @contextlib.contextmanager
    def recorded_requests(self, test_name, integrated_ff):
        with io.open(os.path.join(self.recordings_dir, test_name), 'w') as recording_fp:

            # Write an initial record with sufficient integratoin context information for replaying
            PRINT(json.dumps({
                "ff_key": {"key": "some-key", "secret": "some-secret", "server": integrated_ff["ff_key"]["server"]},
                "ff_env": integrated_ff["ff_env"],
                "ff_env_index_namespace": integrated_ff["ff_env_index_namespace"]
            }), file=recording_fp)

            def mock_recorded(verb):
                def _mocked(url, **kwargs):
                    start = datetime.datetime.now()
                    data = kwargs.get('data')
                    try:
                        response = self.REAL_REQUEST_VERBS[verb](url, **kwargs)
                        status = response.status_code
                        result = response.json()
                        PRINT(f"Recording {verb} {url}")
                        duration = (datetime.datetime.now() - start).total_seconds()
                        duration = math.floor(duration * 10) / 10.0  # round to tenths of a second
                        event = {"verb": verb, "url": url, "data": data,
                                 "duration": duration, "status": status, "result": result}
                        if self.recording_enabled:
                            PRINT(json.dumps(event), file=recording_fp)
                        return response
                    except Exception as e:
                        error_type = full_class_name(e)
                        error_message = str(e)
                        duration = (datetime.datetime.now() - start).total_seconds()
                        event = {"verb": verb, "url": url, "data": data,
                                 "duration": duration, "error_type": error_type, "error_message": error_message}
                        if self.recording_enabled:
                            PRINT(json.dumps(event), file=recording_fp)
                        raise
                _mocked.__name__ = f"_mocked_{verb}_after_recording"
                return _mocked
            with mocked_authorized_requests(GET=mock_recorded('GET'),
                                            PATCH=mock_recorded('PATCH'),
                                            POST=mock_recorded('POST'),
                                            PUT=mock_recorded('PUT'),
                                            DELETE=mock_recorded('DELETE')):
                yield

    @contextlib.contextmanager
    def replayed_requests(self, test_name, mock_time=False):
        with controlled_time_mocking(enabled=mock_time) as dt:
            with io.open(os.path.join(self.recordings_dir, test_name)) as recording_fp:

                PRINT()  # Start output on a fresh line

                def get_next_json():
                    line = recording_fp.readline()
                    if not line:
                        raise AssertionError("Out of replayable records.")
                    parsed_json = json.loads(line)
                    if parsed_json.get('verb') is None:
                        PRINT(f"Consuming special non-request replay record.")
                    else:
                        PRINT(f"Consuming replay record {parsed_json.get('verb')} {parsed_json.get('url')}")
                    return parsed_json

                # Read back initial record with sufficient integratoin context information for replaying
                mocked_integrated_ff = get_next_json()

                def mock_replayed(verb):

                    def _mocked(url, **kwargs):
                        ignored(kwargs)
                        PRINT(f"Replaying {verb} {url}")
                        expected_event = get_next_json()
                        expected_verb = expected_event['verb']
                        expected_url = expected_event['url']
                        if verb != expected_verb or url != expected_url:
                            raise AssertionError(f"Actual call {verb} {url} does not match"
                                                 f" expected call {expected_verb} {expected_url}")
                        if expected_event.get('data') != kwargs.get('data'):  # might or might not have data
                            raise AssertionError(f"Data mismatch on call {verb} {url}.")
                        dt.sleep(expected_event['duration'])
                        error_message = expected_event.get('error_message')
                        if error_message:
                            raise Exception(error_message)
                        else:
                            return MockResponse(status_code=expected_event['status'], json=expected_event['result'])

                    _mocked.__name__ = f"_mocked_{verb}_by_replay"
                    return _mocked

                with mocked_authorized_requests(GET=mock_replayed('GET'),
                                                PATCH=mock_replayed('PATCH'),
                                                POST=mock_replayed('POST'),
                                                PUT=mock_replayed('PUT'),
                                                DELETE=mock_replayed('DELETE')):
                    yield mocked_integrated_ff


def _portal_health_get(namespace, portal_url, key):
    # We used to wire in this URL, but it's better to discover it dynamically so that it can change.
    healh_json_url = f"{portal_url}/health?format=json"
    response = requests.get(healh_json_url)
    health_json = response.json()
    assert health_json['namespace'] == namespace  # check we're talking to proper host
    return health_json[key]


class AbstractIntegratedFixture:
    """
    A class that implements the integrated_ff fixture.
    Implementing this as a class assures that the initialization is done at toplevel before any mocking occurs.
    """

    ENV_NAME = None
    ENV_INDEX_NAMESPACE = None
    ENV_PORTAL_URL = None
    S3_CLIENT = None
    ES_URL = None
    PORTAL_ACCESS_KEY = None
    HIGLASS_ACCESS_KEY = None
    INTEGRATED_FF_ITEMS = None

    @classmethod
    def initialize_class(cls):
        cls.S3_CLIENT = s3_utils.s3Utils(env=cls.ENV_NAME)
        cls.ES_URL = _portal_health_get(portal_url=cls.ENV_PORTAL_URL,
                                        namespace=cls.ENV_INDEX_NAMESPACE,
                                        key="elasticsearch")
        cls.PORTAL_ACCESS_KEY = cls.S3_CLIENT.get_access_keys()
        cls.HIGLASS_ACCESS_KEY = cls.S3_CLIENT.get_higlass_key()

        cls.INTEGRATED_FF_ITEMS = {
            'ff_key': cls.PORTAL_ACCESS_KEY,
            'higlass_key': cls.HIGLASS_ACCESS_KEY,
            'ff_env': cls.ENV_NAME,
            'ff_env_index_namespace': cls.ENV_INDEX_NAMESPACE,
            'es_url': cls.ES_URL,
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


class IntegratedFixture(AbstractIntegratedFixture):
    ENV_NAME = 'fourfront-mastertest'
    ENV_INDEX_NAMESPACE = 'fourfront_mastertest'
    ENV_PORTAL_URL = 'https://mastertest.4dnucleome.org'


IntegratedFixture.initialize_class()
IntegratedFixture.verify_portal_access(IntegratedFixture.PORTAL_ACCESS_KEY)
