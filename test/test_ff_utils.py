import contextlib
import copy
import pytest
import json
import os
import requests
import shutil
import time

from botocore.exceptions import ClientError
from dcicutils import es_utils, ff_utils, s3_utils
from dcicutils.misc_utils import make_counter, remove_prefix, remove_suffix, check_true
from dcicutils.ff_mocks import mocked_s3utils, TestScenarios
from dcicutils.qa_utils import (
    check_duplicated_items_by_key, ignored, raises_regexp, MockResponse, MockBoto3, MockBotoSQSClient,
)
from types import GeneratorType
from unittest import mock
from urllib.parse import urlsplit, parse_qsl


pytestmark = pytest.mark.working


@pytest.fixture
def eset_json():
    return {
        "schema_version": "2",
        "accession": "4DNES4GSP9S4",
        "award": "4871e338-b07d-4665-a00a-357648e5bad6",
        "alternate_accessions": [],
        "aliases": [
            "ren:HG00512_repset"
        ],
        "experimentset_type": "replicate",
        "status": "released",
        "experiments_in_set": [
            "d4b0e597-8c81-43e3-aeda-e9842fc18e8f",
            "8d10f11f-95a8-4b8d-8ff2-748ea8631a23"
        ],
        "lab": "795847de-20b6-4f8c-ba8d-185215469cbf",
        "public_release": "2017-06-30",
        "uuid": "9eb40c13-cf85-487c-9819-71ef74a22dcc",
        "documents": [],
        "description": "Dilution Hi-C experiment on HG00512",
        "submitted_by": "da4f53e5-4e54-4ae7-ad75-ba47316a8bfa",
        "date_created": "2017-04-28T17:46:08.642218+00:00",
        "replicate_exps": [
            {
                "replicate_exp": "d4b0e597-8c81-43e3-aeda-e9842fc18e8f",
                "bio_rep_no": 1,
                "tec_rep_no": 1
            },
            {
                "replicate_exp": "8d10f11f-95a8-4b8d-8ff2-748ea8631a23",
                "bio_rep_no": 2,
                "tec_rep_no": 1
            }
        ],
    }


@pytest.fixture
def bs_embed_json():
    return {
        "lab": {
            "display_title": "David Gilbert, FSU",
            "uuid": "6423b207-8176-4f06-a127-951b98d6a53a",
            "link_id": "~labs~david-gilbert-lab~",
            "@id": "/labs/david-gilbert-lab/"
        },
        "display_title": "4DNBSLACJHX1"
    }


@pytest.fixture
def profiles():
    return {
        "ExperimentSetReplicate": {
            "title": "Replicate Experiments",
            "description": "Experiment Set for technical/biological replicates.",
            "properties": {
                "tags": {"uniqueItems": "true", "description": "Key words that can tag an item - useful for filtering.", "type": "array", "ff_clear": "clone", "items": {"title": "Tag", "description": "A tag for the item.", "type": "string"}, "title": "Tags"},  # noqa: E501
                "documents": {"uniqueItems": "true", "description": "Documents that provide additional information (not data file).", "type": "array", "default": [], "comment": "See Documents sheet or collection for existing items.", "title": "Documents", "items": {"title": "Document", "description": "A document that provides additional information (not data file).", "type": "string", "linkTo": "Document"}},  # noqa: E501
                "notes": {"exclude_from": ["submit4dn", "FFedit-create"], "title": "Notes", "description": "DCIC internal notes.", "type": "string", "elasticsearch_mapping_index_type": {"title": "Field mapping index type", "description": "Defines one of three types of indexing available", "type": "string", "default": "analyzed", "enum": ["analyzed", "not_analyzed", "no"]}}  # noqa: E501
            }
        },
        "TreatmentChemical": {
            "title": "Chemical Treatment",
            "description": "A Chemical or Drug Treatment on Biosample.",
            "properties": {
                "documents": {"uniqueItems": "true", "description": "Documents that provide additional information (not data file).", "type": "array", "default": [], "comment": "See Documents sheet or collection for existing items.", "title": "Documents", "items": {"title": "Document", "description": "A document that provides additional information (not data file).", "type": "string", "linkTo": "Document"}},  # noqa: E501
                "public_release": {"anyOf": [{"format": "date-time"}, {"format": "date"}], "exclude_from": ["submit4dn", "FFedit-create"], "description": "The date which the item was released to the public", "permission": "import_items", "type": "string", "comment": "Do not submit, value is assigned when released.", "title": "Public Release Date"},  # noqa: E501
            }
        }
    }


_DATA_DIR = os.path.join(os.path.dirname(__file__), 'data_files')


@pytest.fixture
def mocked_replicate_experiment():
    with open(os.path.join(_DATA_DIR, 'test_items/431106bc-8535-4448-903e-854af460b260.json')) as opf:
        return json.load(opf)


@pytest.fixture
def qc_metrics():
    with open(os.path.join(_DATA_DIR, 'test_qc_metrics/431106bc-8535-4448-903e-854af460b260.json')) as opf:
        return json.load(opf)


def test_generate_rand_accession():
    test = ff_utils.generate_rand_accession()
    assert '4DN' in test
    assert '0' not in test
    test_cgap = ff_utils.generate_rand_accession('GAP', 'XX')
    assert 'GAPXX' in test_cgap
    assert '4DN' not in test_cgap
    assert '0' not in test_cgap


def test_get_response_json():
    # use responses from http://httpbin.org
    good_res = requests.get('http://httpbin.org/json')
    good_res_json = ff_utils.get_response_json(good_res)
    assert isinstance(good_res_json, dict)
    bad_res = requests.get('http://httpbin.org/status/500')
    with pytest.raises(Exception) as exec_info:
        ff_utils.get_response_json(bad_res)
    assert 'Cannot get json' in str(exec_info.value)


def test_process_add_on():
    add_1 = '&type=Biosample&format=json'
    assert ff_utils.process_add_on(add_1) == '?type=Biosample&format=json'
    add_2 = 'type=Biosample&format=json'
    assert ff_utils.process_add_on(add_2) == '?type=Biosample&format=json'
    add_3 = ''
    assert ff_utils.process_add_on(add_3) == ''


def test_url_params_functions():
    fake_url = 'http://not-a-url.com/?test1=abc&test2=def'
    url_params = ff_utils.get_url_params(fake_url)
    assert url_params['test1'] == ['abc']
    assert url_params['test2'] == ['def']
    url_params['test1'] = ['xyz']
    url_params['test3'] = ['abc']
    new_fake_url = ff_utils.update_url_params_and_unparse(fake_url, url_params)
    assert 'http://not-a-url.com/?' in new_fake_url
    assert 'test1=xyz' in new_fake_url
    assert 'test2=def' in new_fake_url
    assert 'test3=abc' in new_fake_url


def test_unified_authentication_decoding(integrated_ff):
    """
    Test that we decode the various formats and locations of keys and secrets in an uniform way.
    """

    any_id, any_secret, any_env = 'any-id', 'any-secret', 'any-env'

    any_key_tuple = (any_id, any_secret)
    any_key_dict = {"key": any_id, "secret": any_secret}
    any_old_key = {'default': any_key_dict}
    any_old_bogus_key1 = {'default': [any_id, any_secret]}  # The legacy format requires a dictionary in "default"
    any_old_bogus_key2 = {}
    any_old_bogus_key3 = 17
    any_old_bogus_key4 = any_secret

    # In the end, all of the above key formats need to turn into the tuple format.
    # (key, secret) == {"key": key, "secret": secret} = {"default": {"key": key, "secret", secret}}
    any_key_normalized = any_key_tuple

    class UnusedS3Utils:

        def __init__(self, env):
            ignored(env)
            raise AssertionError("s3Utils() got used.")

    for any_key in [any_key_tuple, any_key_dict, any_old_key]:

        with mock.patch.object(s3_utils, "s3Utils", UnusedS3Utils):

            # These keys can be entirely canonicalized locally, without s3Utils getting involved, because
            # the caller has given the auth directly and all we have to do is syntax-check it

            key1 = ff_utils.unified_authentication(any_key_tuple, any_env)
            assert key1 == any_key_normalized

            key2 = ff_utils.unified_authentication(any_key, None)
            assert key2 == any_key_normalized

            # This error can be raised without using s3Utils because we don't know what env to use anyway.

            with pytest.raises(Exception) as exec_info:
                ff_utils.unified_authentication(None, None)

            assert 'Must provide a valid authorization key or ff' in str(exec_info.value)

        class MockS3Utils:

            def __init__(self, env):
                assert env == any_env
                self.env = env

            def get_access_keys(self):  # noqa - this is a mock so PyCharm shouldn't suggest it could be static
                return any_key

        with mock.patch.object(s3_utils, "s3Utils", MockS3Utils):

            # If no auth is given locally, we have to fetch it from s3, so that's where s3Utils is needed.

            key3 = ff_utils.unified_authentication(None, any_env)
            assert key3 == any_key_normalized

    for any_bogus_key in [any_old_bogus_key1, any_old_bogus_key2, any_old_bogus_key3, any_old_bogus_key4]:

        with mock.patch.object(s3_utils, "s3Utils", UnusedS3Utils):
            with pytest.raises(Exception) as exec_info:
                ff_utils.unified_authentication(any_bogus_key, None)
            assert 'Must provide a valid authorization key or ff' in str(exec_info.value)

        class MockS3Utils:

            def __init__(self, env):
                assert env == any_env
                self.env = env

            def get_access_keys(self):  # noqa - this is a mock so PyCharm shouldn't suggest it could be static
                return any_bogus_key

        with mock.patch.object(s3_utils, "s3Utils", MockS3Utils):
            with pytest.raises(Exception) as exec_info:
                ff_utils.unified_authentication(None, any_env)
            assert 'Must provide a valid authorization key or ff' in str(exec_info.value)


@contextlib.contextmanager
def mocked_s3utils_with_sse():
    with mocked_s3utils(beanstalks=['fourfront-foo', 'fourfront-bar'], require_sse=True):
        yield


def test_unified_authentication_unit():

    ts = TestScenarios

    with mocked_s3utils_with_sse():

        # When supplied a basic auth tuple, (key, secret), and a fourfront environment, the tuple is returned.
        auth1 = ff_utils.unified_authentication(ts.some_auth_tuple, ts.foo_env)
        assert auth1 == ts.some_auth_tuple

        # When supplied a password and a fourfront environment,
        auth2 = ff_utils.unified_authentication(ts.some_auth_dict, ts.foo_env)
        assert auth2 == ts.some_auth_tuple  # Given an auth dict, the result is canonicalized to an auth tuple

        # A deprecated form of the dictionary has an extra layer of wrapper we have to strip off.
        # Hopefully calls like this are being rewritten, but for now we continue to support the behavior.
        auth3 = ff_utils.unified_authentication({'default': ts.some_auth_dict}, ts.foo_env)
        assert auth3 == ts.some_auth_tuple  # Given an auth dict, the result is canonicalized to an auth tuple

        # This tests that both forms of default credentials are supported remotely.

        auth4 = ff_utils.unified_authentication(None, ts.foo_env)
        # Check that the auth dict fetched from server is canonicalized to an auth tuple...
        assert auth4 == ts.foo_env_auth_tuple

        auth5 = ff_utils.unified_authentication(None, ts.bar_env)
        # Check that the auth dict fetched from server is canonicalized to an auth tuple...
        assert auth5 == ts.bar_env_auth_tuple

        with raises_regexp(ValueError, "Must provide a valid authorization key or ff environment."):
            # The .unified_authentication operation checks that an auth given as a tuple has length 2.
            ff_utils.unified_authentication(ts.some_badly_formed_auth_tuple, ts.foo_env)

        with raises_regexp(ValueError, "Must provide a valid authorization key or ff environment."):
            # The .unified_authentication operation checks that an auth given as a dict has keys 'key' and 'secret'.
            ff_utils.unified_authentication(ts.some_badly_formed_auth_dict, ts.foo_env)

        with raises_regexp(ValueError, "Must provide a valid authorization key or ff environment."):
            # If the first arg (auth) is None, the second arg (ff_env) must not be.
            ff_utils.unified_authentication(None, None)


# Integration tests

@pytest.mark.integratedx
@pytest.mark.flaky
def test_unified_authentication_integrated(integrated_ff):
    key1 = ff_utils.unified_authentication(integrated_ff['ff_key'], integrated_ff['ff_env'])
    assert len(key1) == 2
    key2 = ff_utils.unified_authentication({'default': integrated_ff['ff_key']}, integrated_ff['ff_env'])
    assert key1 == key2
    key3 = ff_utils.unified_authentication(None, integrated_ff['ff_env'])
    assert key1 == key3
    key4 = ff_utils.unified_authentication(key1, None)
    assert key1 == key4
    with pytest.raises(Exception) as exec_info:
        ff_utils.unified_authentication(None, None)
    assert 'Must provide a valid authorization key or ff' in str(exec_info.value)


@pytest.mark.integrated
def test_unified_authentication_prod_envs_integrated_only():
    # This is ONLY an integration test. There is no unit test functionality tested here.
    # All of the functionality used here is already tested elsewhere.

    # Fourfront prod
    ff_prod_auth = ff_utils.unified_authentication(ff_env="data")
    assert len(ff_prod_auth) == 2
    staging_auth = ff_utils.unified_authentication(ff_env="staging")
    assert staging_auth == ff_prod_auth
    green_auth = ff_utils.unified_authentication(ff_env="fourfront-green")
    assert green_auth == ff_prod_auth
    blue_auth = ff_utils.unified_authentication(ff_env="fourfront-blue")
    assert blue_auth == ff_prod_auth

    # Decommissioned
    # # CGAP prod
    # cgap_prod_auth = ff_utils.unified_authentication(ff_env="fourfront-cgap")
    # assert len(cgap_prod_auth) == 2
    #
    # # Assure CGAP and Fourfront don't share auth
    # auth_is_shared = cgap_prod_auth == ff_prod_auth
    # assert not auth_is_shared

    with raises_regexp(ClientError, "does not exist"):
        # There is no such environment as 'fourfront-data'
        ff_utils.unified_authentication(ff_env="fourfront-data")


def test_get_authentication_with_server_unit():

    ts = TestScenarios

    with mocked_s3utils_with_sse():

        key1 = ff_utils.get_authentication_with_server(ts.bar_env_auth_dict, None)
        assert isinstance(key1, dict)
        assert {'server', 'key', 'secret'} <= set(key1.keys())
        assert key1['key'] == ts.bar_env_auth_key
        assert key1['secret'] == ts.bar_env_auth_secret
        assert key1['server'] == ts.bar_env_url_trimmed

        key2 = ff_utils.get_authentication_with_server(ts.bar_env_default_auth_dict, None)
        assert isinstance(key2, dict)
        assert {'server', 'key', 'secret'} <= set(key2.keys())
        assert key2 == key1

        key3 = ff_utils.get_authentication_with_server(None, ts.bar_env)
        assert isinstance(key2, dict)
        assert {'server', 'key', 'secret'} <= set(key3.keys())
        assert key3 == key1

        with raises_regexp(ValueError, 'ERROR GETTING SERVER'):
            # The mocked foo_env, unlike the bar_env, is missing a 'server' key in its dict,
            # so it will be rejected as bad data by .get_authentication_with_server().
            ff_utils.get_authentication_with_server(ts.foo_env_auth_dict, None)

        with raises_regexp(ValueError, 'ERROR GETTING SERVER'):
            # If the auth is not provided (or None), the ff_env must be given (and not None).
            ff_utils.get_authentication_with_server(None, None)


@pytest.mark.integratedx
@pytest.mark.flaky  # e.g., if a deployment or CNAME swap occurs during the run
def test_get_authentication_with_server_integrated(integrated_ff):
    key1 = ff_utils.get_authentication_with_server(integrated_ff['ff_key'], None)
    assert {'server', 'key', 'secret'} <= set(key1.keys())
    key2 = ff_utils.get_authentication_with_server({'default': integrated_ff['ff_key']}, None)
    assert key1 == key2
    key3 = ff_utils.get_authentication_with_server(None, integrated_ff['ff_env'])
    assert key1 == key3
    bad_key = copy.copy(integrated_ff['ff_key'])
    del bad_key['server']
    with pytest.raises(Exception) as exec_info:
        ff_utils.get_authentication_with_server(bad_key, None)
    assert 'ERROR GETTING SERVER' in str(exec_info.value)


def test_stuff_in_queues_unit():

    class MockBotoSQSClientEmpty(MockBotoSQSClient):
        def compute_mock_queue_attribute(self, QueueUrl, Attribute):  # noQA - Amazon AWS chose the argument names
            return 0

    with mock.patch.object(ff_utils, "boto3", MockBoto3(sqs=MockBotoSQSClientEmpty)):
        assert not ff_utils.stuff_in_queues('fourfront-foo')

    class MockBotoSQSClientPrimary(MockBotoSQSClient):

        def compute_mock_queue_attribute(self, QueueUrl, Attribute):  # noQA - Amazon AWS chose the argument names
            result = 0 if 'secondary' in QueueUrl else 1
            print("Returning %s for url=%s attr=%s" % (result, QueueUrl, Attribute))
            return result

    with mock.patch.object(ff_utils, "boto3", MockBoto3(sqs=MockBotoSQSClientPrimary)):
        assert ff_utils.stuff_in_queues('fourfront-foo')
        assert ff_utils.stuff_in_queues('fourfront-foo', check_secondary=True)

    class MockBotoSQSClientSecondary(MockBotoSQSClient):
        def compute_mock_queue_attribute(self, QueueUrl, Attribute):  # noQA - Amazon AWS chose the argument names
            result = 1 if 'secondary' in QueueUrl else 0
            print("Returning %s for url=%s attr=%s" % (result, QueueUrl, Attribute))
            return result

    with mock.patch.object(ff_utils, "boto3", MockBoto3(sqs=MockBotoSQSClientSecondary)):
        assert not ff_utils.stuff_in_queues('fourfront-foo')
        assert ff_utils.stuff_in_queues('fourfront-foo', check_secondary=True)

    class MockBotoSQSClientErring(MockBotoSQSClient):
        def compute_mock_queue_attribute(self, QueueUrl, Attribute):  # noQA - Amazon AWS chose the argument names
            print("Simulating a boto3 error.")
            raise ClientError({"Error": {"Code": 500, "Message": "Simulated Boto3 Error"}},  # noQA
                              "get_queue_attributes")

    # If there is difficulty getting to the queue, it behaves as if there's stuff in the queue.
    with mock.patch.object(ff_utils, "boto3", MockBoto3(sqs=MockBotoSQSClientErring)):
        assert ff_utils.stuff_in_queues('fourfront-foo')
        assert ff_utils.stuff_in_queues('fourfront-foo', check_secondary=True)


@pytest.mark.integratedx
@pytest.mark.flaky
def test_stuff_in_queues_integrated(integrated_ff):
    """
    Gotta index a bunch of stuff to make this work
    """
    search_res = ff_utils.search_metadata('search/?limit=all&type=File', key=integrated_ff['ff_key'])
    # just take the first handful
    for item in search_res[:8]:
        ff_utils.patch_metadata({}, obj_id=item['uuid'], key=integrated_ff['ff_key'])
    time.sleep(3)  # let queues catch up
    stuff_in_queue = ff_utils.stuff_in_queues(integrated_ff['ff_env'], check_secondary=True)
    assert stuff_in_queue
    with pytest.raises(Exception) as exec_info:
        ff_utils.stuff_in_queues(None, check_secondary=True)  # fail if no env specified
    assert 'Must provide a full fourfront environment name' in str(exec_info.value)


@pytest.mark.integrated
@pytest.mark.flaky
def test_authorized_request_integrated(integrated_ff):
    """
    Cover search case explicitly since it uses a different retry fxn by default
    """
    server = integrated_ff['ff_key']['server']
    item_url = server + '/331111bc-8535-4448-903e-854af460a254'  # a test item
    # not a real verb
    with pytest.raises(Exception) as exec_info:
        ff_utils.authorized_request(item_url, auth=integrated_ff['ff_key'], verb='LAME')
    assert 'Provided verb LAME is not valid' in str(exec_info.value)

    # good GET request for an item (passing in header)
    hdr = {'content-type': 'application/json', 'accept': 'application/json'}
    good_resp1 = ff_utils.authorized_request(item_url, auth=integrated_ff['ff_key'], verb='GET', headers=hdr)
    assert good_resp1.status_code == 200
    # good GET request for a search (passing in a timeout)
    good_resp2 = ff_utils.authorized_request(server + '/search/?type=Biosample',
                                             auth=integrated_ff['ff_key'], verb='GET', timeout=45)
    assert good_resp2.status_code == 200
    # requests that return no results should have a 404 status_code but no error
    no_results_resp = ff_utils.authorized_request(server + '/search/?type=Biosample&name=joe',
                                                  auth=integrated_ff['ff_key'], verb='GET')
    assert no_results_resp.status_code == 404
    assert no_results_resp.json()['@graph'] == []

    # bad GET requests for an item and search
    with pytest.raises(Exception) as exec_info:
        ff_utils.authorized_request(server + '/abcdefg', auth=integrated_ff['ff_key'], verb='GET')
    assert 'Bad status code' in str(exec_info.value)
    with pytest.raises(Exception) as exec_info:
        ff_utils.authorized_request(server + '/search/?type=LAME', auth=integrated_ff['ff_key'], verb='GET')
    assert 'Bad status code' in str(exec_info.value)


def test_get_metadata_unit():

    ts = TestScenarios

    # The first part of this function sets up some common tools and then we test various scenarios

    counter = make_counter()  # used to generate some sample data in mock calls
    unsupplied = object()     # used in defaulting to prove an argument wasn't called

    # use this test biosource
    test_item = '331111bc-8535-4448-903e-854af460b254'
    test_item_id = '/' + test_item

    def make_mocked_stuff_in_queues(return_value=None):
        def mocked_stuff_in_queues(ff_env, check_secondary=False):
            assert return_value is not None, "The mock for stuff_in_queues was not expected to be called."
            check_true(ff_env, "Must provide a full fourfront environment name to stuff_in_queues,"
                               " so it's required by this mock.",
                       error_class=ValueError)
            assert check_secondary is False, "This mock expects check_secondary to be false for stuff_in_queues."
            return return_value
        return mocked_stuff_in_queues

    def mocked_authorized_request(url, auth=None, ff_env=None, verb='GET',
                                  retry_fxn=unsupplied, **kwargs):
        """
        This function can be used a mock for successful uses of 'authorized_request'.
        It tests that certain arguments were passed in predictable ways, such as
        (a) that the mock uses a URL on the 'bar' scenario environment ('http://fourfront-bar.example/')
            so that we know some other authorized request wasn't also attempted that wasn't expecting this mock.
        (b) that the retry_fxn was not passed, since this mock doesn't know what to do with that
        (c) that it's a GET operation, since we're not prepared to store anything and we're testing a getter function
        (d) that proper authorization for the 'bar' scenario was given
        It returns mock data that identifies itself as what was asked for.
        """
        ignored(ff_env, kwargs)
        assert url.startswith(ts.bar_env_url)
        assert retry_fxn == unsupplied, "The retry_fxn argument was not expected by this mock."
        assert verb == 'GET'
        assert auth == ts.bar_env_auth_dict
        return MockResponse(json={
            'mock_data': counter(),  # just so we can tell calls apart
            '@id': remove_prefix(ts.bar_env_url_trimmed, remove_suffix("?datastore=database", url))
        })

    def make_mocked_authorized_request_erring(status_code, json):
        """Creates an appropriate mock for 'authorized_request' that will just return a specified error."""

        def mocked_authorized_request_erring(url, auth=None, ff_env=None, verb='GET',
                                             retry_fxn=unsupplied, **kwargs):
            ignored(url, auth, ff_env, verb, retry_fxn, kwargs)
            return MockResponse(status_code=status_code, json=json)

        return mocked_authorized_request_erring

    # Actual testing begins here.

    # First we test the 'rosy path' where things go according to plan...

    with mock.patch.object(ff_utils, "authorized_request") as mock_authorized_request:

        def test_it(n, check_queue=None, expect_suffix="", **kwargs):
            # This is the basic test we need to do several different ways to test successful operation.
            # Really what we are testing for is that this function calls through to authorized_request
            # in the way we expect it to. (We assume that authorized_request is itself already tested.)

            # First we set up our mock that doesn't do a lot other than generate a test datum to see if
            # that datum is returned correctly.
            mock_authorized_request.side_effect = mocked_authorized_request
            # Do the call, which will bottom out at our mock call
            res_w_key = ff_utils.get_metadata(test_item, key=ts.bar_env_auth_dict, check_queue=check_queue, **kwargs)
            # Check that the data flow back from our mock authorized_request call did what we expect
            assert res_w_key == {'@id': test_item_id, 'mock_data': n}
            # Check that the call out to the mock authorized_request is the thing we think.
            # In particular, we expect that
            # (a) this is a GET
            # (b) it has appropriate auth
            # (c) the auth specifies a server, which will be in the 'bar' environment because we used bar_env_auth_dict
            # (d) on certain calls, an additional query string (e.g., "?datastore=database") will be used in the
            #     function we're testing in some cases (e.g., because check_queue=True is used), so we take that suffix
            #     as an argument and test for it.
            mock_authorized_request.assert_called_with(ts.bar_env_url.rstrip('/') + test_item_id + expect_suffix,
                                                       auth=ts.bar_env_auth_dict,
                                                       verb='GET')

        with mock.patch.object(ff_utils, "stuff_in_queues", make_mocked_stuff_in_queues()):
            test_it(0, check_queue=False)

        with mock.patch.object(ff_utils, "stuff_in_queues", make_mocked_stuff_in_queues(return_value=True)):
            with raises_regexp(ValueError, "environment name"):
                test_it(1, check_queue=True, expect_suffix="?datastore=database")
            test_it(1, check_queue=True, expect_suffix="?datastore=database", ff_env=ts.bar_env)

        with mock.patch.object(ff_utils, "stuff_in_queues", make_mocked_stuff_in_queues(return_value=False)):
            with raises_regexp(ValueError, "environment name"):
                test_it(2, check_queue=True)
            test_it(2, check_queue=True, ff_env=ts.bar_env)

    # Now we test the error scenarios...

    # This tests what happens if we get an error response that doesn't contain valid JSON in the response body.
    # We test for a 500 here but this is intended to stand for any error response with an ill-formed body.

    with mock.patch.object(ff_utils, "authorized_request") as mock_authorized_request:
        # NOTE WELL: If there is no body JSON, an error is raised, but if there is body JSON, it is quietly returned
        #            as if it were what was requested. See next check.
        mock_authorized_request.side_effect = make_mocked_authorized_request_erring(500, json=None)
        with raises_regexp(Exception, "Cannot get json"):
            ff_utils.get_metadata(test_item, key=ts.bar_env_auth_dict, check_queue=False)

    # This tests that the error message JSON can be obtained if the body is well-formed.

    with mock.patch.object(ff_utils, "authorized_request") as mock_authorized_request:
        # NOTE WELL: If there is no body JSON, an error is raised, but if there is body JSON, it is quietly returned
        #            as if it were what was requested. See previous check.
        # TODO: Consider whether get_metadata() would be better off doing a .raise_For_status() -kmp 25-Oct-2020
        error_message_json = {'message': 'foo'}
        mock_authorized_request.side_effect = make_mocked_authorized_request_erring(500, json=error_message_json)
        assert ff_utils.get_metadata(test_item, key=ts.bar_env_auth_dict, check_queue=False) == error_message_json


def test_sls():
    in_and_out = {
        'my_id/': 'my_id/',
        '/my_id/': 'my_id/',
        '//my_id': 'my_id',
        'my/id': 'my/id',
        '/my/id': 'my/id',
    }
    for i, o in in_and_out.items():
        assert ff_utils._sls(i) == o


@pytest.mark.integratedx
@pytest.mark.flaky(max_runs=3)  # very flaky for some reason
def test_get_metadata_integrated(integrated_ff):
    # use this test biosource
    test_item = '331111bc-8535-4448-903e-854af460b254'
    res_w_key = ff_utils.get_metadata(test_item, key=integrated_ff['ff_key'])
    assert res_w_key['uuid'] == test_item
    orig_descrip = res_w_key['description']
    res_w_env = ff_utils.get_metadata(test_item, ff_env=integrated_ff['ff_env'])
    assert res_w_key == res_w_env
    # doesn't work with tuple auth if you don't provide env
    tuple_key = ff_utils.unified_authentication(integrated_ff['ff_key'], integrated_ff['ff_env'])
    with pytest.raises(Exception) as exec_info:
        ff_utils.get_metadata(test_item, key=tuple_key, ff_env=None)
    assert 'ERROR GETTING SERVER' in str(exec_info.value)

    # testing check_queues functionality requires patching
    ff_utils.patch_metadata({'description': 'test description'}, obj_id=test_item, key=integrated_ff['ff_key'])
    # add a bunch more stuff to the queue
    idx_body = json.dumps({'uuids': [test_item], 'target_queue': 'secondary'})
    for i in range(10):
        ff_utils.authorized_request(integrated_ff['ff_key']['server'] + '/queue_indexing',
                                    auth=integrated_ff['ff_key'], verb='POST', data=idx_body)
    time.sleep(5)  # let the queue catch up
    res_w_check = ff_utils.get_metadata(test_item, key=integrated_ff['ff_key'],
                                        ff_env=integrated_ff['ff_env'], check_queue=True)
    res_db = ff_utils.get_metadata(test_item, key=integrated_ff['ff_key'],
                                   add_on='datastore=database')
    assert res_db['description'] == 'test description'
    assert res_w_check['description'] == res_db['description']
    ff_utils.patch_metadata({'description': orig_descrip}, obj_id=test_item, key=integrated_ff['ff_key'])

    # check add_on
    assert isinstance(res_w_key['individual'], dict)
    res_obj = ff_utils.get_metadata(test_item, key=integrated_ff['ff_key'], add_on='frame=object')
    assert isinstance(res_obj['individual'], str)


@pytest.mark.integrated
@pytest.mark.flaky
def test_patch_metadata(integrated_ff):
    test_item = '331111bc-8535-4448-903e-854af460a254'
    original_res = ff_utils.get_metadata(test_item, key=integrated_ff['ff_key'])
    res = ff_utils.patch_metadata({'description': 'patch test'},
                                  obj_id=test_item, key=integrated_ff['ff_key'])
    assert res['@graph'][0]['description'] == 'patch test'
    res2 = ff_utils.patch_metadata({'description': original_res['description'], 'uuid': original_res['uuid']},
                                   key=integrated_ff['ff_key'])
    assert res2['@graph'][0]['description'] == original_res['description']

    with pytest.raises(Exception) as exec_info:
        ff_utils.patch_metadata({'description': 'patch test'}, key=integrated_ff['ff_key'])
    assert 'ERROR getting id' in str(exec_info.value)


@pytest.mark.integrated
@pytest.mark.flaky
def test_post_delete_purge_links_metadata(integrated_ff):
    """
    Combine all of these tests because they logically fit
    """
    post_data = {'biosource_type': 'immortalized cell line', 'award': '1U01CA200059-01',
                 'lab': '4dn-dcic-lab'}
    post_res = ff_utils.post_metadata(post_data, 'biosource', key=integrated_ff['ff_key'])
    post_item = post_res['@graph'][0]
    assert 'uuid' in post_item
    assert post_item['biosource_type'] == post_data['biosource_type']
    # make sure there is a 409 when posting to an existing item
    post_data['uuid'] = post_item['uuid']
    with pytest.raises(Exception) as exec_info:
        ff_utils.post_metadata(post_data, 'biosource', key=integrated_ff['ff_key'])
    assert '409' in str(exec_info.value)  # 409 is conflict error

    # make a biosample that links to the biosource
    bios_data = {'biosource': [post_data['uuid']], 'status': 'deleted',
                 'lab': '4dn-dcic-lab', 'award': '1U01CA200059-01'}
    bios_res = ff_utils.post_metadata(bios_data, 'biosample', key=integrated_ff['ff_key'])
    bios_item = bios_res['@graph'][0]
    assert 'uuid' in bios_item

    # delete the biosource
    del_res = ff_utils.delete_metadata(post_item['uuid'], key=integrated_ff['ff_key'])
    assert del_res['status'] == 'success'
    assert del_res['@graph'][0]['status'] == 'deleted'

    # test get_metadata_links function (this will ensure everything is indexed, as well)
    links = []
    while not links or ff_utils.stuff_in_queues(integrated_ff['ff_env'], True):
        time.sleep(5)
        post_links = ff_utils.get_metadata_links(post_item['uuid'], key=integrated_ff['ff_key'])
        links = post_links.get('uuids_linking_to', [])
    assert len(links) == 1
    assert links[0]['uuid'] == bios_item['uuid']
    assert links[0]['field'] == 'biosource[0].uuid'

    # purge biosource first, which will failed because biosample is still linked
    purge_res1 = ff_utils.purge_metadata(post_item['uuid'], key=integrated_ff['ff_key'])
    assert purge_res1['status'] == 'error'
    assert bios_item['uuid'] in [purge['uuid'] for purge in purge_res1['comment']]

    # purge biosample and then biosource
    purge_res2 = ff_utils.purge_metadata(bios_item['uuid'], key=integrated_ff['ff_key'])
    assert purge_res2['status'] == 'success'

    # wait for indexing to catch up
    while len(links) > 0 or ff_utils.stuff_in_queues(integrated_ff['ff_env'], True):
        time.sleep(5)
        post_links = ff_utils.get_metadata_links(post_item['uuid'], key=integrated_ff['ff_key'])
        links = post_links.get('uuids_linking_to', [])
    assert len(links) == 0

    purge_res3 = ff_utils.purge_metadata(post_item['uuid'], key=integrated_ff['ff_key'])
    assert purge_res3['status'] == 'success'
    # make sure it is purged
    with pytest.raises(Exception) as exec_info:
        ff_utils.get_metadata(post_item['uuid'], key=integrated_ff['ff_key'],
                              add_on='datastore=database')
    assert 'The resource could not be found' in str(exec_info.value)


@pytest.mark.integrated
@pytest.mark.flaky
def test_upsert_metadata(integrated_ff):
    test_data = {'biosource_type': 'immortalized cell line',
                 'award': '1U01CA200059-01', 'lab': '4dn-dcic-lab'}
    upsert_res = ff_utils.upsert_metadata(test_data, 'biosource', key=integrated_ff['ff_key'])
    upsert_item = upsert_res['@graph'][0]
    assert 'uuid' in upsert_item
    assert upsert_item['biosource_type'] == test_data['biosource_type']
    # make sure the item is patched if already existing
    test_data['description'] = 'test description'
    test_data['uuid'] = upsert_item['uuid']
    test_data['status'] = 'deleted'
    upsert_res2 = ff_utils.upsert_metadata(test_data, 'biosource', key=integrated_ff['ff_key'])
    upsert_item2 = upsert_res2['@graph'][0]
    assert upsert_item2['description'] == 'test description'
    assert upsert_item2['status'] == 'deleted'
    with pytest.raises(Exception) as exec_info:
        ff_utils.upsert_metadata(test_data, 'biosourc', key=integrated_ff['ff_key'])
    assert 'Bad status code' in str(exec_info.value)


def make_mocked_item(n):
    return {'uuid': str(n)}


MOCKED_SEARCH_COUNT = 130
MOCKED_SEARCH_ITEMS = [make_mocked_item(uuid_counter) for uuid_counter in range(MOCKED_SEARCH_COUNT)]


def constant_mocked_search_items():
    return MOCKED_SEARCH_ITEMS


class InsertingMockedSearchItems:
    """
    This class is for use in simulating a situation in which an insertion occurs between calls to a search.
    The base set of data from which simulated results are taken will change on the second (POS=1) call,
    with an additional element being inserted at position 0 on that call and persisting for subsequent calls.

    For example, when querying /search/?type=File&from=5&limit=53 there will be two internal queries:

    mocked search http://fourfront-mastertest.9wzadzju3p.us-east-1.elasticbeanstalk.com//search/?type=File&limit=50&sort=-date_created&from=0
    yields Search [{'uuid': '0'}, ..., {'uuid': '49'}] # 50 items
    mocked search http://fourfront-mastertest.9wzadzju3p.us-east-1.elasticbeanstalk.com//search/?type=File&limit=50&sort=-date_created&from=50
    yields Search [{'uuid': '49'}, ..., {'uuid': '98'}] # 50 items

    but only 52 items will be returned because the 50th element, {'uuid': '49'}, is returned twice and most of the
    50 items returned on the second call will be ignored, as only 3 additional items are needed to make 53.
    (If all 50 were duplicates, a third call would have been done seeking more elements.)

    Likewise when querying /search/?type=File&limit=53 (note in this case no from=5) there will be two internal queries:

    mocked search http://fourfront-mastertest.9wzadzju3p.us-east-1.elasticbeanstalk.com//search/?type=File&from=5&limit=50&sort=-date_created
    yields Search [{'uuid': '5'}, ..., {'uuid': '54'}] # 50 items
    mocked search http://fourfront-mastertest.9wzadzju3p.us-east-1.elasticbeanstalk.com//search/?type=File&from=55&limit=50&sort=-date_created
    yields Search [{'uuid': '54'}, ..., {'uuid': '103'}] # 50 items

    and again 52 items will be returned, but this time it's {'uuid': 54'} that is the 50th element. But in this case,
    as with the other, most of the second set of results will be discarded, as only 3 additional items are
    needed to make 53.
    """  # noQA - some long lines in this doc string that are URLs and not easily broken

    POS = 0
    ITEMS = MOCKED_SEARCH_ITEMS.copy()

    @classmethod
    def reset(cls):
        """
        This function can be called proactively to reinitialize the mock.
        We're using a class method, not an instance method, because this simplifies the data flow
        and our use case is sufficiently simple that we don't need lots of these things at once.
        """
        cls.POS = 0
        cls.ITEMS = MOCKED_SEARCH_ITEMS.copy()

    @classmethod
    def handler(cls):
        """
        This function can be called to get the list of data representing the ES data store.
        We've allocated 130 of these in all, so the 'data store' out of which subsequences of results
        are selected will look like:
        [{'uuid': '1'}, {'uuid': '2'}, ..., {'uuid': '129'}]
        On the second call, an item numbered '130' will be inserted at position 0, so the 'data store'
        out of which subsequences of results are selected will look like:
        [{'uuid': '130'}, {'uuid': '1'}, {'uuid': '2'}, ..., {'uuid': '129'}]
        """
        if cls.POS == 1:
            extra_item = make_mocked_item(MOCKED_SEARCH_COUNT)
            cls.ITEMS.insert(0, extra_item)
        cls.POS += 1
        return cls.ITEMS


def make_mocked_search(item_maker=None):

    if item_maker is None:
        item_maker = constant_mocked_search_items

    def mocked_search(url, auth, ff_env, retry_fxn):
        ignored(auth, ff_env, retry_fxn)  # Not the focus of this mock
        parsed = urlsplit(url)
        params = dict(parse_qsl(parsed.query))
        # There are some noQA markers here because PyCharm wrongly infers that the params values are expected
        # to be type 'bytes'. -kmp 15-Jan-2021
        assert params['type'] == 'File', "This mock doesn't handle type=%s" % params['type']  # noQA
        assert params['sort'] == '-date_created', "This mock doesn't handle sort=%s" % params['sort']  # noQA
        search_from = int(params['from'])  # noQA
        search_limit = int(params['limit'])  # noQA
        search_items = item_maker()[search_from:search_from + search_limit]
        if parsed.path.endswith("/browse/"):
            restype = 'Browse'
        elif parsed.path.endswith("/search/"):
            restype = 'Search'
        else:
            raise NotImplementedError("Need a better mock.")
        print("mocked search", url)
        if len(search_items) > 5:
            print("yields %s [%s, ..., %s] # %s items"
                  % (restype, search_items[0], search_items[-1], len(search_items)))
        else:
            print("yields %s %s # %s items" % (restype, search_items, len(search_items)))
        return MockResponse(json={'@type': restype, '@graph': search_items})
    return mocked_search


@pytest.mark.unit
@pytest.mark.parametrize('url', ['', 'to_become_full_url'])
def test_search_metadata_unit(integrated_ff, url):
    """
    Test normal case of search_metadata involving a search always being based on a consistently indexed set of data.
    """
    with mock.patch.object(ff_utils, "authorized_request", make_mocked_search()):
        check_search_metadata(integrated_ff, url)


@pytest.mark.unit
@pytest.mark.parametrize('url', ['', 'to_become_full_url'])
def test_search_metadata_inserting_unit(integrated_ff, url):
    """
    Tests unusual case of search_metadata (C4-336) returning duplicated items.
    See detailed explanation in ``InsertingMockedSearchItems``.
    """
    InsertingMockedSearchItems.reset()
    with mock.patch.object(ff_utils, "authorized_request",
                           make_mocked_search(item_maker=InsertingMockedSearchItems.handler)):
        check_search_metadata(integrated_ff, url, expect_shortfall=True)


@pytest.mark.integratedx
@pytest.mark.flaky
@pytest.mark.parametrize('url', ['', 'to_become_full_url'])
def test_search_metadata_integrated(integrated_ff, url):
    check_search_metadata(integrated_ff, url)


def check_search_metadata(integrated_ff, url, expect_shortfall=False):
    """
    This is a common function shared between unit and integration tests for search_metadata.
    """
    if url != '':  # replace stub with actual url from integrated_ff
        url = integrated_ff['ff_key']['server'] + '/'

    # Note that we do some some .reset() calls on a mock that are not needed when servicing the integration test,
    # but they are harmless and it seemed pointless to make it conditional. -kmp 15-Jan-2021
    InsertingMockedSearchItems.reset()
    search_res = ff_utils.search_metadata(url + 'search/?limit=all&type=File', key=integrated_ff['ff_key'])
    assert isinstance(search_res, list)
    # this will fail if items have not yet been indexed
    assert len(search_res) > 0
    # make sure uuids are unique
    check_duplicated_items_by_key('uuid', search_res, url=url, formatter=lambda x: json.dumps(x, indent=2))
    # search_uuids = set([item['uuid'] for item in search_res])
    # assert len(search_uuids) == len(search_res)

    InsertingMockedSearchItems.reset()
    search_res_slash = ff_utils.search_metadata(url + '/search/?limit=all&type=File', key=integrated_ff['ff_key'])
    assert isinstance(search_res_slash, list)
    assert len(search_res_slash) == len(search_res)

    # search with a limit
    InsertingMockedSearchItems.reset()
    search_res_limit = ff_utils.search_metadata(url + '/search/?limit=3&type=File', key=integrated_ff['ff_key'])
    assert len(search_res_limit) == 3

    # search with a limit from a certain entry
    InsertingMockedSearchItems.reset()
    search_res_from_limit = ff_utils.search_metadata(url + '/search/?type=File&limit=53',
                                                     key=integrated_ff['ff_key'])
    assert len(search_res_from_limit) == 52 if expect_shortfall else 53

    # search with a limit from a certain entry
    InsertingMockedSearchItems.reset()
    search_res_from_limit = ff_utils.search_metadata(url + '/search/?type=File&from=5&limit=53',
                                                     key=integrated_ff['ff_key'])
    assert len(search_res_from_limit) == 52 if expect_shortfall else 53

    # search with a filter
    InsertingMockedSearchItems.reset()
    search_res_filt = ff_utils.search_metadata(url + '/search/?limit=3&type=File&file_type=reads',
                                               key=integrated_ff['ff_key'])
    assert len(search_res_filt) > 0

    # test is_generator=True
    InsertingMockedSearchItems.reset()
    search_res_gen = ff_utils.search_metadata(url + '/search/?limit=3&type=File&file_type=reads',
                                              key=integrated_ff['ff_key'], is_generator=True)
    assert isinstance(search_res_gen, GeneratorType)
    gen_res = [v for v in search_res_gen]  # run the gen
    assert len(gen_res) == 3

    # do same search as limit but use the browse endpoint instead
    InsertingMockedSearchItems.reset()
    browse_res_limit = ff_utils.search_metadata(url + '/browse/?limit=3&type=File', key=integrated_ff['ff_key'])
    assert len(browse_res_limit) == 3


@pytest.mark.integrated
def test_search_metadata_with_generator(integrated_ff):
    """ Test using search_metadata with a generator """
    url = integrated_ff['ff_key']['server'] + '/'

    # helper to validate generator
    def validate_gen(gen, expected):
        found = 0
        for _ in gen:
            found += 1
        assert found == expected

    # do limit = 10 search, iterate through generator, should have 10 results
    search_gen = ff_utils.search_metadata(url + 'search/?limit=10&type=File',
                                          key=integrated_ff['ff_key'],
                                          is_generator=True)
    validate_gen(search_gen, 10)
    # do limit = 7 search, iterate through generator, should have 7 results
    search_gen = ff_utils.search_metadata(url + 'search/?limit=7&type=File',
                                          key=integrated_ff['ff_key'],
                                          is_generator=True)
    validate_gen(search_gen, 7)
    # do limit = 3 search on users
    search_gen = ff_utils.search_metadata(url + 'search/?limit=3&type=User',
                                          key=integrated_ff['ff_key'],
                                          is_generator=True)
    validate_gen(search_gen, 3)


@pytest.mark.integrated
def test_get_es_metadata_with_generator(integrated_ff):
    """ Tests using get_es_metadata with the generator option """
    url = integrated_ff['ff_key']['server'] + '/'
    search_res = ff_utils.search_metadata(url + 'search/?limit=15&type=File', key=integrated_ff['ff_key'])
    # get 15 random uuids, pass into get_es_metadata, iterate through the gen
    uuids = [entry['uuid'] for entry in search_res]
    metadata_gen = ff_utils.get_es_metadata(uuids, key=integrated_ff['ff_key'], is_generator=True)
    found = 0
    for entry in metadata_gen:
        assert entry['uuid'] in uuids
        found += 1
    assert found == 15


@pytest.mark.integrated
@pytest.mark.flaky
def test_get_search_generator(integrated_ff):
    search_url = integrated_ff['ff_key']['server'] + '/search/?type=FileFastq'
    generator1 = ff_utils.get_search_generator(search_url, auth=integrated_ff['ff_key'], page_limit=25)
    list_gen1 = list(generator1)
    assert len(list_gen1) > 0
    for idx, page in enumerate(list_gen1):
        assert isinstance(page, list)
        if idx < len(list_gen1) - 1:
            assert len(page) == 25
        else:
            assert len(page) > 0
    all_gen1 = [page for pages in list_gen1 for page in pages]  # noqa
    generator2 = ff_utils.get_search_generator(search_url, auth=integrated_ff['ff_key'], page_limit=50)
    list_gen2 = list(generator2)
    assert len(list_gen1) > len(list_gen2)
    all_gen2 = [page for pages in list_gen2 for page in pages]  # noqa
    assert len(all_gen1) == len(all_gen2)
    # use a limit in the search
    search_url += '&limit=21'
    generator3 = ff_utils.get_search_generator(search_url, auth=integrated_ff['ff_key'])
    list_gen3 = list(generator3)
    all_gen3 = [page for pages in list_gen3 for page in pages]  # noqa
    assert len(all_gen3) == 21
    # make sure that all results are unique
    all_gen3_uuids = set([item['uuid'] for item in all_gen3])
    assert len(all_gen3_uuids) == len(all_gen3)


@pytest.mark.integrated
@pytest.mark.flaky
def test_get_es_metadata(integrated_ff):
    # use this test biosource and biosample
    test_biosource = '331111bc-8535-4448-903e-854af460b254'
    test_biosample = '111112bc-1111-4448-903e-854af460b123'
    res = ff_utils.get_es_metadata([test_biosource, test_biosample], key=integrated_ff['ff_key'])
    assert len(res) == 2
    if res[0]['uuid'] == test_biosource:
        biosource_res, biosample_res = res
    else:
        biosample_res, biosource_res = res
    assert biosource_res['uuid'] == test_biosource
    assert biosource_res['item_type'] == 'biosource'
    assert isinstance(biosource_res['embedded'], dict)
    assert isinstance(biosource_res['links'], dict)
    assert biosample_res['uuid'] == test_biosample
    assert biosample_res['item_type'] == 'biosample'

    # you can pass in your own elasticsearch client or build it here
    es_url = ff_utils.get_health_page(key=integrated_ff['ff_key'])['elasticsearch']
    es_client = es_utils.create_es_client(es_url, use_aws_auth=True)
    res2 = ff_utils.get_es_metadata([test_biosource], es_client=es_client,
                                    key=integrated_ff['ff_key'])
    assert len(res2) == 1
    assert res2[0]['uuid'] == biosource_res['uuid']

    # you can get more than 10 items. compare a search result to es result
    # use 55 because the default pagination in the es generator is 50 items
    search_res = ff_utils.search_metadata('/search/?limit=55&type=Item&frame=object',
                                          key=integrated_ff['ff_key'])
    search_uuids = [item['uuid'] for item in search_res]
    assert len(search_uuids) == 55
    es_res = ff_utils.get_es_metadata(search_uuids, es_client=es_client,
                                      key=integrated_ff['ff_key'])
    es_search_uuids = [item['uuid'] for item in es_res]
    assert len(es_res) == len(search_res)
    assert set(search_uuids) == set(es_search_uuids)

    # bad item returns empty list
    res = ff_utils.get_es_metadata(['blahblah'], key=integrated_ff['ff_key'])
    assert res == []

    # make sure searches work with pagination set at 100 (default)
    all_items = ff_utils.search_metadata('/search/?type=Item&frame=object', key=integrated_ff['ff_key'])
    all_uuids = [item['uuid'] for item in all_items]
    all_es = ff_utils.get_es_metadata(all_uuids, key=integrated_ff['ff_key'])
    assert len(all_es) == len(all_uuids)
    all_es_uuids = [item['uuid'] for item in all_es]
    assert set(all_es_uuids) == set(all_uuids)

    # make sure filters work with the search
    bios_in_rev = ff_utils.search_metadata('/search/?type=Biosample&frame=object&status=in+review+by+lab',
                                           key=integrated_ff['ff_key'])
    bios_replaced = ff_utils.search_metadata('/search/?type=Biosample&frame=object&status=replaced',
                                             key=integrated_ff['ff_key'])
    bios_uuids = [item['uuid'] for item in bios_in_rev + bios_replaced]
    all_uuids.extend(bios_uuids)  # add the replaced biosample uuids
    filters = {'status': ['in review by lab', 'replaced'], '@type': ['Biosample']}
    bios_es = ff_utils.get_es_metadata(all_uuids, filters=filters, key=integrated_ff['ff_key'])
    assert set([item['uuid'] for item in bios_es]) == set(bios_uuids)

    bios_neg_search = ('/search/?type=Biosample&frame=object&status=in+review+by+lab'
                       '&modifications.modification_type!=Other')
    bios_neg_res = ff_utils.search_metadata(bios_neg_search, key=integrated_ff['ff_key'])
    filters2 = {'status': ['in review by lab'], 'modifications.modification_type': ['!Other'], '@type': ['Biosample']}
    bios_neg_es = ff_utils.get_es_metadata(all_uuids, filters=filters2, key=integrated_ff['ff_key'])
    assert set([item['uuid'] for item in bios_neg_es]) == set(item['uuid'] for item in bios_neg_res)
    # raise error if filters is not dict
    with pytest.raises(Exception) as exec_info:
        ff_utils.get_es_metadata(all_uuids, filters=['not', 'a', 'dict'],
                                 key=integrated_ff['ff_key'])
    assert 'Invalid filters for get_es_metadata' in str(exec_info.value)

    # test is_generator=True, compare to bios_neg_res
    bios_neg_gen = ff_utils.get_es_metadata(all_uuids, filters=filters2,
                                            is_generator=True,
                                            key=integrated_ff['ff_key'])
    assert isinstance(bios_neg_gen, GeneratorType)
    # run the gen
    gen_res = [v for v in bios_neg_gen]
    assert set([item['uuid'] for item in bios_neg_es]) == set(item['uuid'] for item in gen_res)

    # test sources
    bios_neg_sources = ff_utils.get_es_metadata(all_uuids, filters=filters2,
                                                sources=['object.*', 'embedded.biosource.uuid'],
                                                key=integrated_ff['ff_key'])
    for item in bios_neg_sources:
        # get expected frame=object keys from matching biosample from search res
        matching_bios = [bio for bio in bios_neg_res if bio['uuid'] == item['object']['uuid']]
        expected_obj_keys = set(matching_bios[0].keys())
        assert set(item.keys()) == {'object', 'embedded'}
        # expect all keys in object frame, since we used object.*
        assert set(item['object'].keys()) == expected_obj_keys
        assert set(item['embedded'].keys()) == {'biosource'}
        # expected only uuid in embedded.biosource
        for biosource in item['embedded']['biosource']:
            assert set(biosource.keys()) == {'uuid'}
    # confirm that all items were found
    assert set([item['uuid'] for item in bios_neg_es]) == set(item['object']['uuid'] for item in bios_neg_sources)
    # raise error if sources is not list
    with pytest.raises(Exception) as exec_info2:
        ff_utils.get_es_metadata(all_uuids, filters=filters2,
                                 sources='not a list',
                                 key=integrated_ff['ff_key'])
    assert 'Invalid sources for get_es_metadata' in str(exec_info2.value)


@pytest.mark.integrated
@pytest.mark.flaky
def test_get_es_search_generator(integrated_ff):
    # get es_client info from the health page
    health = ff_utils.get_health_page(key=integrated_ff['ff_key'])
    es_url = health['elasticsearch']
    es_client = es_utils.create_es_client(es_url, use_aws_auth=True)
    es_query = {'query': {'match_all': {}}, 'sort': [{'_uid': {'order': 'desc'}}]}
    # search for all fastqs with a low pagination size
    index = health['namespace'] + 'file_fastq'
    es_gen = ff_utils.get_es_search_generator(es_client, index,
                                              es_query, page_size=7)
    list_gen = list(es_gen)
    assert len(list_gen) > 0
    for idx, page in enumerate(list_gen):
        assert isinstance(page, list)
        # last page may be empty if # ontology terms is divisible by 7
        if idx < len(list_gen) - 1:
            assert len(page) == 7
    all_es_uuids = set([page['_source']['uuid'] for pages in list_gen for page in pages])  # noqa
    # make sure all items are unique and len matches ff search
    search_res = ff_utils.search_metadata('/search/?type=FileFastq&frame=object',
                                          key=integrated_ff['ff_key'])
    search_uuids = set(hit['uuid'] for hit in search_res)
    assert all_es_uuids == search_uuids


@pytest.mark.integrated
@pytest.mark.flaky
def test_get_health_page(integrated_ff):
    health_res = ff_utils.get_health_page(key=integrated_ff['ff_key'])
    assert health_res and 'error' not in health_res
    assert 'elasticsearch' in health_res
    assert 'database' in health_res
    assert health_res['beanstalk_env'] == integrated_ff['ff_env']
    # try with ff_env instead of key
    health_res2 = ff_utils.get_health_page(ff_env=integrated_ff['ff_env'])
    assert health_res2 and 'error' not in health_res2
    assert health_res2['elasticsearch'] == health_res['elasticsearch']
    # make sure it's error tolerant
    bad_health_res = ff_utils.get_health_page(ff_env='not_an_env')
    assert bad_health_res and 'error' in bad_health_res


@pytest.mark.integrated
@pytest.mark.flaky
def test_get_schema_names(integrated_ff):
    schema_names = ff_utils.get_schema_names(key=integrated_ff['ff_key'],
                                             ff_env=integrated_ff['ff_env'])
    # assert that it gets quite some schemas
    assert len(schema_names) > 75
    assert schema_names['FileFastq'] == 'file_fastq'
    assert schema_names['ExperimentSetReplicate'] == 'experiment_set_replicate'


@pytest.mark.integrated
@pytest.mark.flaky
def test_expand_es_metadata(integrated_ff):
    test_list = ['7f9eb396-5c1a-4c5e-aebf-28ea39d6a50f']
    key, ff_env = integrated_ff['ff_key'], integrated_ff['ff_env']
    store, uuids = ff_utils.expand_es_metadata(test_list, key=key, ff_env=ff_env)
    for pos_case in ['file_processed', 'user', 'file_format', 'award', 'lab']:
        assert pos_case in store
    for neg_case in ['workflow_run_awsem', 'workflow', 'file_reference', 'software', 'workflow_run_sbg',
                     'quality_metric_pairsqc', 'quality_metric_fastqc']:
        assert neg_case not in store
    # make sure the frame is raw (default)
    test_item = store['file_processed'][0]
    assert test_item['lab'].startswith('828cd4fe')


@pytest.mark.integrated
def test_get_item_facets(integrated_ff):
    """ Tests that we can resolve information on facets """
    key, ff_env = integrated_ff['ff_key'], integrated_ff['ff_env']
    item_type = 'experiment_set_replicate'
    facets = ff_utils.get_item_facets(item_type, key=key, ff_env=ff_env)
    assert 'Lab' in facets
    assert 'Center' in facets
    assert 'Set Type' in facets
    assert 'Internal Release Date' in facets


@pytest.mark.integrated
def test_get_item_facet_values(integrated_ff):
    """ Tests that we correctly grab facets and all their possible values """
    key, ff_env = integrated_ff['ff_key'], integrated_ff['ff_env']
    item_type = 'experiment_set_replicate'
    facets = ff_utils.get_item_facet_values(item_type, key=key, ff_env=ff_env)
    assert 'Project' in facets
    assert '4DN' in facets['Project']
    assert 'Assay Details' in facets
    assert 'Target: YFG protein' in facets['Assay Details']
    assert 'Status' in facets


@pytest.mark.integrated
def test_faceted_search_exp_set(integrated_ff):
    """ Tests the experiment set search features using mastertest """
    key, ff_env = integrated_ff['ff_key'], integrated_ff['ff_env']
    all_facets = ff_utils.get_item_facets('experiment_set_replicate', key=key, ff_env=ff_env)
    for_all = {'key': key, 'ff_env': ff_env, 'item_facets': all_facets}

    # helper method that verifies a top level facet value
    def validate_items(items, facet, expected):
        facet_levels = facet.split('.')
        for item in items:
            it = item
            for level in facet_levels:
                it = it[level]
            assert it == expected

    project = {'Project': '4DN'}
    project.update(for_all)
    resp = ff_utils.faceted_search(**project)
    assert len(resp) == 8
    validate_items(resp, all_facets['Project'], '4DN')
    lab = {'Lab': '4DN Testing Lab'}
    lab.update(for_all)
    resp = ff_utils.faceted_search(**lab)
    assert len(resp) == 12
    validate_items(resp, all_facets['Lab'], '4DN Testing Lab')
    exp_cat = {'Experiment Category': 'Microscopy'}
    exp_cat.update(for_all)
    resp = ff_utils.faceted_search(**exp_cat)
    assert len(resp) == 1
    exp_type = {'Experiment Type': 'Dilution Hi-C'}
    exp_type.update(for_all)
    resp = ff_utils.faceted_search(**exp_type)
    assert len(resp) == 3
    dataset = {'Dataset': 'No value'}
    dataset.update(for_all)
    resp = ff_utils.faceted_search(**dataset)
    assert len(resp) == 9
    sample_type = {'Sample Type': 'immortalized cells'}
    sample_type.update(for_all)
    resp = ff_utils.faceted_search(**sample_type)
    assert len(resp) == 12
    sample_cat = {'Sample Category': 'In vitro Differentiation'}
    sample_cat.update(for_all)
    resp = ff_utils.faceted_search(**sample_cat)
    assert len(resp) == 1
    sample = {'Sample': 'GM12878'}
    sample.update(for_all)
    resp = ff_utils.faceted_search(**sample)
    assert len(resp) == 12
    tissue_src = {'Tissue Source': 'endoderm'}
    tissue_src.update(for_all)
    resp = ff_utils.faceted_search(**tissue_src)
    assert len(resp) == 1
    pub = {'Publication': 'No value'}
    pub.update(for_all)
    resp = ff_utils.faceted_search(**pub)
    assert len(resp) == 9
    mods = {'Modifications': 'Stable Transfection'}
    mods.update(for_all)
    resp = ff_utils.faceted_search(**mods)
    assert len(resp) == 6
    treats = {'Treatments': 'RNAi'}
    treats.update(for_all)
    resp = ff_utils.faceted_search(**treats)
    assert len(resp) == 6
    assay_details = {'Assay Details': 'No value'}
    assay_details.update(for_all)
    resp = ff_utils.faceted_search(**assay_details)
    assert len(resp) == 1
    status = {'Status': 'released'}
    status.update(for_all)
    resp = ff_utils.faceted_search(**status)
    assert len(resp) == 12
    warnings = {'Warnings': 'No value'}
    warnings.update(for_all)
    resp = ff_utils.faceted_search(**warnings)
    assert len(resp) == 5
    both_projects = {'Project': '4DN|External'}
    both_projects.update(for_all)
    resp = ff_utils.faceted_search(**both_projects)
    assert len(resp) == 13
    both_labs = {'Lab': '4DN Testing Lab|Some Other Guys lab'}
    both_labs.update(for_all)
    resp = ff_utils.faceted_search(**both_labs)
    assert len(resp) == 13
    proj_exp_type = {'Project': '4DN', 'Experiment Type': 'Dilution Hi-C'}
    proj_exp_type.update(for_all)
    resp = ff_utils.faceted_search(**proj_exp_type)
    assert len(resp) == 2
    proj_exp_type = {'Project': '4DN|External', 'Experiment Type': 'Dilution Hi-C|2-stage Repli-seq'}
    proj_exp_type.update(for_all)
    resp = ff_utils.faceted_search(**proj_exp_type)
    assert len(resp) == 5
    proj_exp_sam = {'Project': '4DN|External',
                    'Experiment Type': 'Dilution Hi-C|2-stage Repli-seq',
                    'Sample Type': 'in vitro differentiated cells'}
    proj_exp_sam.update(for_all)
    resp = ff_utils.faceted_search(**proj_exp_sam)
    assert len(resp) == 1
    exp_sam = {'Experiment Type': 'ATAC-seq', 'Sample': 'primary cell'}
    exp_sam.update(for_all)
    resp = ff_utils.faceted_search(**exp_sam)
    assert len(resp) == 1
    exp_sam_data = {'Experiment Category': 'Sequencing', 'Sample': 'GM12878',
                    'Dataset': 'Z et al. 2-Stage Repliseq'}
    exp_sam_data.update(for_all)
    resp = ff_utils.faceted_search(**exp_sam_data)
    assert len(resp) == 2


@pytest.mark.integrated
def test_faceted_search_users(integrated_ff):
    """
    Tests faceted_search with users intead of experiment set
    Tests a negative search as well
    """
    key, ff_env = integrated_ff['ff_key'], integrated_ff['ff_env']
    all_facets = ff_utils.get_item_facets('user', key=key, ff_env=ff_env)
    any_affiliation = {'item_type': 'user',
                       'key': key,
                       'ff_env': ff_env,
                       'item_facets': all_facets}
    resp = ff_utils.faceted_search(**any_affiliation)
    total = len(resp)
    print("total=", total)  # Probably a number somewhere near 30
    assert 10 < total < 50
    affiliation = {'item_type': 'user',
                   'Affiliation': '4DN Testing Lab',
                   'key': key,
                   'ff_env': ff_env,
                   'item_facets': all_facets}
    resp = ff_utils.faceted_search(**affiliation)
    affiliated = len(resp)
    print("affiliated=", affiliated)  # Probably a number near 5
    assert affiliated < 10
    neg_affiliation = {'item_type': 'user',
                       'Affiliation': '-4DN Testing Lab',
                       'key': key,
                       'ff_env': ff_env,
                       'item_facets': all_facets}
    resp = ff_utils.faceted_search(**neg_affiliation)
    unaffiliated = len(resp)  # Probably a number near 25, but in any case the length of the complement set
    assert unaffiliated == total - affiliated
    neg_affiliation = {'item_type': 'user',
                       'Affiliation': '-4DN Testing Lab',
                       'key': key,
                       'ff_env': ff_env,
                       'item_facets': all_facets,
                       'Limit': '10'}  # test limit
    resp = ff_utils.faceted_search(**neg_affiliation)
    assert len(resp) == 10


@pytest.mark.unit
def test_fetch_qc_metrics_logic_unit(mocked_replicate_experiment):
    """
    Tests that the fetch_qc_metrics function is being used correctly inside the get_associated_qc_metrics function
    """
    with mock.patch("dcicutils.ff_utils.get_metadata") as mock_get_metadata:
        mock_get_metadata.side_effect = mocked_get_metadata_from_data_files
        result = ff_utils.fetch_files_qc_metrics(
            mocked_get_metadata_from_data_files("331106bc-8535-3338-903e-854af460b544"),
            associated_files=['other_processed_files'],
            ignore_typical_fields=False)
        assert '131106bc-8535-4448-903e-854af460b000' in result
        assert '131106bc-8535-4448-903e-854abbbbbbbb' in result


@pytest.mark.integratedx
def test_fetch_qc_metrics_logic_integrated(mocked_replicate_experiment):
    """
    Tests that the fetch_qc_metrics function is being used correctly inside the get_associated_qc_metrics function
    """
    result = ff_utils.fetch_files_qc_metrics(
        ff_utils.get_metadata("331106bc-8535-3338-903e-854af460b544", ff_env='fourfront-mastertest'),
        ff_env='fourfront-mastertest',
        associated_files=['other_processed_files'],
        ignore_typical_fields=False)
    assert '131106bc-8535-4448-903e-854af460b000' in result
    assert '131106bc-8535-4448-903e-854abbbbbbbb' in result


def mocked_get_metadata_from_data_files(uuid, **kwargs):
    result = get_mocked_result(kind='mocked metadata', dirname='test_items', uuid=uuid, ignored_kwargs=kwargs)
    # Guard against bad mocks.
    assert 'uuid' in result, f"The result of a mocked get_metadata call for {uuid} has no 'uuid' key."
    assert result['uuid'] == uuid, f"Result of a mocked get_metadata({uuid!r}) call has wrong uuid {result['uuid']!r}."
    return result


def get_mocked_result(*, kind, dirname, uuid, ignored_kwargs=None):
    data_file = os.path.join(_DATA_DIR, dirname, uuid + '.json')
    print(f"Getting {kind} for {uuid} ignoring kwargs={ignored_kwargs} from {data_file}")
    with open(data_file, 'r') as fp:
        return json.load(fp)


@pytest.mark.unit
def test_get_qc_metrics_logic_unit():
    """
    End to end test on 'get_associated_qc_metrics' to check the logic of the fuction to make sure
    it is getting the qc metrics.
    """
    with mock.patch("dcicutils.ff_utils.get_metadata") as mock_get_metadata:
        mock_get_metadata.side_effect = mocked_get_metadata_from_data_files
        print()  # start output on a fresh line
        input_uuid = "431106bc-8535-4448-903e-854af460b260"
        result = ff_utils.get_associated_qc_metrics(input_uuid)
        assert "131106bc-8535-4448-903e-854abbbbbbbb" in result  # quick pre-test
        assert result == get_mocked_result(kind='QC metrics',
                                           dirname='test_qc_metrics',
                                           uuid=input_uuid)


@pytest.mark.integrated
def test_get_qc_metrics(integrated_ff):
    """
    Tests that we correctly extract qc metric uuids (and therefore items) from the helper
    """

    key, ff_env = integrated_ff['ff_key'], integrated_ff['ff_env']
    uuid = '331106bc-8535-3338-903e-854af460b544'
    qc_metrics = ff_utils.get_associated_qc_metrics(uuid, key=key, ff_env=ff_env)
    assert len(qc_metrics.keys()) == 1
    assert '131106bc-8535-4448-903e-854abbbbbbbb' in qc_metrics
    target_qc = qc_metrics['131106bc-8535-4448-903e-854abbbbbbbb']
    assert 'QualityMetric' in target_qc['values']['@type']
    assert target_qc['organism'] == 'human'
    assert target_qc['experiment_type'] == 'Dilution Hi-C'
    assert target_qc['experiment_subclass'] == 'Hi-C'
    assert target_qc['source_file_association'] == 'processed_files'
    assert target_qc['source_experiment'] == '4DNEXO67APV1'
    assert target_qc['source_experimentSet'] == '4DNESOPFAAA1'
    assert target_qc['biosource_summary'] == "GM12878"

    kwargs = {  # do same as above w/ kwargs, specify to include raw files this time
        'key': key,
        'ff_env': ff_env,
        'include_raw_files': True
    }
    qc_metrics = ff_utils.get_associated_qc_metrics(uuid, **kwargs)
    assert len(qc_metrics.keys()) == 2
    assert '131106bc-8535-4448-903e-854abbbbbbbb' in qc_metrics
    assert '4c9dabc6-61d6-4054-a951-c4fdd0023800' in qc_metrics
    assert 'QualityMetric' in qc_metrics['131106bc-8535-4448-903e-854abbbbbbbb']['values']['@type']
    assert 'QualityMetric' in qc_metrics['4c9dabc6-61d6-4054-a951-c4fdd0023800']['values']['@type']


@pytest.mark.integrated
@pytest.mark.flaky
def test_expand_es_metadata_frame_object_embedded(integrated_ff):
    test_list = ['7f9eb396-5c1a-4c5e-aebf-28ea39d6a50f']
    key, ff_env = integrated_ff['ff_key'], integrated_ff['ff_env']
    store_obj, uuids_obj = ff_utils.expand_es_metadata(test_list, store_frame='object', key=key, ff_env=ff_env)
    # make sure the frame is object (default)
    test_item_obj = store_obj['file_processed'][0]
    assert test_item_obj['lab'].startswith('/labs/')

    # now test frame=embedded
    store_emb, uuids_emb = ff_utils.expand_es_metadata(test_list, store_frame='embedded', key=key, ff_env=ff_env)
    test_item_emb = store_emb['file_processed'][0]
    assert isinstance(test_item_emb['lab'], dict)
    assert test_item_emb['lab']['@id'].startswith('/labs/')
    # links found stay the same between embedded and obj
    assert set(uuids_obj) == set(uuids_emb)


@pytest.mark.integrated
@pytest.mark.flaky
def test_expand_es_metadata_add_wfrs(integrated_ff):
    test_list = ['7f9eb396-5c1a-4c5e-aebf-28ea39d6a50f']
    key, ff_env = integrated_ff['ff_key'], integrated_ff['ff_env']
    store, uuids = ff_utils.expand_es_metadata(test_list, add_pc_wfr=True, key=key, ff_env=ff_env)
    for pos_case in ['workflow_run_awsem', 'workflow', 'file_reference', 'software', 'workflow_run_sbg',
                     'quality_metric_pairsqc', 'quality_metric_fastqc']:
        assert pos_case in store


@pytest.mark.integrated
@pytest.mark.flaky
def test_expand_es_metadata_complain_wrong_frame(integrated_ff):
    test_list = ['7f9eb396-5c1a-4c5e-aebf-28ea39d6a50f']
    key = integrated_ff['ff_key']
    with pytest.raises(Exception) as exec_info:
        store, uuids = ff_utils.expand_es_metadata(test_list, add_pc_wfr=True, store_frame='embroiled', key=key)
        ignored(store, uuids)  # do we want to do any testing here?
    assert str(exec_info.value) == """Invalid frame name "embroiled", please use one of ['raw', 'object', 'embedded']"""


@pytest.mark.integrated
@pytest.mark.flaky
def test_expand_es_metadata_ignore_fields(integrated_ff):
    test_list = ['7f9eb396-5c1a-4c5e-aebf-28ea39d6a50f']
    key, ff_env = integrated_ff['ff_key'], integrated_ff['ff_env']
    store, uuids = ff_utils.expand_es_metadata(test_list, add_pc_wfr=True, ignore_field=['quality_metric',
                                                                                         'output_quality_metrics'],
                                               key=key, ff_env=ff_env)
    for pos_case in ['workflow_run_awsem', 'workflow', 'file_reference', 'software', 'workflow_run_sbg']:
        assert pos_case in store
    for neg_case in ['quality_metric_pairsqc', 'quality_metric_fastqc']:
        assert neg_case not in store


@pytest.mark.integrated
@pytest.mark.flaky
def test_delete_field(integrated_ff):
    """ Tests deleting a field from a specific item """
    key, ff_env = integrated_ff['ff_key'], integrated_ff['ff_env']
    res1 = ff_utils.delete_field('7f9eb396-5c1a-4c5e-aebf-28ea39d6a50f', 'software', key=key, ff_env=ff_env)
    assert res1['status'] == 'success'
    res2 = ff_utils.delete_field('7f9eb396-5c1a-4c5e-aebf-28ea39d6a50f', 'not_a_field', key=key, ff_env=ff_env)
    assert res2['status'] == 'success'  # a non-existent field should still 'succeed'
    with pytest.raises(Exception) as exec_info:
        ff_utils.delete_field('7f9eb396-5c1a-4c5e-aebf-28ea39d6a50ff', 'not_a_field', key=key, ff_env=ff_env)
    assert "Bad status code" in str(exec_info.value)


@pytest.mark.file_operation
@pytest.mark.flaky
def test_dump_results_to_json(integrated_ff):

    def clear_folder(folder):
        ignored(folder)
        try:
            shutil.rmtree(test_folder)
        except FileNotFoundError:
            pass

    test_folder = 'test/test_data'
    clear_folder(test_folder)
    test_list = ['7f9eb396-5c1a-4c5e-aebf-28ea39d6a50f']
    key, ff_env = integrated_ff['ff_key'], integrated_ff['ff_env']
    store, uuids = ff_utils.expand_es_metadata(test_list, store_frame='object', key=key, ff_env=ff_env)
    len_store = len(store)
    ff_utils.dump_results_to_json(store, test_folder)
    all_files = os.listdir(test_folder)
    assert len(all_files) == len_store
    clear_folder(test_folder)


@pytest.mark.integrated
def test_search_es_metadata(integrated_ff):
    """ Tests search_es_metadata on mastertest """
    res = ff_utils.search_es_metadata('fourfront-mastertestuser', {'size': '1000'},
                                      key=integrated_ff['ff_key'], ff_env=integrated_ff['ff_env'])
    # The exact number may vary, so just do some random plausibility checking of result.
    n_users = len(res)  # Probably a bit more than 20, since 20 are in the master inserts
    assert 20 < n_users < 100
    assert all(item["_type"] == "user" for item in res)  # Make sure they are all users
    assert all("@" in item["_source"]["embedded"]["email"] for item in res)  # Make sure all have an email address
    check_duplicated_items_by_key('_id', res, formatter=lambda x: json.dumps(x, indent=2))
    # assert len(res) == len({ item["_id"] for item in res })  # Make sure ids are unique
    test_query = {
        'query': {
            'bool': {
                'must': [  # search for will's user insert
                    {'terms': {'_id': ['1a12362f-4eb6-4a9c-8173-776667226988']}}
                ],
                'must_not': []
            }
        },
        'sort': [{'_uid': {'order': 'desc'}}]
    }
    res = ff_utils.search_es_metadata('fourfront-mastertestuser', test_query,
                                      key=integrated_ff['ff_key'], ff_env=integrated_ff['ff_env'])
    assert len(res) == 1
    # Do some plausibility checking that it's Will and that he has data.
    assert res[0]["_source"]["embedded"]["first_name"] == "Will"
    assert res[0]["_source"]["embedded"]["groups"] == ["admin"]


@pytest.mark.integrated
def test_search_es_metadata_generator(integrated_ff):
    """ Tests SearchESMetadataHandler both normally and with a generator, verifies consistent results """
    handler = ff_utils.SearchESMetadataHandler(key=integrated_ff['ff_key'], ff_env=integrated_ff['ff_env'])
    no_gen_res = ff_utils.search_es_metadata('fourfront-mastertestuser', {'size': '1000'},
                                             key=integrated_ff['ff_key'], ff_env=integrated_ff['ff_env'])
    res = handler.execute_search('fourfront-mastertestuser', {'size': '1000'}, is_generator=True, page_size=5)
    count = 0
    for _ in res:
        count += 1
    assert count == len(no_gen_res)


def test_convert_param():
    """ Very basic test that illustrates what convert_param should do """
    params = {'param1': 5}
    expected1 = [{'workflow_argument_name': 'param1', 'value': 5}]
    expected2 = [{'workflow_argument_name': 'param1', 'value': '5'}]
    converted_params1 = ff_utils.convert_param(params)
    converted_params2 = ff_utils.convert_param(params, vals_as_string=True)
    assert expected1 == converted_params1
    assert expected2 == converted_params2


@pytest.mark.integrated
def test_get_page(integrated_ff):
    ff_env = integrated_ff['ff_env']
    health_res = ff_utils.get_health_page(ff_env=ff_env)
    assert health_res['namespace'] == ff_env
    counts_res = ff_utils.get_counts_page(ff_env=ff_env)['db_es_total']
    assert 'DB' in counts_res
    assert 'ES' in counts_res
    indexing_status_res = ff_utils.get_indexing_status(ff_env=ff_env)
    assert 'primary_waiting' in indexing_status_res


@pytest.mark.integratedx
def test_are_counts_even_integrated(integrated_ff):
    ff_env = integrated_ff['ff_env']
    counts_are_even, totals = ff_utils.get_counts_summary(ff_env)
    if counts_are_even:
        assert 'more items' not in ' '.join(totals)
    else:
        assert 'more items' in ' '.join(totals)


# These are stripped-down versions of actual results that illustrate the kinds of output we might expect.

SAMPLE_COUNTS_MISMATCH = {
    'title': 'Item Counts',
    'db_es_total': 'DB: 54  ES: 57   < ES has 3 more items >',
    'db_es_compare': {
        'AnalysisStep': 'DB: 26   ES: 26 ',
        'BiosampleCellCulture': 'DB: 3   ES: 3 ',
        'Construct': 'DB: 1   ES: 1 ',
        'Document': 'DB: 2   ES: 2 ',
        'Enzyme': 'DB: 9   ES: 9 ',
        'Biosample': 'DB: 13   ES: 16   < ES has 3 more items >',
    }
}

SAMPLE_COUNTS_MATCH = {
    'title': 'Item Counts',
    'db_es_total': 'DB: 54  ES: 54 ',
    'db_es_compare': {
        'AnalysisStep': 'DB: 26   ES: 26 ',
        'BiosampleCellCulture': 'DB: 3   ES: 3 ',
        'Construct': 'DB: 1   ES: 1 ',
        'Document': 'DB: 2   ES: 2 ',
        'Enzyme': 'DB: 9   ES: 9 ',
        'Biosample': 'DB: 13   ES: 13 ',
    }
}


@pytest.mark.parametrize('expect_match, sample_counts', [(True, SAMPLE_COUNTS_MATCH), (False, SAMPLE_COUNTS_MISMATCH)])
def test_are_counts_even_unit(expect_match, sample_counts):

    unsupplied = object()
    ts = TestScenarios

    def mocked_authorized_request(url, auth=None, ff_env=None, verb='GET',
                                  retry_fxn=unsupplied, **kwargs):
        print("URL=%s auth=%s ff_env=%s verb=%s" % (url, auth, ff_env, verb))
        ignored(ff_env, kwargs)
        assert auth == ts.bar_env_auth_dict
        assert verb == 'GET'
        assert retry_fxn == unsupplied

        return MockResponse(json=sample_counts)

    with mocked_s3utils_with_sse():
        with mock.patch.object(ff_utils, "authorized_request", mocked_authorized_request):
            with mock.patch.object(s3_utils.s3Utils, "get_access_keys",
                                   return_value=ts.bar_env_auth_dict):

                counts_are_even, totals = ff_utils.get_counts_summary(env='fourfront-foo')
                print("expect_match=", expect_match, "counts_are_even=", counts_are_even, "totals=", totals)

                if expect_match:
                    assert counts_are_even
                    assert 'more items' not in ' '.join(totals)
                else:
                    assert not counts_are_even
                    assert 'more items' in ' '.join(totals)


@pytest.mark.parametrize('url, expected_bucket, expected_key', [
    ('https://s3.amazonaws.com/cgap-devtest-main-application-cgap-devtest-wfout/GAPFI1HVXJ5F/fastqc_report.html',
     'cgap-devtest-main-application-cgap-devtest-wfout', 'GAPFI1HVXJ5F/fastqc_report.html'),
    ('https://cgap-devtest-main-application-tibanna-logs.s3.amazonaws.com/41c2fJDQcLk3.metrics/metrics.html',
     'cgap-devtest-main-application-tibanna-logs', '41c2fJDQcLk3.metrics/metrics.html'),
    ('https://elasticbeanstalk-fourfront-cgap-files.s3.amazonaws.com/GAPFIDUMMY/GAPFIDUMMY.fastq.gz',
     'elasticbeanstalk-fourfront-cgap-files', 'GAPFIDUMMY/GAPFIDUMMY.fastq.gz')
])
def test_parse_s3_bucket_key_url(url, expected_bucket, expected_key):
    """ Tests that we correctly parse the given URls into their bucket, key identifiers.
        Note that these tests cases are specific to our DB!
    """
    bucket, key = ff_utils.parse_s3_bucket_and_key_url(url)
    assert expected_bucket == bucket and key == expected_key
