import pytest

from dcicutils.es_utils import (
    create_es_client, execute_lucene_query_on_es, get_bulk_uuids_embedded, ElasticSearchServiceClient,
)
from dcicutils.ff_utils import get_es_metadata
from dcicutils.misc_utils import ignored
from dcicutils.qa_utils import timed
from unittest import mock


class TestElasticSearchServiceClient:

    @staticmethod
    def mock_update_es_success(DomainName, ElasticsearchClusterConfig):  # noQA - mixed-case params chosen by AWS
        ignored(DomainName, ElasticsearchClusterConfig)
        return {
            'ResponseMetadata': {
                'HTTPStatusCode': 200
            }
        }

    @staticmethod
    def mock_update_es_fail(DomainName, ElasticsearchClusterConfig):  # noQA - mixed-case params chosen by AWS
        return {
            'ResponseMetadata': {
                'HTTPStatusCode': 403
            }
        }

    @staticmethod
    def mock_update_es_bad_response(DomainName, ElasticsearchClusterConfig):  # noQA - mixed-case params chosen by AWS
        return {
            'something_else': {
                'blah': 403
            }
        }

    @staticmethod
    def mock_update_es_unknown(DomainName, ElasticsearchClusterConfig):  # noQA - mixed-case params chosen by AWS
        ignored(DomainName, ElasticsearchClusterConfig)
        raise Exception('Literally anything')

    def test_elasticsearch_service_client_resize_accepted(self):
        """ Tests handling of a success response. """
        client = ElasticSearchServiceClient()
        with mock.patch.object(client.client, 'update_elasticsearch_domain_config',
                               self.mock_update_es_success):
            success = client.resize_elasticsearch_cluster(
                domain_name='fourfront-newtest',
                master_node_type='t2.medium.elasticsearch',
                master_node_count=3,
                data_node_type='c5.large.elasticsearch',
                data_node_count=2
            )
            assert success

    def test_elasticsearch_service_client_resize_fail(self):
        """ Tests handling of a 403 response. """
        client = ElasticSearchServiceClient()
        with mock.patch.object(client.client, 'update_elasticsearch_domain_config',
                               self.mock_update_es_fail):
            success = client.resize_elasticsearch_cluster(
                domain_name='fourfront-newtest',
                master_node_type='t2.medium.elasticsearch',
                master_node_count=3,
                data_node_type='c5.large.elasticsearch',
                data_node_count=2
            )
            assert not success

    def test_elasticsearch_service_client_resize_bad_response(self):
        """ Tests handling of a badly formatted response. """
        client = ElasticSearchServiceClient()
        with mock.patch.object(client.client, 'update_elasticsearch_domain_config',
                               self.mock_update_es_bad_response):
            success = client.resize_elasticsearch_cluster(
                domain_name='fourfront-newtest',
                master_node_type='t2.medium.elasticsearch',
                master_node_count=3,
                data_node_type='c5.large.elasticsearch',
                data_node_count=2
            )
            assert not success

    def test_elasticsearch_service_client_resize_unknown(self):
        """ Tests handling of a unknown error. """
        client = ElasticSearchServiceClient()
        with mock.patch.object(client.client, 'update_elasticsearch_domain_config',
                               self.mock_update_es_unknown):
            success = client.resize_elasticsearch_cluster(
                domain_name='fourfront-newtest',
                master_node_type='t2.medium.elasticsearch',
                master_node_count=3,
                data_node_type='c5.large.elasticsearch',
                data_node_count=2
            )
            assert not success


@pytest.fixture
def es_client_fixture(integrated_ff):
    """ Fixture that creates an es client to mastertest """
    return create_es_client(integrated_ff['es_url'])


@pytest.mark.integrated
def test_lucene_query_basic(es_client_fixture):
    """ Tests basic lucene queries via the underlying endpoint on mastertest """
    results = execute_lucene_query_on_es(client=es_client_fixture, index='fourfront-mastertestuser', query={})
    assert len(results) == 10
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
    results = execute_lucene_query_on_es(client=es_client_fixture, index='fourfront-mastertestuser', query=test_query)
    assert len(results) == 1


@pytest.mark.integrated
def test_get_bulk_uuids_embedded(es_client_fixture):
    """ Tests getting some bulk uuids acquired from search. """
    uuids = ['1a12362f-4eb6-4a9c-8173-776667226988']  # only one uuid first
    result1 = get_bulk_uuids_embedded(es_client_fixture, 'fourfront-mastertestuser', uuids, is_generator=False)
    assert len(result1) == 1
    assert result1[0]['uuid'] == uuids[0]  # check uuid
    assert result1[0]['lab']['awards'] is not None  # check embedding
    assert result1[0]['lab']['awards'][0]['project'] is not None

    # one page of results, should give us 10
    users = execute_lucene_query_on_es(client=es_client_fixture, index='fourfront-mastertestuser', query={})
    uuids = [doc['_id'] for doc in users]
    result2 = get_bulk_uuids_embedded(es_client_fixture, 'fourfront-mastertestuser', uuids)
    assert len(result2) == 10
    for doc in result2:
        assert doc['uuid'] in uuids  # check uuids

    result3 = get_bulk_uuids_embedded(es_client_fixture, 'fourfront-mastertestuser', uuids, is_generator=True)
    for doc in result3:
        assert doc['uuid'] in uuids  # check uuids from gen


@pytest.mark.integrated
def test_get_bulk_uuids_outperforms_get_es_metadata(integrated_ff, es_client_fixture):
    """ Tests that the new method out performs the new one. """
    users = execute_lucene_query_on_es(client=es_client_fixture, index='fourfront-mastertestuser', query={})
    uuids = [doc['_id'] for doc in users]
    times = []

    def set_current_time(start, end):  # noqa I want this default argument to be mutable
        times.append(end - start)

    with timed(reporter=set_current_time):
        get_bulk_uuids_embedded(es_client_fixture, 'fourfront-mastertestuser', uuids, is_generator=False)
    assert len(times) == 1
    with timed(reporter=set_current_time):
        get_es_metadata(uuids, key=integrated_ff['ff_key'], ff_env=integrated_ff['ff_env'])
    assert len(times) == 2
    assert times[0] < times[1]  # should always be much faster (normalized for # of uuids retrieved)
