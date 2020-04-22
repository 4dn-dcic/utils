import pytest

from dcicutils.es_utils import create_es_client, execute_lucene_query_on_es


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