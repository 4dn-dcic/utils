import pytest

from dcicutils.es_utils import create_es_client, execute_lucene_query_on_es


@pytest.fixture
def es_client_fixture(integrated_ff):
    """ Fixture that creates an es client to mastertest """
    return create_es_client(integrated_ff['es_url'])


@pytest.mark.integrated
def test_lucene_query_basic(es_client_fixture):
    """ Tests basic lucene queries via the endpoint on mastertest """
    q = {}
    results = execute_lucene_query_on_es(client=es_client_fixture, index='fourfront-mastertestuser', query=q)
    assert len(results) == 10