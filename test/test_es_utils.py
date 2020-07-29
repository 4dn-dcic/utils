import pytest
import copy
from dcicutils.qa_utils import timed
from dcicutils.ff_utils import expand_es_metadata
from dcicutils.es_utils import create_es_client, execute_lucene_query_on_es, get_bulk_uuids_embedded


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
    result = get_bulk_uuids_embedded(es_client_fixture, 'fourfront-mastertestuser', uuids)
    assert len(result) == 1
    assert result[0]['uuid'] == uuids[0]  # check uuid
    assert result[0]['lab']['awards'] is not None  # check embedding
    assert result[0]['lab']['awards'][0]['project'] is not None

    # one page of results, should give us 10
    users = execute_lucene_query_on_es(client=es_client_fixture, index='fourfront-mastertestuser', query={})
    uuids = [doc['_id'] for doc in users]
    result = get_bulk_uuids_embedded(es_client_fixture, 'fourfront-mastertestuser', uuids)
    assert len(result) == 10
    for doc in result:
        assert doc['uuid'] in uuids  # check uuids


@pytest.mark.integrated
def test_get_bulk_uuids_outperforms_expand_es_metadata(integrated_ff, es_client_fixture):
    """ Tests that the new method out performs the new one. """
    users = execute_lucene_query_on_es(client=es_client_fixture, index='fourfront-mastertestuser', query={})
    uuids = [doc['_id'] for doc in users]
    current_time = [None]  # pass by reference

    def set_current_time(start, end, t=current_time):  # noqa I want this default argument to be mutable
        t[0] = end - start

    # this extracts the 10 desired uuids in embedded view using mget in ~200 ms
    with timed(reporter=set_current_time):
        get_bulk_uuids_embedded(es_client_fixture, 'fourfront-mastertestuser', uuids)
    assert current_time is not None
    time1 = copy.deepcopy(current_time)  # store the actual result

    # this gets 19 total uuids in 1700ms (~850 ms adjusted, so roughly 4x slower)
    with timed(reporter=set_current_time):
        expand_es_metadata(uuids, key=integrated_ff['ff_key'], ff_env=integrated_ff['ff_env'])
    assert time1[0] < (current_time[0] / 2)  # noqa should always be much faster
