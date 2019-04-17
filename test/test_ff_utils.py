import pytest
import json
import time
from dcicutils import ff_utils
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


def test_generate_rand_accession():
    test = ff_utils.generate_rand_accession()
    assert '4DN' in test
    assert '0' not in test


def test_get_response_json():
    # use responses from http://httpbin.org
    import requests
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


# Integration tests


@pytest.mark.integrated
def test_unified_authentication(integrated_ff):
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
def test_get_authentication_with_server(integrated_ff):
    import copy
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


@pytest.mark.integrated
def test_stuff_in_queues(integrated_ff):
    """
    Gotta index a bunch of stuff to make this work
    """
    search_res = ff_utils.search_metadata('search/?limit=all&type=File', key=integrated_ff['ff_key'])
    # just take the first handful
    for item in search_res[:8]:
        ff_utils.patch_metadata({}, obj_id=item['uuid'], key=integrated_ff['ff_key'])
    time.sleep(5)  # let queues catch up
    stuff_in_queue = ff_utils.stuff_in_queues(integrated_ff['ff_env'], check_secondary=True)
    assert stuff_in_queue


@pytest.mark.integrated
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

    # good GET request for an item
    good_resp1 = ff_utils.authorized_request(item_url, auth=integrated_ff['ff_key'], verb='GET')
    assert good_resp1.status_code == 200
    # good GET request for a search
    good_resp2 = ff_utils.authorized_request(server + '/search/?type=Biosample',
                                             auth=integrated_ff['ff_key'], verb='GET')
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


@pytest.mark.integrated
def test_get_metadata(integrated_ff, basestring):
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
    assert isinstance(res_obj['individual'], basestring)


@pytest.mark.integrated
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
    tries = 0
    while not links and tries < 10:
        time.sleep(2)
        post_links = ff_utils.get_metadata_links(post_item['uuid'], key=integrated_ff['ff_key'])
        links = post_links.get('uuids_linking_to', [])
        tries += 1
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
    tries = 0
    while len(links) > 0 and tries < 10:
        time.sleep(2)
        post_links = ff_utils.get_metadata_links(post_item['uuid'], key=integrated_ff['ff_key'])
        links = post_links.get('uuids_linking_to', [])
        tries += 1
    assert len(links) == 0

    purge_res3 = ff_utils.purge_metadata(post_item['uuid'], key=integrated_ff['ff_key'])
    assert purge_res3['status'] == 'success'
    # make sure it is purged
    with pytest.raises(Exception) as exec_info:
        ff_utils.get_metadata(post_item['uuid'], key=integrated_ff['ff_key'],
                              add_on='datastore=database')
    assert 'The resource could not be found' in str(exec_info.value)


@pytest.mark.integrated
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


@pytest.mark.integrated
def test_search_metadata(integrated_ff):
    from types import GeneratorType
    search_res = ff_utils.search_metadata('search/?limit=all&type=File', key=integrated_ff['ff_key'])
    assert isinstance(search_res, list)
    # this will fail if items have not yet been indexed
    assert len(search_res) > 0
    # make sure uuids are unique
    search_uuids = set([item['uuid'] for item in search_res])
    assert len(search_uuids) == len(search_res)
    search_res_slash = ff_utils.search_metadata('/search/?limit=all&type=File', key=integrated_ff['ff_key'])
    assert isinstance(search_res_slash, list)
    assert len(search_res_slash) == len(search_res)
    # search with a limit
    search_res_limit = ff_utils.search_metadata('/search/?limit=3&type=File', key=integrated_ff['ff_key'])
    assert len(search_res_limit) == 3
    # search with a filter
    search_res_filt = ff_utils.search_metadata('/search/?limit=3&type=File&file_type=reads',
                                               key=integrated_ff['ff_key'])
    assert len(search_res_filt) > 0
    # test is_generator=True
    search_res_gen = ff_utils.search_metadata('/search/?limit=3&type=File&file_type=reads',
                                              key=integrated_ff['ff_key'], is_generator=True)
    assert isinstance(search_res_gen, GeneratorType)
    gen_res = [v for v in search_res_gen]  # run the gen
    assert len(gen_res) == 3


@pytest.mark.integrated
def test_get_search_generator(integrated_ff):
    search_url = integrated_ff['ff_key']['server'] + '/search/?type=OntologyTerm'
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
    search_url += '&limit=33'
    generator3 = ff_utils.get_search_generator(search_url, auth=integrated_ff['ff_key'])
    list_gen3 = list(generator3)
    all_gen3 = [page for pages in list_gen3 for page in pages]  # noqa
    assert len(all_gen3) == 33
    # make sure that all results are unique
    all_gen3_uuids = set([item['uuid'] for item in all_gen3])
    assert len(all_gen3_uuids) == len(all_gen3)


@pytest.mark.integrated
def test_get_es_metadata(integrated_ff):
    from dcicutils import es_utils
    from types import GeneratorType
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
def test_get_es_search_generator(integrated_ff):
    from dcicutils import es_utils
    # get es_client info from the health page
    es_url = ff_utils.get_health_page(key=integrated_ff['ff_key'])['elasticsearch']
    es_client = es_utils.create_es_client(es_url, use_aws_auth=True)
    es_query = {'query': {'match_all': {}}, 'sort': [{'_uid': {'order': 'desc'}}]}
    # search for all ontology terms with a low pagination size
    es_gen = ff_utils.get_es_search_generator(es_client, 'ontology_term',
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
    search_res = ff_utils.search_metadata('/search/?type=OntologyTerm&frame=object',
                                          key=integrated_ff['ff_key'])
    search_uuids = set(hit['uuid'] for hit in search_res)
    assert all_es_uuids == search_uuids


@pytest.mark.integrated
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
def test_get_schema_names(integrated_ff):
    schema_names = ff_utils.get_schema_names(key=integrated_ff['ff_key'],
                                             ff_env=integrated_ff['ff_env'])
    # assert that it gets quite some schemas
    assert len(schema_names) > 75
    assert schema_names['FileFastq'] == 'file_fastq'
    assert schema_names['ExperimentSetReplicate'] == 'experiment_set_replicate'


@pytest.mark.integrated
def test_expand_es_metadata(integrated_ff):
    test_list = ['7f9eb396-5c1a-4c5e-aebf-28ea39d6a50f']
    key, ff_env = integrated_ff['ff_key'], integrated_ff['ff_env']
    store, uuids = ff_utils.expand_es_metadata(test_list, key=key, ff_env=ff_env)
    for pos_case in ['file_processed', 'user', 'file_format', 'award', 'lab']:
        assert pos_case in store
    for neg_case in ['workflow_run_awsem', 'workflow', 'file_reference', 'software', 'workflow_run_sbg',
                     'quality_metric_pairsqc',  'quality_metric_fastqc']:
        assert neg_case not in store
    # make sure the frame is raw (default)
    test_item = store['file_processed'][0]
    assert test_item['lab'].startswith('828cd4fe')


@pytest.mark.integrated
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
def test_expand_es_metadata_add_wfrs(integrated_ff):
    test_list = ['7f9eb396-5c1a-4c5e-aebf-28ea39d6a50f']
    key, ff_env = integrated_ff['ff_key'], integrated_ff['ff_env']
    store, uuids = ff_utils.expand_es_metadata(test_list, add_pc_wfr=True, key=key, ff_env=ff_env)
    for pos_case in ['workflow_run_awsem', 'workflow', 'file_reference', 'software', 'workflow_run_sbg',
                     'quality_metric_pairsqc',  'quality_metric_fastqc']:
        assert pos_case in store


@pytest.mark.integrated
def test_expand_es_metadata_complain_wrong_frame(integrated_ff):
    test_list = ['7f9eb396-5c1a-4c5e-aebf-28ea39d6a50f']
    key = integrated_ff['ff_key']
    with pytest.raises(Exception) as exec_info:
        store, uuids = ff_utils.expand_es_metadata(test_list, add_pc_wfr=True, store_frame='embroiled', key=key)
    assert str(exec_info.value) == """Invalid frame name "embroiled", please use one of ['raw', 'object', 'embedded']"""


@pytest.mark.integrated
def test_expand_es_metadata_ignore_fields(integrated_ff):
    test_list = ['7f9eb396-5c1a-4c5e-aebf-28ea39d6a50f']
    key, ff_env = integrated_ff['ff_key'], integrated_ff['ff_env']
    store, uuids = ff_utils.expand_es_metadata(test_list, add_pc_wfr=True, ignore_field=['quality_metric',
                                                                                         'output_quality_metrics'],
                                               key=key, ff_env=ff_env)
    for pos_case in ['workflow_run_awsem', 'workflow', 'file_reference', 'software', 'workflow_run_sbg']:
        assert pos_case in store
    for neg_case in ['quality_metric_pairsqc',  'quality_metric_fastqc']:
        neg_case not in store


@pytest.mark.file_operation
def test_dump_results_to_json(integrated_ff):
    import shutil
    import os

    def clear_folder(folder):
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
