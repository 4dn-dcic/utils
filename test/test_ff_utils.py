from dcicutils import ff_utils
import pytest
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


@pytest.fixture
def connection(mocker):
    return mocker.patch.object(ff_utils.submit_utils, 'FDN_Connection')


def test_fdn_connection_no_key_or_connection():
    assert not ff_utils.fdn_connection()


def test_fdn_connection_w_connection(connection):
    conn = ff_utils.fdn_connection('', connection)
    assert conn == connection


def test_fdn_connection_w_key_dict(mocker):
    keydict = {
        'test_key': {
            'server': 'https://data.4dnucleome.org',
            'key': 'ABCDEFG',
            'secret': 'so_so_secret'
        }
    }
    with mocker.patch('dcicutils.ff_utils.submit_utils.FDN_Connection', return_value='test_connection'):
        with mocker.patch('dcicutils.ff_utils.submit_utils.FDN_Key', return_value='test_key') as make_key:
            conn = ff_utils.fdn_connection(keydict)
            assert make_key.called_with(keydict, 'default')
            assert conn == 'test_connection'


def test_fdn_connection_w_keyfile_keyname(mocker):
    keyfile = 'my_keyfile'
    keyname = '4dnprod'
    with mocker.patch('dcicutils.ff_utils.submit_utils.FDN_Connection', return_value='test_connection'):
        with mocker.patch('dcicutils.ff_utils.submit_utils.FDN_Key', return_value='test_key') as make_key:
            conn = ff_utils.fdn_connection(keyfile, keyname=keyname)
            assert make_key.called_with(keyfile, keyname)
            assert conn == 'test_connection'


def test_fdn_connection_where_connection_trumps_key(mocker, connection):
    keydict = {
        'test_key': {
            'server': 'https://data.4dnucleome.org',
            'key': 'ABCDEFG',
            'secret': 'so_so_secret'
        }
    }
    conn = ff_utils.fdn_connection(keydict, connection)
    assert conn == connection


def test_is_uuid():
    uuids = [
        '231111bc-8535-4448-903e-854af460b254',
        '231111bc-8535-4448-903e-854af460b25',
        '231111bc85354448903e854af460b254'
    ]
    for i, u in enumerate(uuids):
        if i == 0:
            assert ff_utils.is_uuid(u)
        else:
            assert not ff_utils.is_uuid(u)


def test_find_uuids_from_eset(eset_json):
    field2uuid = {
        "award": "4871e338-b07d-4665-a00a-357648e5bad6",
        "lab": "795847de-20b6-4f8c-ba8d-185215469cbf",
        "uuid": "9eb40c13-cf85-487c-9819-71ef74a22dcc",
        "submitted_by": "da4f53e5-4e54-4ae7-ad75-ba47316a8bfa"
    }
    exps = ["d4b0e597-8c81-43e3-aeda-e9842fc18e8f", "8d10f11f-95a8-4b8d-8ff2-748ea8631a23"]
    for field, val in eset_json.items():
        ulist = ff_utils.find_uuids(val)
        if field in field2uuid:
            assert field2uuid[field] == ulist[0]
        elif field in ["experiments_in_set", "replicate_exps"]:
            for u in ulist:
                assert u in exps


def test_filter_dict_by_value(eset_json):
    to_filter = {
        "schema_version": "2",
        "accession": "4DNES4GSP9S4",
        "aliases": ["ren:HG00512_repset"]
    }
    vals = list(to_filter.values())
    included = ff_utils.filter_dict_by_value(eset_json, vals)
    assert len(included) == len(to_filter)
    for f in to_filter.keys():
        assert f in included

    excluded = ff_utils.filter_dict_by_value(eset_json, vals, include=False)
    assert len(excluded) == len(eset_json) - len(to_filter)
    for f in to_filter.keys():
        assert f not in excluded


def test_has_field_value_check_for_field_only(eset_json):
    fieldnames = ['schema_version', 'award', 'alternate_accessions']
    for f in fieldnames:
        assert ff_utils.has_field_value(eset_json, f)


def test_has_field_value_no_it_doesnt(eset_json):
    fieldnames = ['biosample', 'blah', 'bio_rep_no']
    for f in fieldnames:
        assert not ff_utils.has_field_value(eset_json, f)


def test_has_field_value_check_for_field_and_value(eset_json):
    fields_w_values = {
        "schema_version": "2",
        "accession": "4DNES4GSP9S4",
        "aliases": "ren:HG00512_repset"
    }
    for f, v in fields_w_values.items():
        assert ff_utils.has_field_value(eset_json, f, v)


def test_has_field_value_check_for_field_w_item(bs_embed_json):
    f = "lab"
    v = "/labs/david-gilbert-lab/"
    assert ff_utils.has_field_value(bs_embed_json, f, v)


def test_get_types_that_can_have_field(mocker, profiles):
    field = 'tags'
    with mocker.patch('dcicutils.ff_utils.submit_utils.get_FDN', return_value=profiles):
        types_w_field = ff_utils.get_types_that_can_have_field('conn', field)
        assert 'ExperimentSetReplicate' in types_w_field
        assert 'TreatmentChemical' not in types_w_field


def test_get_item_type_from_dict(eset_json):
    eset_json['@type'] = ['ExperimentSetReplicate', 'ExperimentSet', 'Item']
    es_ty = ff_utils.get_item_type('blah', eset_json)
    assert es_ty == 'ExperimentSetReplicate'


def test_get_item_type_from_id(mocker, connection):
    with mocker.patch('dcicutils.ff_utils.submit_utils.get_FDN', return_value={'@type': ['ExperimentSetReplicate']}):
        result = ff_utils.get_item_type(connection, 'blah')
        assert result == 'ExperimentSetReplicate'


def test_authorized_request(mocker, empty_success_response):
    from dcicutils.ff_utils import authorized_request
    import json
    expected = empty_success_response

    with mocker.patch('dcicutils.ff_utils.requests.post', return_value=empty_success_response):
        res = authorized_request('https://data.4dnucleome.org/files-microscopy/4DNFII5APXQO/upload',
                                 ff_env='fourfront-webprod', verb='POST', data=json.dumps({}))

        assert res.status_code == expected.status_code


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
    import time
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
def test_post_metadata(integrated_ff):
    test_data = {'biosource_type': 'immortalized cell line', 'award': '1U01CA200059-01',
                 'lab': '4dn-dcic-lab', 'status': 'deleted'}
    post_res = ff_utils.post_metadata(test_data, 'biosource', key=integrated_ff['ff_key'])
    post_item = post_res['@graph'][0]
    assert 'uuid' in post_item
    assert post_item['biosource_type'] == test_data['biosource_type']
    # make sure there is a 409 when posting to an existing item
    test_data['uuid'] = post_item['uuid']
    with pytest.raises(Exception) as exec_info:
        ff_utils.post_metadata(test_data, 'biosource', key=integrated_ff['ff_key'])
    assert '409' in str(exec_info.value)  # 409 is conflict error


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
    search_res = ff_utils.search_metadata('search/?limit=all&type=Biosource', key=integrated_ff['ff_key'])
    assert isinstance(search_res, list)
    # this will fail if biosources have not yet been indexed
    assert len(search_res) > 0
    # make sure uuids are unique
    search_uuids = set([item['uuid'] for item in search_res])
    assert len(search_uuids) == len(search_res)
    search_res_slash = ff_utils.search_metadata('/search/?limit=all&type=Biosource', key=integrated_ff['ff_key'])
    assert isinstance(search_res_slash, list)
    assert len(search_res_slash) == len(search_res)
    # search with a limit
    search_res_limit = ff_utils.search_metadata('/search/?limit=3&type=Biosource', key=integrated_ff['ff_key'])
    assert len(search_res_limit) == 3
    # search with a filter
    search_res_filt = ff_utils.search_metadata('/search/?limit=3&type=Biosource&biosource_type=immortalized cell line',
                                               key=integrated_ff['ff_key'])
    assert len(search_res_filt) > 0


@pytest.mark.integrated
def get_search_generator(integrated_ff):
    search_url = integrated_ff['server'] + '/search/?type=OntologyTerm'
    generator1 = ff_utils.get_search_generator(search_url, auth=integrated_ff['ff_key'], page_limit=25)
    list_gen1 = list(generator1)
    assert len(list_gen1) > 0
    for idx, page in list_gen1:
        assert isinstance(page, list)
        if idx < len(list_gen1) - 1:
            assert len(page) == 25
        else:
            assert len(page) > 0
    all_gen1 = [l for page in pages for pages in list_gen1]  # noqa
    generator2 = ff_utils.get_search_generator(search_url, auth=integrated_ff['ff_key'], page_limit=50)
    list_gen2 = list(generator2)
    assert len(list_gen1) > list(list_gen2)
    all_gen2 = [l for page in pages for pages in list_gen2]  # noqa
    assert len(all_gen1) == len(all_gen2)
    # use a limit in the search
    search_url += '&limit=33'
    generator3 = ff_utils.get_search_generator(search_url, auth=integrated_ff['ff_key'])
    list_gen3 = list(generator3)
    all_gen3 = [l for page in pages for pages in list_gen3]  # noqa
    assert len(all_gen3) == 33
    # make sure that all results are unique
    all_gen3_uuids = set([item['uuid'] for item in all_gen3])
    assert len(all_gen3_uuids) == len(all_gen3)


@pytest.mark.integrated
def test_fdn_connection(integrated_ff):
    connection = ff_utils.fdn_connection(key={'default': integrated_ff['ff_key']})
    assert connection.server
    # must provide key or connection
    null_connection = ff_utils.fdn_connection()
    assert null_connection is None
    # bad connnection
    with pytest.raises(Exception) as exec_info:
        ff_utils.fdn_connection(key={'default': 'abcdefg'})
    assert 'Unable to connect' in str(exec_info.value)


@pytest.mark.integrated
def test_get_linked_items(integrated_ff):
    test_item = '331111bc-8535-4448-903e-854af460a254'
    connection = ff_utils.fdn_connection(key={'default': integrated_ff['ff_key']})
    linked = ff_utils.get_linked_items(connection, test_item)
    assert isinstance(linked, dict) and len(linked.keys()) > 1
    assert test_item in linked  # item is in its own linked items
