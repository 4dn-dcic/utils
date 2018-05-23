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
