import contextlib
import json
import os
import pytest

# from collections import namedtuple
from dcicutils import bundle_utils as bundle_utils_module, ff_utils as ff_utils_module
from dcicutils.common import AnyJsonData
from dcicutils.env_utils import EnvUtils, public_env_name
from dcicutils.misc_utils import is_uuid, local_attrs, NamedObject, AbstractVirtualApp
from dcicutils.qa_utils import printed_output, mock_not_called, MockResponse
from dcicutils.bundle_utils import (
    # High-level interfaces
    ItemManager, load_items, ITEM_MANAGER_REGISTRY,
    # Low-level implementation
    SchemaAutoloadMixin,
    ItemTools, XlsxItemManager,
    CsvItemManager, TsvItemManager,
    # TypeHint, EnumHint,
    BoolHint,
)
from dcicutils.sheet_utils import (
    # High-level interfaces
    # TABLE_SET_MANAGER_REGISTRY,
    # Low-level implementation
    # BasicTableSetManager,
    # XlsxManager,
    CsvManager,  # TsvManager,
    # Error handling
    LoadArgumentsError, LoadTableError,  # LoadFailure,
    # Utilities
    infer_tab_name_from_filename,  # prefer_number, unwanted_kwargs, expand_string_escape_sequences,
)
from typing import Dict, Optional
from unittest import mock
from .conftest_settings import TEST_DIR
from .helpers import using_fresh_ff_state_for_testing
from .test_sheet_utils import (
    SAMPLE_XLSX_FILE, SAMPLE_XLSX_FILE_ITEM_CONTENT,  # SAMPLE_XLSX_FILE_RAW_CONTENT,
    SAMPLE_CSV_FILE, SAMPLE_CSV_FILE_ITEM_CONTENT,  # SAMPLE_CSV_FILE_RAW_CONTENT,
    SAMPLE_TSV_FILE, SAMPLE_TSV_FILE_ITEM_CONTENT,  # SAMPLE_TSV_FILE_RAW_CONTENT,
    SAMPLE_JSON_TABS_FILE, SAMPLE_JSON_TABS_FILE_ITEM_CONTENT,
    SAMPLE_YAML_TABS_FILE,
)


def test_item_tools_parse_sheet_header():
    assert ItemTools.parse_sheet_header('.a') == ['a']
    assert ItemTools.parse_sheet_header('a') == ['a']
    assert ItemTools.parse_sheet_header('#0') == [0]
    assert ItemTools.parse_sheet_header('0') == [0]
    assert ItemTools.parse_sheet_header('foo.bar') == ['foo', 'bar']
    assert ItemTools.parse_sheet_header('a.b#0') == ['a', 'b', 0]
    assert ItemTools.parse_sheet_header('x.xx#17#8.z') == ['x', 'xx', 17, 8, 'z']

    # We don't error-check this, but it shouldn't matter
    assert ItemTools.parse_sheet_header('#abc') == ['abc']
    assert ItemTools.parse_sheet_header('.123') == [123]
    assert ItemTools.parse_sheet_header('#abc.123#456.def') == ['abc', 123, 456, 'def']


def test_item_tools_parse_sheet_headers():
    input = ['a.b', 'a.c', 'a.d#1', 'a.d#2']
    expected = [['a', 'b'], ['a', 'c'], ['a', 'd', 1], ['a', 'd', 2]]
    assert ItemTools.parse_sheet_headers(input) == expected


@pytest.mark.parametrize('parsed_headers,expected_prototype', [
    (['a'],
     {'a': None}),
    (['a', 'b'],
     {'a': None, 'b': None}),
    (['a.b', 'a.c', 'a.d#0', 'a.d#1'],
     {'a': {'b': None, 'c': None, 'd': [None, None]}}),
    (['a.b', 'a.c', 'a.d#0.foo', 'a.d#0.bar'],
     {'a': {'b': None, 'c': None, 'd': [{'foo': None, 'bar': None}]}}),
    (['a.b', 'a.c', 'a.d#0.foo', 'a.d#0.bar', 'a.d#1.foo', 'a.d#1.bar'],
     {'a': {'b': None, 'c': None, 'd': [{'foo': None, 'bar': None}, {'foo': None, 'bar': None}]}}),
])
def test_item_tools_compute_patch_prototype(parsed_headers, expected_prototype):
    parsed_headers = ItemTools.parse_sheet_headers(parsed_headers)
    assert ItemTools.compute_patch_prototype(parsed_headers) == expected_prototype


@pytest.mark.parametrize('headers', [['0'], ['x', '0.y']])
def test_item_tools_compute_patch_prototype_errors(headers):

    parsed_headers = ItemTools.parse_sheet_headers(headers)
    with pytest.raises(LoadTableError) as exc:
        ItemTools.compute_patch_prototype(parsed_headers)
    assert str(exc.value) == "A header cannot begin with a numeric ref: 0"


def test_item_tools_parse_item_value_basic():

    for x in [37, 19.3, True, False, None, 'simple text']:
        assert ItemTools.parse_item_value(x) == x

    assert ItemTools.parse_item_value('3') == 3
    assert ItemTools.parse_item_value('+3') == 3
    assert ItemTools.parse_item_value('-3') == -3

    assert ItemTools.parse_item_value('3.5') == 3.5
    assert ItemTools.parse_item_value('+3.5') == 3.5
    assert ItemTools.parse_item_value('-3.5') == -3.5

    assert ItemTools.parse_item_value('3.5e1') == 35.0
    assert ItemTools.parse_item_value('+3.5e1') == 35.0
    assert ItemTools.parse_item_value('-3.5e1') == -35.0

    assert ItemTools.parse_item_value('') is None

    assert ItemTools.parse_item_value('null') is None
    assert ItemTools.parse_item_value('Null') is None
    assert ItemTools.parse_item_value('NULL') is None

    assert ItemTools.parse_item_value('true') is True
    assert ItemTools.parse_item_value('True') is True
    assert ItemTools.parse_item_value('TRUE') is True

    assert ItemTools.parse_item_value('false') is False
    assert ItemTools.parse_item_value('False') is False
    assert ItemTools.parse_item_value('FALSE') is False

    assert ItemTools.parse_item_value('|') == []  # special case: lone '|' means empty
    assert ItemTools.parse_item_value('alpha|') == ['alpha']  # special case: trailing '|' means singleton
    assert ItemTools.parse_item_value('|alpha|') == [None, 'alpha']
    assert ItemTools.parse_item_value('|alpha') == [None, 'alpha']
    assert ItemTools.parse_item_value('alpha|beta|gamma') == ['alpha', 'beta', 'gamma']
    assert ItemTools.parse_item_value('alpha|true|false|null||7|1.5') == ['alpha', True, False, None, None, 7, 1.5]


@pytest.mark.parametrize('instaguids_enabled', [True, False])
def test_item_tools_parse_item_value_guids(instaguids_enabled):

    with local_attrs(ItemTools, INSTAGUIDS_ENABLED=instaguids_enabled):

        sample_simple_field_input = "#foo"

        parsed = ItemTools.parse_item_value(sample_simple_field_input)
        assert parsed == sample_simple_field_input

        context = {}
        parsed = ItemTools.parse_item_value(sample_simple_field_input, context=context)
        if instaguids_enabled:
            assert is_uuid(parsed)
            assert parsed == context[sample_simple_field_input]
        else:
            assert parsed == sample_simple_field_input
            assert context == {}

        sample_compound_field_input = '#foo|#bar'
        sample_compound_field_list = ['#foo', '#bar']

        parsed = ItemTools.parse_item_value(sample_compound_field_input)
        assert parsed == sample_compound_field_list

        context = {}
        parsed = ItemTools.parse_item_value(sample_compound_field_input, context=context)
        assert isinstance(parsed, list)
        if instaguids_enabled:
            assert all(is_uuid(x) for x in parsed)
            assert '#foo' in context and '#bar' in context
        else:
            assert parsed == sample_compound_field_list
            assert context == {}


def test_item_tools_set_path_value():
    x = {'foo': 1, 'bar': 2}
    ItemTools.set_path_value(x, ['foo'], 3)
    assert x == {'foo': 3, 'bar': 2}

    x = {'foo': [11, 22, 33], 'bar': {'x': 'xx', 'y': 'yy'}}
    ItemTools.set_path_value(x, ['foo', 1], 17)
    assert x == {'foo': [11, 17, 33], 'bar': {'x': 'xx', 'y': 'yy'}}

    x = {'foo': [11, 22, 33], 'bar': {'x': 'xx', 'y': 'yy'}}
    ItemTools.set_path_value(x, ['bar', 'x'], 'something')
    assert x == {'foo': [11, 22, 33], 'bar': {'x': 'something', 'y': 'yy'}}


def test_item_tools_find_type_hint():
    assert ItemTools.find_type_hint(None, 'anything') is None

    assert ItemTools.find_type_hint(['foo', 'bar'], None) is None
    assert ItemTools.find_type_hint(['foo', 'bar'], "something") is None
    assert ItemTools.find_type_hint(['foo', 'bar'], {}) is None

    actual = ItemTools.find_type_hint(['foo', 'bar'], {"type": "object"})
    assert actual is None

    schema = {
        "type": "object",
        "properties": {
            "foo": {
                "type": "boolean"
            }
        }
    }
    actual = ItemTools.find_type_hint(['foo', 'bar'], schema)
    assert actual is None

    actual = ItemTools.find_type_hint(['foo'], schema)
    assert isinstance(actual, BoolHint)

    schema = {
        "type": "object",
        "properties": {
            "foo": {
                "type": "object",
                "properties": {
                    "bar": {
                        "type": "boolean"
                    }
                }
            }
        }
    }
    actual = ItemTools.find_type_hint(['foo', 'bar'], schema)
    assert isinstance(actual, BoolHint)

    actual = ItemTools.find_type_hint(['foo'], schema)
    assert actual is None


def test_item_manager_registry_manager_for_filename():
    assert ITEM_MANAGER_REGISTRY.manager_for_filename("xyz/foo.csv") == CsvItemManager

    with pytest.raises(Exception) as exc:
        ITEM_MANAGER_REGISTRY.manager_for_filename("xyz/foo.something.missing")
    assert str(exc.value) == "Unknown file type: xyz/foo.something.missing"


def test_xlsx_item_manager_load_content():
    it = XlsxItemManager(SAMPLE_XLSX_FILE, autoload_schemas=False)
    assert it.load_content() == SAMPLE_XLSX_FILE_ITEM_CONTENT


def test_xlsx_item_manager_load():
    assert XlsxItemManager.load(SAMPLE_XLSX_FILE, autoload_schemas=False) == SAMPLE_XLSX_FILE_ITEM_CONTENT


def test_xlsx_item_manager_load_csv():
    with pytest.raises(LoadArgumentsError) as exc:
        XlsxItemManager.load(SAMPLE_CSV_FILE)
    assert str(exc.value).startswith('The TableSetManager subclass XlsxItemManager'
                                     ' expects only .xlsx filenames:')


def test_csv_item_manager_load_content():
    it = CsvItemManager(SAMPLE_CSV_FILE, autoload_schemas=False)
    assert it.load_content() == SAMPLE_CSV_FILE_ITEM_CONTENT


def test_csv_item_manager_load():
    assert CsvItemManager.load(SAMPLE_CSV_FILE, autoload_schemas=False) == SAMPLE_CSV_FILE_ITEM_CONTENT


def test_csv_item_manager_load_csv():
    with pytest.raises(LoadArgumentsError) as exc:
        CsvItemManager.load(SAMPLE_XLSX_FILE, autoload_schemas=False)
    assert str(exc.value).startswith('The TableSetManager subclass CsvItemManager'
                                     ' expects only .csv filenames:')


def test_tsv_item_manager_load_content():
    it = TsvItemManager(SAMPLE_TSV_FILE, autoload_schemas=False)
    assert it.load_content() == SAMPLE_TSV_FILE_ITEM_CONTENT


def test_tsv_item_manager_load():
    assert TsvItemManager.load(SAMPLE_TSV_FILE, autoload_schemas=False) == SAMPLE_TSV_FILE_ITEM_CONTENT


def test_tsv_item_manager_load_csv():
    with pytest.raises(LoadArgumentsError) as exc:
        TsvItemManager.load(SAMPLE_XLSX_FILE, autoload_schemas=False)
    assert str(exc.value).startswith('The TableSetManager subclass TsvItemManager'
                                     ' expects only .tsv or .tsv.txt filenames:')


def test_item_manager_load():
    assert ItemManager.load(SAMPLE_XLSX_FILE, autoload_schemas=False) == SAMPLE_XLSX_FILE_ITEM_CONTENT
    assert ItemManager.load(SAMPLE_CSV_FILE, autoload_schemas=False) == SAMPLE_CSV_FILE_ITEM_CONTENT
    assert ItemManager.load(SAMPLE_TSV_FILE, autoload_schemas=False) == SAMPLE_TSV_FILE_ITEM_CONTENT

    loaded = ItemManager.load(SAMPLE_JSON_TABS_FILE, autoload_schemas=False)
    print("loaded=", json.dumps(loaded, indent=2))
    expected = SAMPLE_JSON_TABS_FILE_ITEM_CONTENT
    print("expected=", json.dumps(expected, indent=2))
    assert loaded == expected

    with pytest.raises(LoadArgumentsError) as exc:
        ItemManager.load("something.else")
    assert str(exc.value) == "Unknown file type: something.else"


def test_load_items():
    assert load_items(SAMPLE_XLSX_FILE, autoload_schemas=False) == SAMPLE_XLSX_FILE_ITEM_CONTENT
    assert load_items(SAMPLE_CSV_FILE, autoload_schemas=False) == SAMPLE_CSV_FILE_ITEM_CONTENT

    with pytest.raises(LoadArgumentsError) as exc:
        load_items("something.else")
    assert str(exc.value) == "Unknown file type: something.else"


SAMPLE_CSV_FILE2 = os.path.join(TEST_DIR, 'data_files/sample_items2.csv')

SAMPLE_CSV_FILE2_SHEET_NAME = infer_tab_name_from_filename(SAMPLE_CSV_FILE2)

SAMPLE_CSV_FILE2_SCHEMAS = {
    "Person": {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "sex": {"type": "string", "enum": ["Male", "Female"]},
            "member": {"type": "boolean"}
        }
    }
}

SAMPLE_CSV_FILE2_CONTENT = {
    SAMPLE_CSV_FILE2_SHEET_NAME: [
        {"name": "john", "sex": "M", "member": "false"},
        {"name": "juan", "sex": "male", "member": "true"},
        {"name": "igor", "sex": "unknown", "member": None},
        {"name": "mary", "sex": "Female", "member": "t"}
    ]
}

SAMPLE_CSV_FILE2_ITEM_CONTENT = {
    SAMPLE_CSV_FILE2_SHEET_NAME: [
        {"name": "john", "sex": "M", "member": False},
        {"name": "juan", "sex": "male", "member": True},
        {"name": "igor", "sex": "unknown", "member": None},
        {"name": "mary", "sex": "Female", "member": "t"}
    ]
}

SAMPLE_CSV_FILE2_PERSON_CONTENT_HINTED = {
    "Person": [
        {"name": "john", "sex": "Male", "member": False},
        {"name": "juan", "sex": "Male", "member": True},
        {"name": "igor", "sex": "unknown", "member": None},
        {"name": "mary", "sex": "Female", "member": True}
    ]
}


SAMPLE_JSON_FILE2 = os.path.join(TEST_DIR, 'data_files/sample_items2.json')

SAMPLE_JSON_FILE2_SHEET_NAME = infer_tab_name_from_filename(SAMPLE_JSON_FILE2)


SAMPLE_CSV_FILE3_SCHEMAS = {
    "Person": {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "sex": {"type": "string", "enum": ["Male", "Female"]},
            "children": {"type": "array", "items": {"type": "string"}},
            "parents": {"type": "array", "items": {"type": "string"}},
            "mother": {"type": "string"},
            "father": {"type": "string"},
        }
    }
}

SAMPLE_CSV_FILE3_PERSON_CONTENT_HINTED = {
    "Person": [
        {
            "name": "John",
            "uuid": "#john",
            "sex": "Male",
            "father": "#igor",
            "mother": "#mary",
            "parents": None,
            "children": None,
        },
        {
            "name": "Juan",
            "uuid": "#juan",
            "sex": "Male",
            "father": None,
            "mother": None,
            "parents": ["#igor", "#mary"],
            "children": None,
        },
        {
            "name": "Igor",
            "uuid": "#igor",
            "sex": "Male",
            "father": None,
            "mother": None,
            "parents": None,
            "children": ["#john"],
        },
        {
            "name": "Mary",
            "uuid": "#mary",
            "sex": "Female",
            "father": None,
            "mother": None,
            "parents": None,
            "children": ["#john"],
        },
    ]
}

SAMPLE_CSV_FILE3 = os.path.join(TEST_DIR, 'data_files/sample_items3.csv')


def matches_template(json1: AnyJsonData, json2: AnyJsonData, *, previous_matches: Dict[str, str] = None) -> bool:
    if previous_matches is None:
        previous_matches = {}
    if isinstance(json1, dict) and isinstance(json2, dict):
        keys1 = set(json1.keys())
        keys2 = set(json2.keys())
        if keys1 != keys2:
            print(f"Keys don't match: {keys1} vs {keys2}")
            return False
        return all(matches_template(json1[key], json2[key], previous_matches=previous_matches) for key in keys1)
    elif isinstance(json1, list) and isinstance(json2, list):
        n1 = len(json1)
        n2 = len(json2)
        if n1 != n2:
            print(f"Length doesn't match: {n1} vs {n2}")
            return False
        return all(matches_template(json1[i], json2[i], previous_matches=previous_matches) for i in range(n1))
    elif isinstance(json1, str) and isinstance(json2, str) and is_uuid(json1) and json2.startswith("#"):
        previously_matched = previous_matches.get(json2)
        if previously_matched:
            result = json1 == previously_matched
            if not result:
                print(f"Instaguid mismatch: {json1} vs {json2}")
            return result
        else:
            # Remember the match
            previous_matches[json2] = json1
            return True
    else:  # any other atomic items can be just directly compared
        result = json1 == json2
        if not result:
            print(f"Unequal: {json1} vs {json2}")
        return result


def test_load_items_with_schema():

    print("Case 1")
    expected = SAMPLE_CSV_FILE2_CONTENT
    actual = CsvManager.load(SAMPLE_CSV_FILE2)
    assert actual == expected

    print("Case 2")
    expected = SAMPLE_CSV_FILE2_ITEM_CONTENT
    actual = load_items(SAMPLE_CSV_FILE2, schemas=SAMPLE_CSV_FILE2_SCHEMAS)
    assert actual == expected

    print("Case 3")
    expected = SAMPLE_CSV_FILE2_PERSON_CONTENT_HINTED
    actual = load_items(SAMPLE_CSV_FILE2, schemas=SAMPLE_CSV_FILE2_SCHEMAS, tab_name='Person')
    assert actual == expected


def test_sample_items_csv_vs_json():

    csv_content = load_items(SAMPLE_CSV_FILE2, schemas=SAMPLE_CSV_FILE2_SCHEMAS, tab_name='Person')

    json_content = load_items(SAMPLE_JSON_FILE2, tab_name="Person")

    assert csv_content == json_content


def test_sample_items_json_vs_yaml():

    tabs_data_from_json = load_items(SAMPLE_JSON_TABS_FILE)
    tabs_data_from_yaml = load_items(SAMPLE_YAML_TABS_FILE)
    assert tabs_data_from_json == tabs_data_from_yaml


@pytest.mark.parametrize('instaguids_enabled', [True, False])
def test_load_items_with_schema_and_instaguids(instaguids_enabled):

    with local_attrs(ItemTools, INSTAGUIDS_ENABLED=instaguids_enabled):

        expected = SAMPLE_CSV_FILE3_PERSON_CONTENT_HINTED
        print("expected=", json.dumps(expected, indent=2))
        actual = load_items(SAMPLE_CSV_FILE3, schemas=SAMPLE_CSV_FILE3_SCHEMAS, tab_name='Person')
        print("actual=", json.dumps(actual, indent=2))
        if instaguids_enabled:
            assert matches_template(actual, expected)
        else:
            assert actual == expected  # no substitution performed


class SchemaAutoloaderForTesting(SchemaAutoloadMixin):

    def __init__(self, **kwargs):
        super().__init__(filename='ignored.file.name', **kwargs)


@contextlib.contextmanager
def schema_autoloader_for_testing(**kwargs) -> SchemaAutoloadMixin:
    autoloader: Optional[SchemaAutoloadMixin] = None
    success = False
    try:
        autoloader: SchemaAutoloadMixin = SchemaAutoloaderForTesting(**kwargs)
        assert autoloader.SCHEMA_CACHE == {}, "The schema cache is not clean."
        yield autoloader
        success = True
    finally:
        if autoloader is not None:
            autoloader.clear_schema_cache()
            assert autoloader.SCHEMA_CACHE == SchemaAutoloadMixin.SCHEMA_CACHE == {}
        if not success:
            raise


@using_fresh_ff_state_for_testing()
@pytest.mark.integrated
@pytest.mark.parametrize('portal_env', [None, 'data'])
def test_schema_autoload_mixin_caching(portal_env):

    with schema_autoloader_for_testing(portal_env=portal_env) as autoloader:

        assert autoloader.portal_env == 'data'  # it should have defaulted even if we didn't supply it

        assert autoloader.SCHEMA_CACHE == SchemaAutoloadMixin.SCHEMA_CACHE == {}

        sample_schema_name = 'foo'
        sample_schema = {'mock_schema_for': 'foo'}

        with mock.patch.object(bundle_utils_module, "get_schema") as mock_get_schema:
            mock_get_schema.return_value = sample_schema
            assert autoloader.fetch_schema(sample_schema_name, portal_env=autoloader.portal_env) == sample_schema

        schema_cache_with_sample_schema = {sample_schema_name: sample_schema}
        assert SchemaAutoloadMixin.SCHEMA_CACHE == schema_cache_with_sample_schema
        assert autoloader.SCHEMA_CACHE == schema_cache_with_sample_schema


@using_fresh_ff_state_for_testing()
@pytest.mark.integrated
@pytest.mark.parametrize('portal_env', [None, 'data'])
def test_schema_autoload_mixin_fetch_schema(portal_env):

    with schema_autoloader_for_testing(portal_env=portal_env) as autoloader:

        assert autoloader.portal_env == 'data'

        user_schema = autoloader.fetch_schema('user', portal_env=autoloader.portal_env)

        assert user_schema['$id'] == '/profiles/user.json'
        assert user_schema['title'] == 'User'
        assert 'properties' in user_schema


@using_fresh_ff_state_for_testing()
@pytest.mark.integrated
@pytest.mark.parametrize('autoload_schemas', [True, False])
@pytest.mark.parametrize('cache_schemas', [True, False])
@pytest.mark.parametrize('portal_env', [None, 'data'])
def test_schema_autoload_mixin_fetch_relevant_schemas(autoload_schemas, cache_schemas, portal_env):

    with printed_output() as printed:
        with local_attrs(SchemaAutoloadMixin, CACHE_SCHEMAS=cache_schemas):
            with schema_autoloader_for_testing(portal_env=portal_env, autoload_schemas=autoload_schemas) as autoloader:

                assert autoloader.portal_env == ('data' if autoload_schemas or portal_env else None)

                if autoload_schemas:

                    schemas = autoloader.fetch_relevant_schemas(['User', 'Lab'])
                    assert isinstance(schemas, dict)
                    assert len(schemas) == 2
                    assert set(schemas.keys()) == {'User', 'Lab'}

                else:

                    assert autoloader.fetch_relevant_schemas(['User', 'Lab']) == {}

                if portal_env == 'data' or not autoload_schemas:
                    assert printed.lines == []
                else:
                    assert printed.lines == [
                        "The portal_env was not explicitly supplied. Schemas will come from portal_env='data'."
                    ]


SAMPLE_ITEMS_FOR_REAL_SCHEMAS_FILE = os.path.join(TEST_DIR, 'data_files/sample_items_for_real_schemas.csv')


@using_fresh_ff_state_for_testing()
@pytest.mark.integrated
def test_workbook_with_schemas():

    print()  # start o a fresh line

    SchemaAutoloadMixin.clear_schema_cache()

    actual_data = CsvManager(filename=SAMPLE_ITEMS_FOR_REAL_SCHEMAS_FILE, tab_name='ExperimentSeq').load_content()
    expected_data = {
        "ExperimentSeq": [
            {
                "accession": "foo",
                "fragment_size_selection_method": "spri"
            },
            {
                "accession": "bar",
                "fragment_size_selection_method": "blue"
            }
        ]
    }
    assert actual_data == expected_data

    actual_items = load_items(SAMPLE_ITEMS_FOR_REAL_SCHEMAS_FILE,
                              tab_name='ExperimentSeq', autoload_schemas=True)
    expected_items = {
        "ExperimentSeq": [
            {
                "accession": "foo",
                "fragment_size_selection_method": "SPRI beads"
            },
            {
                "accession": "bar",
                "fragment_size_selection_method": "BluePippin"
            }
        ]
    }
    assert actual_items == expected_items


@using_fresh_ff_state_for_testing()
@pytest.mark.integrated
def test_workbook_with_schemas_and_portal_vapp():

    print()  # start on a fresh line

    SchemaAutoloadMixin.clear_schema_cache()

    portal_env = public_env_name(EnvUtils.PRD_ENV_NAME)

    experiment_seq_schema = ff_utils_module.get_schema('ExperimentSeq', portal_env=portal_env)

    expected_items = {
        "ExperimentSeq": [
            {
                "accession": "foo",
                "fragment_size_selection_method": "SPRI beads"
            },
            {
                "accession": "bar",
                "fragment_size_selection_method": "BluePippin"
            }
        ]
    }

    class MockVapp(NamedObject, AbstractVirtualApp):

        def __init__(self, name):
            super().__init__(name=name)
            self.call_count = 0

        def get(self, path_url):
            assert path_url.startswith('/profiles/ExperimentSeq.json?')
            self.call_count += 1
            response = MockResponse(200, json=experiment_seq_schema)
            return response

    portal_vapp = MockVapp(name=f'MockVapp[{portal_env}]')

    old_count = portal_vapp.call_count

    with mock.patch.object(ff_utils_module, "get_authentication_with_server",
                           mock_not_called("get_authentication_with_server")):
        with mock.patch.object(ff_utils_module, "get_metadata",
                               mock_not_called("get_metadata")):
            actual_items = load_items(SAMPLE_ITEMS_FOR_REAL_SCHEMAS_FILE,
                                      tab_name='ExperimentSeq', autoload_schemas=True, portal_vapp=portal_vapp)

    assert portal_vapp.call_count == old_count + 1
    assert actual_items == expected_items
