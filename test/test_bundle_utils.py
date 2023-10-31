import contextlib
import glob
import json
import os
import pytest
import re

from dcicutils import (
    bundle_utils as bundle_utils_module,
    ff_utils as ff_utils_module,
    validation_utils as validation_utils_module
)
from dcicutils.bundle_utils import (
    # High-level interfaces
    load_table_structures, load_items,
    # Low-level implementation
    SchemaManager, ItemTools, TableChecker, OptionalTypeHints,
    BoolHint, NumHint,
    # Probably we should test NumHint, TypeHint, EnumHint, RefHint, etc. as well. -kmp 23-Oct-2023
)
from dcicutils.common import AnyJsonData
from dcicutils.env_utils import EnvUtils, public_env_name
from dcicutils.misc_utils import (
    ignored, is_uuid, NamedObject, AbstractVirtualApp, to_snake_case, json_file_contents, find_association,
)
from dcicutils.qa_utils import printed_output, mock_not_called, MockResponse
from dcicutils.sheet_utils import (
    CsvManager, LoadArgumentsError, LoadTableError,
    infer_tab_name_from_filename, load_table_set,
)
from typing import Dict
from unittest import mock
from .conftest_settings import TEST_DIR
from .helpers import using_fresh_ff_state_for_testing
from .helpers_for_bundles import (
    SAMPLE_PROJECT_UUID, SAMPLE_INSTITUTION_UUID,
    SAMPLE_WORKBOOK_WITH_UNMATCHED_UUID_REFS, SAMPLE_WORKBOOK_WITH_MATCHED_UUID_REFS,
    SAMPLE_WORKBOOK_WITH_NAME_REFS,
)
from .test_sheet_utils import (
    SAMPLE_XLSX_FILE, SAMPLE_XLSX_FILE_ITEM_CONTENT,  # SAMPLE_XLSX_FILE_RAW_CONTENT,
    SAMPLE_XLSX_FILE_INFLATED_CONTENT,
    SAMPLE_CSV_FILE, SAMPLE_CSV_FILE_ITEM_CONTENT,  # SAMPLE_CSV_FILE_RAW_CONTENT,
    SAMPLE_CSV_FILE_INFLATED_CONTENT,
    SAMPLE_TSV_FILE, SAMPLE_TSV_FILE_ITEM_CONTENT,  # SAMPLE_TSV_FILE_RAW_CONTENT,
    SAMPLE_TSV_FILE_INFLATED_CONTENT,
    SAMPLE_JSON_TABS_FILE, SAMPLE_JSON_TABS_FILE_ITEM_CONTENT,
    SAMPLE_YAML_TABS_FILE,
)


def test_optional_type_hints():

    x = OptionalTypeHints()
    assert x.positional_hints == []
    assert x.other_hints == {}
    assert x[0] is None
    assert x[100] is None
    with pytest.raises(ValueError) as exc:
        print(x[-1])
    assert str(exc.value) == "Negative hint positions are not allowed: -1"

    bh = BoolHint()
    nh = NumHint()
    ih = NumHint(declared_type='int')

    x = OptionalTypeHints([bh, nh])
    assert x.positional_hints == [bh, nh]
    assert x.other_hints == {}
    assert x[0] is bh
    assert x[1] is nh
    assert x[2] is None

    x = OptionalTypeHints([bh, nh], positional_breadcrumbs=[('foo', 'x'), ('foo', 'y')])
    assert x.positional_hints == [bh, nh]
    assert x.other_hints == {
        ('foo', 'x'): bh,
        ('foo', 'y'): nh,
    }
    assert x[0] is bh
    assert x[1] is nh
    assert x[2] is None
    assert x[('something',)] is None
    assert x[('foo', 'x')] is bh
    assert x[('foo', 'y')] is nh
    assert x[('foo', 'z')] is None

    with pytest.raises(ValueError) as exc:
        x[2] = bh
    assert str(exc.value) == "Cannot assign OptionalTypeHints by position after initial creation: 2"
    assert x.positional_hints == [bh, nh]

    with pytest.raises(ValueError) as exc:
        x['something'] = bh
    assert str(exc.value) == "Attempt to set an OptionalTypeHints key to other than a breadcrumbs tuple: 'something'"
    assert x.positional_hints == [bh, nh]

    x[('something',)] = ih
    assert x.positional_hints == [bh, nh]
    assert x.other_hints == {
        ('foo', 'x'): bh,
        ('foo', 'y'): nh,
        ('something',): ih,
    }
    assert x[('something',)] == ih

    with pytest.raises(ValueError) as exc:
        x[('something',)] = ih
    assert str(exc.value) == "Attempt to redefine OptionalTypeHint key ('something',)."


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

    expectations = [

        # Integers
        ('3', 3), ('+3', 3), ('-3', -3),

        # Floats
        ('3.5', 3.5), ('+3.5', 3.5), ('-3.5', -3.5),
        ('3.5e1', 35.0), ('+3.5e1', 35.0), ('-3.5e1', -35.0),

        # Nulls
        (None, None),
        ('', None), ('null', None), ('Null', None), ('NULL', None),

        # Booleans
        ('true', True), ('True', True), ('TRUE', True),
        ('false', False), ('False', False), ('FALSE', False),
    ]

    for input, heuristic_result in expectations:
        assert ItemTools.parse_item_value(input) == input
        assert ItemTools.parse_item_value(input, apply_heuristics=False) == input
        assert ItemTools.parse_item_value(input, apply_heuristics=True) == heuristic_result
        assert ItemTools.parse_item_value(input, apply_heuristics=True, split_pipe=False) == heuristic_result
        assert ItemTools.parse_item_value(input, apply_heuristics=True, split_pipe=True) == heuristic_result

    expectations = [
        # Lists
        ('|', []),  # special case: lone '|' means empty
        ('alpha|', ['alpha']), ('7|', [7]),  # special case: trailing '|' means singleton
        # These follow from general case of '|' as separator of items recursively parsed
        ('|alpha', [None, 'alpha']), ('|alpha|', [None, 'alpha']), ('|7', [None, 7]),
        ('alpha|beta|gamma', ['alpha', 'beta', 'gamma']),
        ('alpha|true|false|null||7|1.5', ['alpha', True, False, None, None, 7, 1.5])
    ]

    for input, heuristic_result in expectations:
        assert ItemTools.parse_item_value(input) == input
        assert ItemTools.parse_item_value(input, apply_heuristics=False) == input
        assert ItemTools.parse_item_value(input, apply_heuristics=True) == input
        assert ItemTools.parse_item_value(input, apply_heuristics=True, split_pipe=False) == input
        assert ItemTools.parse_item_value(input, apply_heuristics=True, split_pipe=True) == heuristic_result


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
    assert ItemTools.find_type_hint_for_parsed_header(None, 'anything') is None

    assert ItemTools.find_type_hint_for_parsed_header(['foo', 'bar'], None) is None
    assert ItemTools.find_type_hint_for_parsed_header(['foo', 'bar'], "something") is None
    assert ItemTools.find_type_hint_for_parsed_header(['foo', 'bar'], {}) is None

    actual = ItemTools.find_type_hint_for_parsed_header(['foo', 'bar'], {"type": "object"})
    assert actual is None

    schema = {
        "type": "object",
        "properties": {
            "foo": {
                "type": "boolean"
            }
        }
    }
    actual = ItemTools.find_type_hint_for_parsed_header(['foo', 'bar'], schema)
    assert actual is None

    actual = ItemTools.find_type_hint_for_parsed_header(['foo'], schema)
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
    actual = ItemTools.find_type_hint_for_parsed_header(['foo', 'bar'], schema)
    assert isinstance(actual, BoolHint)

    actual = ItemTools.find_type_hint_for_parsed_header(['foo'], schema)
    assert actual is None


def test_load_table_structures():
    assert load_table_structures(SAMPLE_XLSX_FILE, apply_heuristics=True) == SAMPLE_XLSX_FILE_INFLATED_CONTENT
    assert load_table_structures(SAMPLE_CSV_FILE, apply_heuristics=True) == SAMPLE_CSV_FILE_INFLATED_CONTENT
    assert load_table_structures(SAMPLE_TSV_FILE, apply_heuristics=True) == SAMPLE_TSV_FILE_INFLATED_CONTENT

    loaded = load_table_structures(SAMPLE_JSON_TABS_FILE)
    print("loaded=", json.dumps(loaded, indent=2))
    expected = SAMPLE_JSON_TABS_FILE_ITEM_CONTENT
    print("expected=", json.dumps(expected, indent=2))
    assert loaded == expected

    with pytest.raises(LoadArgumentsError) as exc:
        load_table_structures("something.else")
    assert str(exc.value) == "Unknown file type: something.else"


@contextlib.contextmanager
def no_schemas():

    with mock.patch.object(validation_utils_module, "get_schema") as mock_get_schema:
        mock_get_schema.return_value = {}
        yield


def test_load_items():

    # with mock.patch.object(validation_utils_module, "get_schema") as mock_get_schema:
    #     mock_get_schema.return_value = {}
    with no_schemas():

        assert load_items(SAMPLE_XLSX_FILE, apply_heuristics=True) == SAMPLE_XLSX_FILE_ITEM_CONTENT
        assert load_items(SAMPLE_CSV_FILE, apply_heuristics=True) == SAMPLE_CSV_FILE_ITEM_CONTENT
        assert load_items(SAMPLE_TSV_FILE, apply_heuristics=True) == SAMPLE_TSV_FILE_ITEM_CONTENT

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

    print()  # start on a fresh line

    print("Case 1")
    expected = SAMPLE_CSV_FILE2_CONTENT
    actual = CsvManager.load(SAMPLE_CSV_FILE2)
    assert actual == expected

    print("Case 2")
    file_base_name = os.path.splitext(os.path.basename(SAMPLE_CSV_FILE2))[0]
    expected = SAMPLE_CSV_FILE2_ITEM_CONTENT
    actual = load_items(SAMPLE_CSV_FILE2, override_schemas={file_base_name: {}}, apply_heuristics=True)
    assert actual == expected

    print("Case 3")
    expected = SAMPLE_CSV_FILE2_PERSON_CONTENT_HINTED
    actual = load_items(SAMPLE_CSV_FILE2, override_schemas=SAMPLE_CSV_FILE2_SCHEMAS, tab_name='Person')
    assert actual == expected


def test_sample_items_csv_vs_json():

    csv_content = load_items(SAMPLE_CSV_FILE2, tab_name='Person', override_schemas=SAMPLE_CSV_FILE2_SCHEMAS)

    json_content = load_items(SAMPLE_JSON_FILE2, tab_name="Person", override_schemas=SAMPLE_CSV_FILE2_SCHEMAS)

    assert csv_content == json_content


def test_sample_items_json_vs_yaml():

    with SchemaManager.fresh_schema_manager_context_for_testing():

        # with mock.patch.object(validation_utils_module, "get_schema") as mock_get_schema:
        #     mock_get_schema.return_value = {}  # no schema checking
        with no_schemas():

            tabs_data_from_json = load_items(SAMPLE_JSON_TABS_FILE)
            tabs_data_from_yaml = load_items(SAMPLE_YAML_TABS_FILE)
            assert tabs_data_from_json == tabs_data_from_yaml


@using_fresh_ff_state_for_testing()
@pytest.mark.integrated
@pytest.mark.parametrize('portal_env', [None, 'data'])
def test_schema_autoload_mixin_caching(portal_env):

    with SchemaManager.fresh_schema_manager_context_for_testing():

        schema_manager = SchemaManager(portal_env=portal_env)

        assert schema_manager.portal_env == 'data'  # it should have defaulted even if we didn't supply it

        assert schema_manager.SCHEMA_CACHE == {}

        sample_schema_name = 'foo'
        sample_schema = {'mock_schema_for': 'foo'}

        with mock.patch.object(validation_utils_module, "get_schema") as mock_get_schema:
            mock_get_schema.return_value = sample_schema
            assert schema_manager.fetch_schema(sample_schema_name) == sample_schema

        schema_cache_with_sample_schema = {sample_schema_name: sample_schema}
        assert schema_manager.SCHEMA_CACHE == schema_cache_with_sample_schema


@using_fresh_ff_state_for_testing()
@pytest.mark.integrated
@pytest.mark.parametrize('portal_env', [None, 'data'])
def test_schema_autoload_mixin_fetch_schema(portal_env):

    with SchemaManager.fresh_schema_manager_context_for_testing():

        schema_manager = SchemaManager(portal_env=portal_env)

        assert schema_manager.portal_env == 'data'

        user_schema = schema_manager.fetch_schema('user')

        assert user_schema['$id'] == '/profiles/user.json'
        assert user_schema['title'] == 'User'
        assert 'properties' in user_schema


@using_fresh_ff_state_for_testing()
@pytest.mark.integrated
@pytest.mark.parametrize('portal_env', [None, 'data'])
def test_schema_autoload_mixin_fetch_relevant_schemas(portal_env):

    with printed_output() as printed:
        with SchemaManager.fresh_schema_manager_context_for_testing():
            schema_manager = SchemaManager(portal_env=portal_env)
            schemas = schema_manager.fetch_relevant_schemas(['User', 'Lab'])
            assert isinstance(schemas, dict)
            assert len(schemas) == 2
            assert set(schemas.keys()) == {'User', 'Lab'}

            if portal_env == 'data':
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

    with SchemaManager.fresh_schema_manager_context_for_testing():

        actual_data = load_table_set(filename=SAMPLE_ITEMS_FOR_REAL_SCHEMAS_FILE, tab_name='ExperimentSeq')
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

        # portal_env = public_env_name(EnvUtils.PRD_ENV_NAME)

        actual_items = load_items(SAMPLE_ITEMS_FOR_REAL_SCHEMAS_FILE,
                                  tab_name='ExperimentSeq')
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

    with SchemaManager.fresh_schema_manager_context_for_testing():

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
                                          tab_name='ExperimentSeq', portal_vapp=portal_vapp)

        assert portal_vapp.call_count == old_count + 1
        assert actual_items == expected_items


_SAMPLE_SCHEMA_DIR = os.path.join(TEST_DIR, "data_files", "sample_schemas")
_SAMPLE_SCHEMAS = {
    os.path.splitext(os.path.basename(file))[0]: json_file_contents(file)
    for file in glob.glob(os.path.join(_SAMPLE_SCHEMA_DIR, "*.json"))
}

_SAMPLE_INSERTS_DIR = os.path.join(TEST_DIR, "data_files", "sample_inserts")
_SAMPLE_INSERTS = load_table_set(_SAMPLE_INSERTS_DIR)

ID_NAME_PATTERN = re.compile("^/?([^/]*)/([^/]*)/?$")


@contextlib.contextmanager
def mocked_schemas(mock_remotes: bool = True, expected_portal_env=None, expected_portal_vapp=None):

    def lookup_mock_schema(item_type):
        schema = _SAMPLE_SCHEMAS.get(item_type)
        assert schema, f"The item type {item_type} is not mocked."
        return schema

    def lookup_sample_insert(item_type, item_ref):
        data = _SAMPLE_INSERTS[item_type]
        schema = lookup_mock_schema(item_type)
        possible_identifying_properties = set(schema.get("identifyingProperties") or []) | {'uuid'}
        if not data:
            return None
        for prop in possible_identifying_properties:
            if prop not in data[0]:
                continue
            found = find_association(data, **{prop: item_ref})
            if found:
                return found
        return None

    def mocked_get_schema(schema_name, portal_env=None, portal_vapp=None):
        if expected_portal_env is not None:
            assert portal_env == expected_portal_env, (f"get_schema got ff_env={portal_env!r},"
                                                       f" but expected ff_env={expected_portal_env!r}.")
        if expected_portal_vapp is not None:
            assert portal_vapp == expected_portal_vapp, (f"get_schema got portal_vapp={portal_vapp!r},"
                                                         f" but expected portal_vapp={expected_portal_vapp!r}.")
        schema_snake_name = to_snake_case(schema_name)
        return lookup_mock_schema(schema_snake_name)

    def mocked_get_metadata(obj_id, key=None, ff_env=None, check_queue=False, add_on=''):
        ignored(key, ff_env, check_queue, add_on)
        if not mock_remotes:
            raise Exception("No mock-remote {obj_id} was found.")
        parts = ID_NAME_PATTERN.match(obj_id)
        assert parts, f"mocked_get_metadata got {obj_id}, but expected /<object-type>/<object-ref>"
        item_type, item_ref = parts.groups()
        return lookup_sample_insert(item_type=item_type, item_ref=item_ref)

    with mock.patch.object(validation_utils_module, "get_schema") as mock_get_schema:
        mock_get_schema.side_effect = mocked_get_schema
        with mock.patch.object(bundle_utils_module, "get_metadata") as mock_get_metadata:
            mock_get_metadata.side_effect = mocked_get_metadata
            yield


def test_table_checker():

    print()  # start on a fresh line

    mock_ff_env = 'some-env'

    with mocked_schemas(mock_remotes=True):

        with printed_output() as printed:
            with pytest.raises(Exception) as exc:
                checker = TableChecker(SAMPLE_WORKBOOK_WITH_UNMATCHED_UUID_REFS,
                                       flattened=True,
                                       portal_env=mock_ff_env)
                checker.check_tabs()
            assert str(exc.value) == "There were 2 problems while compiling hints."
            assert printed.lines == [
                f"Problem: User[0].project: Unable to validate Project reference: {SAMPLE_PROJECT_UUID!r}",
                (f"Problem: User[0].user_institution: Unable to validate Institution reference:"
                 f" {SAMPLE_INSTITUTION_UUID!r}")
            ]

        checker = TableChecker(SAMPLE_WORKBOOK_WITH_MATCHED_UUID_REFS,
                               flattened=True,
                               portal_env=mock_ff_env)
        checker.check_tabs()

        checker = TableChecker(SAMPLE_WORKBOOK_WITH_NAME_REFS,
                               flattened=True,
                               portal_env=mock_ff_env)
        checker.check_tabs()
