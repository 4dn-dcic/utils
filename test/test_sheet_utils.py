import json
import os
import pytest

from collections import namedtuple
from dcicutils.common import AnyJsonData
from dcicutils.misc_utils import is_uuid, local_attrs
from dcicutils.sheet_utils import (
    # High-level interfaces
    ItemManager, load_items,
    # Low-level implementation
    BasicTableSetManager,
    ItemTools, XlsxManager, XlsxItemManager,
    CsvManager, CsvItemManager, TsvManager, TsvItemManager,
    # TypeHint, EnumHint,
    BoolHint,
    # Error handling
    LoadFailure, LoadArgumentsError, LoadTableError,
    # Utilities
    prefer_number, unwanted_kwargs,
)
from typing import Dict
from .conftest_settings import TEST_DIR


def test_load_failure():

    sample_message = "This is a test."

    load_failure_object = LoadFailure(sample_message)
    assert isinstance(load_failure_object, LoadFailure)
    assert str(load_failure_object) == sample_message


def test_load_argument_error():

    sample_message = "This is a test."

    load_failure_object = LoadArgumentsError(sample_message)
    assert isinstance(load_failure_object, LoadArgumentsError)
    assert str(load_failure_object) == sample_message


def test_load_table_error():

    sample_message = "This is a test."

    load_failure_object = LoadTableError(sample_message)
    assert isinstance(load_failure_object, LoadTableError)
    assert str(load_failure_object) == sample_message


def test_prefer_number():

    assert prefer_number('') is None
    assert prefer_number('123') == 123
    assert prefer_number('3.14') == 3.14
    assert prefer_number('abc') == 'abc'
    assert prefer_number('123i') == '123i'
    assert prefer_number('123e') == '123e'
    assert prefer_number('123e0') == 123.0
    assert prefer_number('123e1') == 1230.0
    assert prefer_number('123e+1') == 1230.0
    assert prefer_number('123e-1') == 12.3


def test_unwanted_kwargs_without_error():
    unwanted_kwargs(context="Function foo", kwargs={})
    unwanted_kwargs(context="Function foo", kwargs={}, context_plural=True, detailed=True)


tst_args = "context,context_plural,detailed,kwargs,message"

TstArgs = namedtuple("TstArgs1", tst_args, defaults=(None,) * len(tst_args.split(',')))


@pytest.mark.parametrize(tst_args, [
    TstArgs(context="Function foo", context_plural=False, detailed=False, kwargs={'a': 1},
            message="Function foo doesn't use keyword argument a."),
    TstArgs(context="Function foo", context_plural=False, detailed=False, kwargs={'a': 1, 'b': 2},
            message="Function foo doesn't use keyword arguments a and b."),
    TstArgs(context="Functions like foo", context_plural=True, detailed=False, kwargs={'a': 1},
            message="Functions like foo don't use keyword argument a."),
    TstArgs(context="Functions like foo", context_plural=True, detailed=False, kwargs={'a': 1, 'b': 2},
            message="Functions like foo don't use keyword arguments a and b."),
    # Don't need to do all the cases again
    TstArgs(context="Function foo", kwargs={'a': 1, 'b': 2},
            message="Function foo doesn't use keyword arguments a and b."),  # noQA - PyCharm can't see defaults
    TstArgs(context="Function foo", detailed=True, kwargs={'a': 1, 'b': 2},
            message="Function foo doesn't use keyword arguments a=1 and b=2."),  # noQA PyCharm can't see defaults
])
def test_unwanted_kwargs_with_error(context, context_plural, detailed, kwargs, message):

    with pytest.raises(LoadArgumentsError) as exc:
        unwanted_kwargs(context=context, kwargs=kwargs, context_plural=context_plural, detailed=detailed)
    assert str(exc.value) == message


def test_back_table_set_create_state():

    assert BasicTableSetManager._create_tab_processor_state('some-tab') is None


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


SAMPLE_XLSX_FILE = os.path.join(TEST_DIR, 'data_files/sample_items.xlsx')

SAMPLE_XLSX_FILE_RAW_CONTENT = {
    "Sheet1": [
        {"x": 1, "y.a": 1, "y.z": 1},
        {"x": 1, "y.a": 2, "y.z": 3},
        {"x": "alpha", "y.a": "beta", "y.z": "gamma|delta"},
    ],
    "Sheet2": [
        {
            "name": "bill", "age": 23,
            "mother.name": "mary", "mother.age": 58,
            "father.name": "fred", "father.age": 63,
            "friends#0.name": "sam", "friends#0.age": 22,
            "friends#1.name": "arthur", "friends#1.age": 19,
        },
        {
            "name": "joe", "age": 9,
            "mother.name": "estrella", "mother.age": 35,
            "father.name": "anthony", "father.age": 34,
            "friends#0.name": "anders", "friends#0.age": 9,
            "friends#1.name": None, "friends#1.age": None,
        },
    ]
}

SAMPLE_XLSX_FILE_ITEM_CONTENT = {
    "Sheet1": [
        {"x": 1, "y": {"a": 1, "z": 1}},
        {"x": 1, "y": {"a": 2, "z": 3}},
        {"x": "alpha", "y": {"a": "beta", "z": ["gamma", "delta"]}},
    ],
    "Sheet2": [
        {
            "name": "bill", "age": 23,
            "mother": {"name": "mary", "age": 58},
            "father": {"name": "fred", "age": 63},
            "friends": [
                {"name": "sam", "age": 22},
                {"name": "arthur", "age": 19},
            ]
        },
        {
            "name": "joe", "age": 9,
            "mother": {"name": "estrella", "age": 35},
            "father": {"name": "anthony", "age": 34},
            "friends": [
                {"name": "anders", "age": 9},
                {"name": None, "age": None}
            ]
        },
    ],
}

SAMPLE_CSV_FILE = os.path.join(TEST_DIR, 'data_files/sample_items_sheet2.csv')

SAMPLE_CSV_FILE_RAW_CONTENT = {CsvManager.DEFAULT_TAB_NAME: SAMPLE_XLSX_FILE_RAW_CONTENT['Sheet2']}

SAMPLE_CSV_FILE_ITEM_CONTENT = {CsvItemManager.DEFAULT_TAB_NAME: SAMPLE_XLSX_FILE_ITEM_CONTENT['Sheet2']}

SAMPLE_TSV_FILE = os.path.join(TEST_DIR, 'data_files/sample_items_sheet2.tsv')

SAMPLE_TSV_FILE_RAW_CONTENT = {TsvManager.DEFAULT_TAB_NAME: SAMPLE_XLSX_FILE_RAW_CONTENT['Sheet2']}

SAMPLE_TSV_FILE_ITEM_CONTENT = {TsvItemManager.DEFAULT_TAB_NAME: SAMPLE_XLSX_FILE_ITEM_CONTENT['Sheet2']}


def test_xlsx_manager_load_content():

    wt = XlsxManager(SAMPLE_XLSX_FILE)
    assert wt.load_content() == SAMPLE_XLSX_FILE_RAW_CONTENT


def test_xlsx_manager_load():

    assert XlsxManager.load(SAMPLE_XLSX_FILE) == SAMPLE_XLSX_FILE_RAW_CONTENT


def test_xlsx_manager_load_csv():

    with pytest.raises(LoadArgumentsError) as exc:
        XlsxManager.load(SAMPLE_CSV_FILE)
    assert str(exc.value).startswith('The TableSetManager subclass XlsxManager'
                                     ' expects only .xlsx filenames:')


def test_xlsx_item_manager_load_content():

    it = XlsxItemManager(SAMPLE_XLSX_FILE)
    assert it.load_content() == SAMPLE_XLSX_FILE_ITEM_CONTENT


def test_xlsx_item_manager_load():

    assert XlsxItemManager.load(SAMPLE_XLSX_FILE) == SAMPLE_XLSX_FILE_ITEM_CONTENT


def test_xlsx_item_manager_load_csv():

    with pytest.raises(LoadArgumentsError) as exc:
        XlsxItemManager.load(SAMPLE_CSV_FILE)
    assert str(exc.value).startswith('The TableSetManager subclass XlsxItemManager'
                                     ' expects only .xlsx filenames:')


def test_csv_manager_load_content():

    wt = CsvManager(SAMPLE_CSV_FILE)
    assert wt.load_content() == SAMPLE_CSV_FILE_RAW_CONTENT


def test_csv_manager_load():

    assert CsvManager.load(SAMPLE_CSV_FILE) == SAMPLE_CSV_FILE_RAW_CONTENT


def test_csv_manager_load_csv():

    with pytest.raises(LoadArgumentsError) as exc:
        CsvManager.load(SAMPLE_XLSX_FILE)
    assert str(exc.value).startswith('The TableSetManager subclass CsvManager'
                                     ' expects only .csv filenames:')


def test_csv_item_manager_load_content():

    it = CsvItemManager(SAMPLE_CSV_FILE)
    assert it.load_content() == SAMPLE_CSV_FILE_ITEM_CONTENT


def test_csv_item_manager_load():

    assert CsvItemManager.load(SAMPLE_CSV_FILE) == SAMPLE_CSV_FILE_ITEM_CONTENT


def test_csv_item_manager_load_csv():

    with pytest.raises(LoadArgumentsError) as exc:
        CsvItemManager.load(SAMPLE_XLSX_FILE)
    assert str(exc.value).startswith('The TableSetManager subclass CsvItemManager'
                                     ' expects only .csv filenames:')


def test_tsv_manager_load_content():

    wt = TsvManager(SAMPLE_TSV_FILE)
    assert wt.load_content() == SAMPLE_TSV_FILE_RAW_CONTENT


def test_tsv_manager_expand_escape_sequences():

    assert TsvManager.expand_escape_sequences("foo") == "foo"
    assert TsvManager.expand_escape_sequences("foo\\tbar") == "foo\tbar"
    assert TsvManager.expand_escape_sequences("\\r\\t\\n\\\\") == "\r\t\n\\"
    assert TsvManager.expand_escape_sequences("foo\\fbar") == "foo\\fbar"


def test_tsv_manager_load():

    assert TsvManager.load(SAMPLE_TSV_FILE) == SAMPLE_TSV_FILE_RAW_CONTENT


def test_tsv_manager_load_csv():

    with pytest.raises(LoadArgumentsError) as exc:
        TsvManager.load(SAMPLE_XLSX_FILE)
    assert str(exc.value).startswith('The TableSetManager subclass TsvManager'
                                     ' expects only .tsv or .tsv.txt filenames:')


def test_tsv_item_manager_load_content():

    it = TsvItemManager(SAMPLE_TSV_FILE)
    assert it.load_content() == SAMPLE_TSV_FILE_ITEM_CONTENT


def test_tsv_item_manager_load():

    assert TsvItemManager.load(SAMPLE_TSV_FILE) == SAMPLE_TSV_FILE_ITEM_CONTENT


def test_tsv_item_manager_load_csv():

    with pytest.raises(LoadArgumentsError) as exc:
        TsvItemManager.load(SAMPLE_XLSX_FILE)
    assert str(exc.value).startswith('The TableSetManager subclass TsvItemManager'
                                     ' expects only .tsv or .tsv.txt filenames:')


def test_item_manager_load():

    assert ItemManager.load(SAMPLE_XLSX_FILE) == SAMPLE_XLSX_FILE_ITEM_CONTENT

    assert ItemManager.load(SAMPLE_CSV_FILE) == SAMPLE_CSV_FILE_ITEM_CONTENT

    assert ItemManager.load(SAMPLE_TSV_FILE) == SAMPLE_TSV_FILE_ITEM_CONTENT

    with pytest.raises(LoadArgumentsError) as exc:
        ItemManager.load("something.else")
    assert str(exc.value) == "Unknown file type: something.else"


def test_load_items():

    assert load_items(SAMPLE_XLSX_FILE) == SAMPLE_XLSX_FILE_ITEM_CONTENT

    assert load_items(SAMPLE_CSV_FILE) == SAMPLE_CSV_FILE_ITEM_CONTENT

    with pytest.raises(LoadArgumentsError) as exc:
        load_items("something.else")
    assert str(exc.value) == "Unknown file type: something.else"


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
    CsvManager.DEFAULT_TAB_NAME: [
        {"name": "john", "sex": "M", "member": "false"},
        {"name": "juan", "sex": "male", "member": "true"},
        {"name": "igor", "sex": "unknown", "member": None},
        {"name": "mary", "sex": "Female", "member": "t"}
    ]
}

SAMPLE_CSV_FILE2_ITEM_CONTENT = {
    CsvItemManager.DEFAULT_TAB_NAME: [
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

SAMPLE_CSV_FILE2 = os.path.join(TEST_DIR, 'data_files/sample_items2.csv')

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
