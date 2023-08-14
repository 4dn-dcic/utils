import os
import pytest

from dcicutils.sheet_utils import WorkbookManager, ItemManager
from .conftest_settings import TEST_DIR


def test_item_manager_parse_sheet_header():
    assert ItemManager.parse_sheet_header('.a') == ['a']
    assert ItemManager.parse_sheet_header('a') == ['a']
    assert ItemManager.parse_sheet_header('#0') == [0]
    assert ItemManager.parse_sheet_header('0') == [0]
    assert ItemManager.parse_sheet_header('foo.bar') == ['foo', 'bar']
    assert ItemManager.parse_sheet_header('a.b#0') == ['a', 'b', 0]
    assert ItemManager.parse_sheet_header('x.xx#17#8.z') == ['x', 'xx', 17, 8, 'z']

    # We don't error-check this, but it shouldn't matter
    assert ItemManager.parse_sheet_header('#abc') == ['abc']
    assert ItemManager.parse_sheet_header('.123') == [123]
    assert ItemManager.parse_sheet_header('#abc.123#456.def') == ['abc', 123, 456, 'def']


def test_item_manager_parse_sheet_headers():
    input = ['a.b', 'a.c', 'a.d#1', 'a.d#2']
    expected = [['a', 'b'], ['a', 'c'], ['a', 'd', 1], ['a', 'd', 2]]
    assert ItemManager.parse_sheet_headers(input) == expected


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
def test_item_manager_compute_patch_prototype(parsed_headers, expected_prototype):
    parsed_headers = ItemManager.parse_sheet_headers(parsed_headers)
    assert ItemManager.compute_patch_prototype(parsed_headers) == expected_prototype


@pytest.mark.parametrize('headers', [['0'], ['x', '0.y']])
def test_item_manager_compute_patch_prototype_errors(headers):

    parsed_headers = ItemManager.parse_sheet_headers(headers)
    with pytest.raises(ValueError) as exc:
        ItemManager.compute_patch_prototype(parsed_headers)
    assert str(exc.value) == "A header cannot begin with a numeric ref: 0"


def test_item_manager_set_path_value():

    x = {'foo': 1, 'bar': 2}
    ItemManager.set_path_value(x, ['foo'], 3)
    assert x == {'foo': 3, 'bar': 2}

    x = {'foo': [11, 22, 33], 'bar': {'x': 'xx', 'y': 'yy'}}
    ItemManager.set_path_value(x, ['foo', 1], 17)
    assert x == {'foo': [11, 17, 33], 'bar': {'x': 'xx', 'y': 'yy'}}

    x = {'foo': [11, 22, 33], 'bar': {'x': 'xx', 'y': 'yy'}}
    ItemManager.set_path_value(x, ['bar', 'x'], 'something')
    assert x == {'foo': [11, 22, 33], 'bar': {'x': 'something', 'y': 'yy'}}


SAMPLE_FILE = os.path.join(TEST_DIR, 'data_files/sample_items.xlsx')

SAMPLE_FILE_RAW_CONTENT = {
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

SAMPLE_FILE_ITEM_CONTENT = {
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


def test_workbook_manager_load_content():

    wt = WorkbookManager(SAMPLE_FILE)
    assert wt.load_content() == SAMPLE_FILE_RAW_CONTENT


def test_workbook_manager_load_workbook():

    assert WorkbookManager.load_workbook(SAMPLE_FILE) == SAMPLE_FILE_RAW_CONTENT


def test_item_manager_parse_value():

    for x in [37, 19.3, True, False, None, 'simple text']:
        assert ItemManager.parse_value(x) == x

    assert ItemManager.parse_value('3') == 3
    assert ItemManager.parse_value('+3') == 3
    assert ItemManager.parse_value('-3') == -3

    assert ItemManager.parse_value('3.5') == 3.5
    assert ItemManager.parse_value('+3.5') == 3.5
    assert ItemManager.parse_value('-3.5') == -3.5

    assert ItemManager.parse_value('3.5e1') == 35.0
    assert ItemManager.parse_value('+3.5e1') == 35.0
    assert ItemManager.parse_value('-3.5e1') == -35.0

    assert ItemManager.parse_value('') is None

    assert ItemManager.parse_value('null') is None
    assert ItemManager.parse_value('Null') is None
    assert ItemManager.parse_value('NULL') is None

    assert ItemManager.parse_value('true') is True
    assert ItemManager.parse_value('True') is True
    assert ItemManager.parse_value('TRUE') is True

    assert ItemManager.parse_value('false') is False
    assert ItemManager.parse_value('False') is False
    assert ItemManager.parse_value('FALSE') is False

    assert ItemManager.parse_value('alpha|beta|gamma') == ['alpha', 'beta', 'gamma']
    assert ItemManager.parse_value('alpha|true|false|null||7|1.5') == ['alpha', True, False, None, None, 7, 1.5]


def test_item_manager_load_content():

    it = ItemManager(SAMPLE_FILE)
    assert it.load_content() == SAMPLE_FILE_ITEM_CONTENT


def test_item_manager_load_workbook():

    assert ItemManager.load_workbook(SAMPLE_FILE) == SAMPLE_FILE_ITEM_CONTENT
