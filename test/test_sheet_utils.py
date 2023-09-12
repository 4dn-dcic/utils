# import contextlib
import json
import os
import pytest

from collections import namedtuple
# from dcicutils import bundle_utils as bundle_utils_module, ff_utils as ff_utils_module
# from dcicutils.common import AnyJsonData
# from dcicutils.env_utils import EnvUtils, public_env_name
# from dcicutils.misc_utils import is_uuid, local_attrs, NamedObject, AbstractVirtualApp
# from dcicutils.qa_utils import printed_output, mock_not_called, MockResponse
# from dcicutils.bundle_utils import (
#     # High-level interfaces
#     ItemManager, load_items, ITEM_MANAGER_REGISTRY,
#     # Low-level implementation
#     SchemaAutoloadMixin,
#     ItemTools,
#     XlsxItemManager,
#     CsvItemManager, TsvItemManager,
#     # TypeHint, EnumHint,
#     BoolHint,
# )
from dcicutils.sheet_utils import (
    # High-level interfaces
    TABLE_SET_MANAGER_REGISTRY,
    # Low-level implementation
    BasicTableSetManager,
    XlsxManager,
    CsvManager, TsvManager,
    # Error handling
    LoadFailure, LoadArgumentsError, LoadTableError,
    # Utilities
    prefer_number, unwanted_kwargs, expand_string_escape_sequences, infer_tab_name_from_filename,
)
# from typing import Dict, Optional
# from unittest import mock
from .conftest_settings import TEST_DIR
# from .helpers import using_fresh_ff_state_for_testing


TEST_SHEET_1 = 'Sheet1'


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


def test_expand_string_escape_sequences():
    assert expand_string_escape_sequences("foo") == "foo"
    assert expand_string_escape_sequences("foo\\tbar") == "foo\tbar"
    assert expand_string_escape_sequences("\\r\\t\\n\\\\") == "\r\t\n\\"
    assert expand_string_escape_sequences("foo\\fbar") == "foo\\fbar"


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


def test_infer_tab_name_from_filename():
    assert infer_tab_name_from_filename('some/dir/some') == 'some'
    assert infer_tab_name_from_filename('some/dir/some.file') == 'some'
    assert infer_tab_name_from_filename('some/dir/some.file.name') == 'some'


def test_table_set_manager_registry_manager_for_filename():
    assert TABLE_SET_MANAGER_REGISTRY.manager_for_filename("xyz/foo.csv") == CsvManager

    with pytest.raises(Exception) as exc:
        TABLE_SET_MANAGER_REGISTRY.manager_for_filename("xyz/foo.something.missing")
    assert str(exc.value) == "Unknown file type: xyz/foo.something.missing"


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

SAMPLE_CSV_FILE_SHEET_NAME = infer_tab_name_from_filename(SAMPLE_CSV_FILE)

SAMPLE_CSV_FILE_RAW_CONTENT = {SAMPLE_CSV_FILE_SHEET_NAME: SAMPLE_XLSX_FILE_RAW_CONTENT['Sheet2']}

SAMPLE_CSV_FILE_ITEM_CONTENT = {SAMPLE_CSV_FILE_SHEET_NAME: SAMPLE_XLSX_FILE_ITEM_CONTENT['Sheet2']}

SAMPLE_TSV_FILE = os.path.join(TEST_DIR, 'data_files/sample_items_sheet2.tsv')

SAMPLE_TSV_FILE_SHEET_NAME = infer_tab_name_from_filename(SAMPLE_TSV_FILE)

SAMPLE_TSV_FILE_RAW_CONTENT = {SAMPLE_TSV_FILE_SHEET_NAME: SAMPLE_XLSX_FILE_RAW_CONTENT['Sheet2']}

SAMPLE_TSV_FILE_ITEM_CONTENT = {SAMPLE_TSV_FILE_SHEET_NAME: SAMPLE_XLSX_FILE_ITEM_CONTENT['Sheet2']}

SAMPLE_JSON_TABS_FILE = os.path.join(TEST_DIR, 'data_files/sample_items.tabs.json')

SAMPLE_JSON_TABS_FILE_ITEM_CONTENT = SAMPLE_XLSX_FILE_ITEM_CONTENT

SAMPLE_YAML_TABS_FILE = os.path.join(TEST_DIR, 'data_files/sample_items.tabs.yaml')

SAMPLE_YAML_TABS_FILE_ITEM_CONTENT = SAMPLE_XLSX_FILE_ITEM_CONTENT


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


def test_csv_escaping():
    actual = CsvManager.load("test/data_files/escaping.csv", escaping=False)
    expected = json.load(open("test/data_files/escaping-false.json"))
    assert actual == expected

    actual = CsvManager.load("test/data_files/escaping.csv", escaping=True)
    expected = json.load(open("test/data_files/escaping-true.json"))
    assert actual == expected


def test_tsv_manager_load_content():
    wt = TsvManager(SAMPLE_TSV_FILE)
    assert wt.load_content() == SAMPLE_TSV_FILE_RAW_CONTENT


def test_tsv_manager_load():
    assert TsvManager.load(SAMPLE_TSV_FILE) == SAMPLE_TSV_FILE_RAW_CONTENT


def test_tsv_manager_load_csv():
    with pytest.raises(LoadArgumentsError) as exc:
        TsvManager.load(SAMPLE_XLSX_FILE)
    assert str(exc.value).startswith('The TableSetManager subclass TsvManager'
                                     ' expects only .tsv or .tsv.txt filenames:')
