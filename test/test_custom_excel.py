# Regression tests for the CustomExcel synthetic-array-column indexing.
#
# CustomExcel remaps spreadsheet columns into synthetic (array) columns. The synthetic
# array index (the N in "qc_values#N.key") is registered up-front in the sheet header by
# CustomExcelSheetReader._define_header and then emitted per row by _iter_mapper. If those
# two paths number the indices differently, the row emits synthetic column names that were
# never registered, and structured_data's _StructuredRowTemplate.set_value silently drops
# them (it is a no-op for unregistered column names) -- i.e. mapped values disappear with
# no error.
#
# These tests drive the full path through StructuredDataSet (portal=None, no schemas) and
# assert the exact materialized objects, so a dropped synthetic column shows up as a missing
# field. They cover the cases where header/row numbering used to diverge: multiple array
# groups, a scalar mapping interleaved between array mappings, mixed array names within one
# source column, a bare "name#" placeholder, plus the ordinary single-array and empty-cell
# (compaction) behavior that must keep working.

import openpyxl
import pytest
from dcicutils.submitr.custom_excel import CustomExcel
from dcicutils.structured_data import StructuredDataSet

SHEET_NAME = "MyType"


def _materialize(tmp_path, monkeypatch, mapping: dict, header: list, rows: list) -> list:
    """Write a one-sheet xlsx, force CustomExcel to use the given (already post-processed)
    sheet->column mapping, load it via StructuredDataSet, and return the list of objects
    produced for the sheet."""
    monkeypatch.setattr(CustomExcel, "_get_custom_column_mappings",
                        staticmethod(lambda portal=None: {SHEET_NAME: mapping}))
    path = str(tmp_path / "custom_excel_test.xlsx")
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = SHEET_NAME
    sheet.append(header)
    for row in rows:
        sheet.append(row)
    workbook.save(path)
    structured_data = StructuredDataSet(file=path, portal=None, excel_class=CustomExcel)
    return structured_data.data.get(SHEET_NAME)


def test_single_array_two_columns(tmp_path, monkeypatch):
    # Ordinary case (non-regression): two source columns feed one array; indices 0 and 1.
    mapping = {
        "colA": {"qc_values#.value": "{value}", "qc_values#.key": "A"},
        "colB": {"qc_values#.value": "{value}", "qc_values#.key": "B"},
    }
    result = _materialize(tmp_path, monkeypatch, mapping, ["colA", "colB"], [["5", "9"]])
    assert result == [{"qc_values": [{"value": "5", "key": "A"},
                                     {"value": "9", "key": "B"}]}]


def test_empty_cell_produces_compact_array(tmp_path, monkeypatch):
    # An empty source cell is skipped and leaves no gap: the non-empty column takes index 0,
    # and there is no trailing null element for the (registered) unused index.
    mapping = {
        "colA": {"qc_values#.value": "{value}", "qc_values#.key": "A"},
        "colB": {"qc_values#.value": "{value}", "qc_values#.key": "B"},
    }
    result = _materialize(tmp_path, monkeypatch, mapping, ["colA", "colB"],
                          [["", "9"], ["7", "9"]])
    assert result == [
        {"qc_values": [{"value": "9", "key": "B"}]},
        {"qc_values": [{"value": "7", "key": "A"}, {"value": "9", "key": "B"}]},
    ]


def test_multiple_array_groups_are_not_dropped(tmp_path, monkeypatch):
    # Two distinct array names across two source columns. Previously the header registered
    # qc_flags#1 while the row emitted qc_flags#0, so the whole qc_flags group was dropped.
    mapping = {
        "colA": {"qc_values#.value": "{value}", "qc_values#.key": "A"},
        "colB": {"qc_flags#.value": "{value}", "qc_flags#.key": "B"},
    }
    result = _materialize(tmp_path, monkeypatch, mapping, ["colA", "colB"], [["5", "9"]])
    assert result == [{"qc_values": [{"value": "5", "key": "A"}],
                       "qc_flags": [{"value": "9", "key": "B"}]}]


def test_scalar_mapping_interleaved_between_arrays(tmp_path, monkeypatch):
    # A scalar (non-array) mapped column between two array columns must not consume an array
    # index. Previously the header registered qc_values#0 and qc_values#2 (the scalar column
    # bumped the shared counter) while the row emitted qc_values#0 and qc_values#1, dropping
    # the second array element.
    mapping = {
        "colA": {"qc_values#.value": "{value}"},
        "colB": {"note": "{value}"},
        "colC": {"qc_values#.value": "{value}"},
    }
    result = _materialize(tmp_path, monkeypatch, mapping, ["colA", "colB", "colC"],
                          [["5", "hello", "9"]])
    assert result == [{"qc_values": [{"value": "5"}, {"value": "9"}], "note": "hello"}]


def test_mixed_array_names_in_one_column(tmp_path, monkeypatch):
    # A single source column contributing to two different array names must populate both.
    # Previously the row picked only the first array name and left the second placeholder
    # bare (unregistered), dropping it.
    mapping = {
        "colA": {"qc_values#.value": "{value}", "qc_flags#.flag": "{value}"},
    }
    result = _materialize(tmp_path, monkeypatch, mapping, ["colA"], [["5"]])
    assert result == [{"qc_values": [{"value": "5"}], "qc_flags": [{"flag": "5"}]}]


def test_bare_array_placeholder_key(tmp_path, monkeypatch):
    # A synthetic key that is exactly "name#" (no trailing field) maps to a scalar array
    # element. Previously the header appended it verbatim ("qc_values#") while the row
    # rewrote it to "qc_values#0", a guaranteed mismatch that dropped the value.
    mapping = {
        "colA": {"qc_values#": "{value}"},
        "colB": {"qc_values#": "{value}"},
    }
    result = _materialize(tmp_path, monkeypatch, mapping, ["colA", "colB"], [["5", "9"]])
    assert result == [{"qc_values": ["5", "9"]}]


if __name__ == "__main__":
    pytest.main([__file__, "-vv"])
