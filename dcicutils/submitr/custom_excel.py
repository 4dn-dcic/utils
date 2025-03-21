from copy import deepcopy
import io
import json
import os
from requests import get as requests_get
from typing import Any, List, Optional
from dcicutils.data_readers import Excel, ExcelSheetReader
from dcicutils.misc_utils import to_boolean, to_float, to_integer

# This module implements a custom Excel spreadsheet class which support "custom column mappings",
# meaning that, and a very low/early level in processing, the columns/values in the spreadsheet
# can be redefined/remapped to different columns/values. The mapping is defined by a JSON config
# file (by default in config/custom_column_mappings.json). It can be thought of as a virtual
# preprocessing step on the spreadsheet. This was first implemented to support the simplified QC
# columns/values. For EXAMPLE, so the spreadsheet author can specify single columns like this:
#
#   total_raw_reads_sequenced: 11870183
#   total_raw_bases_sequenced: 44928835584
#
# But this will be mapped, i.e the system will act AS-IF we instead had these columns/values:
#
#   qc_values#0.derived_from: total_raw_reads_sequenced
#   qc_values#0.value:        11870183
#   qc_values#0.key:          Total Raw Reads Sequenced
#   qc_values#0.tooltip:      # of reads (150bp)
#   qc_values#1.derived_from: total_raw_bases_sequenced
#   qc_values#1.value:        44928835584
#   qc_values#1.key:          Total Raw Bases Sequenced
#   qc_values#1.tooltip:      None
#
# The relevant portion of the controlling config file (config/custom_column_mappings.json)
# for the above example looks something like this:
#
#   "sheet_mappings": {
#       "ExternalQualityMetric": "external_quality_metric"
#   },
#   "column_mappings": {
#       "external_quality_metric": {
#           "total_raw_reads_sequenced": {
#               "qc_values#.derived_from": "{name}",
#               "qc_values#.value": "{value:integer}",
#               "qc_values#.key": "Total Raw Reads Sequenced",
#               "qc_values#.tooltip": "# of reads (150bp)"
#           },
#           "total_raw_bases_sequenced": {
#               "qc_values#.derived_from": "{name}",
#               "qc_values#.value": "{value:integer}",
#               "qc_values#.key": "Total Raw Bases Sequenced",
#               "qc_values#.tooltip": null
#           },
#           "et cetera": "..."
#       }
#   }
#
# This says that for the ExternalQualityMetric sheet (only) the mappings with the config file
# section column_mappings.external_quality_metric will be applied. The "qc_values#" portion of
# the mapped columns names will be expanded to "qc_values#0" for total_raw_reads_sequenced items,
# and to "qc_values#1" for the total_raw_bases_sequenced items, and so on. This will be based on
# the ACTUAL columns present in the sheet; so if total_raw_reads_sequenced were not present in
# the sheet, then the total_raw_bases_sequenced items would be expanded to "qc_values#0".
# Note the special "{name}" and "{value}" values ("macros") for the target (synthetic) properties;
# these will be evaluated (here) to the name of the original property name and value, respectively.
#
# Since the (first) actual use-case of this is in fact for these qc_values, and since these have
# effectively untyped values (i.e. the ExternalQualityMetric schema specifies all primitive types
# as possible/acceptable types for qc_values.value), we also allow a ":TYPE" suffix for the
# special "{value}" macro, so that a specific primitive type may be specified, e.g. "{value:integer}"
# will evaluate the original property value as an integer (if it cannot be converted to an integer
# then whatever its value is, will be passed on through as a string).
#
# The hook for this is to pass the CustomExcel type to StructuredDataSet in submission.py.
# Note that the config file is fetched from GitHub, with a fallback to config/custom_column_mappings.json.
#
# ALSO ...
# This CustomExcel class also handles multiple sheets within a spreadsheet representing
# the same (portal) type; see comments below near the ExcelSheetName class definition.

CUSTOM_COLUMN_MAPPINGS_BASE_URL = "https://raw.githubusercontent.com/smaht-dac/submitr/refs/heads"
CUSTOM_COLUMN_MAPPINGS_BRANCH = "master"
CUSTOM_COLUMN_MAPPINGS_PATH = "submitr/config/custom_column_mappings.json"
CUSTOM_COLUMN_MAPPINGS_URL = f"{CUSTOM_COLUMN_MAPPINGS_BASE_URL}/{CUSTOM_COLUMN_MAPPINGS_BRANCH}/{CUSTOM_COLUMN_MAPPINGS_PATH}"  # noqa
CUSTOM_COLUMN_MAPPINGS_LOCAL = False

COLUMN_NAME_ARRAY_SUFFIX_CHAR = "#"
COLUMN_NAME_SEPARATOR = "."


class CustomExcel(Excel):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._custom_column_mappings = CustomExcel._get_custom_column_mappings()

    def sheet_reader(self, sheet_name: str) -> ExcelSheetReader:
        return CustomExcelSheetReader(self, sheet_name=sheet_name, workbook=self._workbook,
                                      custom_column_mappings=self._custom_column_mappings)

    @staticmethod
    def effective_sheet_name(sheet_name: str) -> str:
        if (underscore := sheet_name.find("_")) > 1:
            return sheet_name[underscore + 1:]
        return sheet_name

    @staticmethod
    def _get_custom_column_mappings() -> Optional[dict]:

        def fetch_custom_column_mappings():
            custom_column_mappings = None
            if CUSTOM_COLUMN_MAPPINGS_LOCAL is not True:
                # Fetch config file directly from GitHub (yes this repo is public).
                try:
                    custom_column_mappings = requests_get(CUSTOM_COLUMN_MAPPINGS_URL).json()
                except Exception:
                    pass
            if not custom_column_mappings:
                # Fallback to the actual config file in this package.
                try:
                    file = os.path.join(os.path.dirname(__file__), "config", "custom_column_mappings.json")
                    with io.open(file, "r") as f:
                        custom_column_mappings = json.load(f)
                except Exception:
                    custom_column_mappings = None
            if not isinstance(custom_column_mappings, dict):
                custom_column_mappings = {}
            return custom_column_mappings

        def post_process_custom_column_mappings(custom_column_mappings: dict) -> Optional[dict]:
            if isinstance(column_mappings := custom_column_mappings.get("column_mappings"), dict):
                if isinstance(sheet_mappings := custom_column_mappings.get("sheet_mappings"), dict):
                    for sheet_name in list(sheet_mappings.keys()):
                        if isinstance(sheet_mappings[sheet_name], str):
                            if isinstance(column_mappings.get(sheet_mappings[sheet_name]), dict):
                                sheet_mappings[sheet_name] = column_mappings.get(sheet_mappings[sheet_name])
                            else:
                                del sheet_mappings[sheet_name]
                        elif not isinstance(sheet_mappings[sheet_name], dict):
                            del sheet_mappings[sheet_name]
                return sheet_mappings
            return None

        if not (custom_column_mappings := fetch_custom_column_mappings()):
            return None
        if not (custom_column_mappings := post_process_custom_column_mappings(custom_column_mappings)):
            return None
        return custom_column_mappings


class CustomExcelSheetReader(ExcelSheetReader):

    def __init__(self, *args, **kwargs) -> None:
        ARGUMENT_NAME_SHEET_NAME = "sheet_name"
        ARGUMENT_NAME_CUSTOM_COLUMN_MAPPINGS = "custom_column_mappings"
        self._custom_column_mappings = None
        if ARGUMENT_NAME_CUSTOM_COLUMN_MAPPINGS in kwargs:
            def lookup_custom_column_mappings(custom_column_mappings: dict, sheet_name: str) -> Optional[dict]:
                if isinstance(custom_column_mappings, dict) and isinstance(sheet_name, str):
                    if isinstance(found_custom_column_mappings := custom_column_mappings.get(sheet_name), dict):
                        return found_custom_column_mappings
                    if (effective_sheet_name := CustomExcel.effective_sheet_name(sheet_name)) != sheet_name:
                        if isinstance(found_custom_column_mappings :=
                                      custom_column_mappings.get(effective_sheet_name), dict):
                            return found_custom_column_mappings
                return None
            custom_column_mappings = kwargs[ARGUMENT_NAME_CUSTOM_COLUMN_MAPPINGS]
            del kwargs[ARGUMENT_NAME_CUSTOM_COLUMN_MAPPINGS]
            if not (isinstance(custom_column_mappings, dict) and
                    isinstance(sheet_name := kwargs.get(ARGUMENT_NAME_SHEET_NAME, None), str) and
                    isinstance(custom_column_mappings :=
                               lookup_custom_column_mappings(custom_column_mappings, sheet_name), dict)):
                custom_column_mappings = None
            self._custom_column_mappings = custom_column_mappings
        super().__init__(*args, **kwargs)

    def _define_header(self, header: List[Optional[Any]]) -> None:

        def fixup_custom_column_mappings(custom_column_mappings: dict, actual_column_names: List[str]) -> dict:

            # This fixes up the custom column mappings config for this particular sheet based
            # on the actual (header) column names, i.e. e.g. in particular for the array
            # specifiers like mapping "qc_values#.value" to qc_values#0.value".

            def fixup_custom_array_column_mappings(custom_column_mappings: dict) -> None:

                def get_simple_array_column_name_component(column_name: str) -> Optional[str]:
                    if isinstance(column_name, str):
                        if column_name_components := column_name.split(COLUMN_NAME_SEPARATOR):
                            if (suffix := column_name_components[0].find(COLUMN_NAME_ARRAY_SUFFIX_CHAR)) > 0:
                                if (suffix + 1) == len(column_name_components[0]):
                                    return column_name_components[0][:suffix]
                    return None

                synthetic_array_column_names = {}
                for column_name in custom_column_mappings:
                    for synthetic_column_name in list(custom_column_mappings[column_name].keys()):
                        synthetic_array_column_name = get_simple_array_column_name_component(synthetic_column_name)
                        if synthetic_array_column_name:
                            if synthetic_array_column_name not in synthetic_array_column_names:
                                synthetic_array_column_names[synthetic_array_column_name] = \
                                    {"index": 0, "columns": [column_name]}
                            elif (column_name not in
                                  synthetic_array_column_names[synthetic_array_column_name]["columns"]):
                                synthetic_array_column_names[synthetic_array_column_name]["index"] += 1
                                synthetic_array_column_names[synthetic_array_column_name]["columns"].append(column_name)
                            synthetic_array_column_index = \
                                synthetic_array_column_names[synthetic_array_column_name]["index"]
                            synthetic_array_column_name = synthetic_column_name.replace(
                                f"{synthetic_array_column_name}#",
                                f"{synthetic_array_column_name}#{synthetic_array_column_index}")
                            custom_column_mappings[column_name][synthetic_array_column_name] = \
                                custom_column_mappings[column_name][synthetic_column_name]
                            del custom_column_mappings[column_name][synthetic_column_name]

            custom_column_mappings = deepcopy(custom_column_mappings)
            for custom_column_name in list(custom_column_mappings.keys()):
                if custom_column_name not in actual_column_names:
                    del custom_column_mappings[custom_column_name]
            fixup_custom_array_column_mappings(custom_column_mappings)
            return custom_column_mappings

        super()._define_header(header)
        if self._custom_column_mappings:
            self._custom_column_mappings = fixup_custom_column_mappings(self._custom_column_mappings, self.header)
            self._original_header = self.header
            self.header = []
            for column_name in header:
                if column_name in self._custom_column_mappings:
                    synthetic_column_names = list(self._custom_column_mappings[column_name].keys())
                    self.header += synthetic_column_names
                else:
                    self.header.append(column_name)

    def _iter_header(self) -> List[str]:
        if self._custom_column_mappings:
            return self._original_header
        return super()._iter_header()

    def _iter_mapper(self, row: dict) -> List[str]:
        if self._custom_column_mappings:
            synthetic_columns = {}
            columns_to_delete = []
            for column_name in row:
                if column_name in self._custom_column_mappings:
                    column_mapping = self._custom_column_mappings[column_name]
                    for synthetic_column_name in column_mapping:
                        synthetic_column_value = column_mapping[synthetic_column_name]
                        if synthetic_column_value == "{name}":
                            synthetic_columns[synthetic_column_name] = column_name
                        elif (column_value := self._parse_value_specifier(synthetic_column_value,
                                                                          row[column_name])) is not None:
                            synthetic_columns[synthetic_column_name] = column_value
                        else:
                            synthetic_columns[synthetic_column_name] = synthetic_column_value
                    columns_to_delete.append(column_name)
            if columns_to_delete:
                for column_to_delete in columns_to_delete:
                    del row[column_to_delete]
            if synthetic_columns:
                row.update(synthetic_columns)
        return row

    @staticmethod
    def _parse_value_specifier(value_specifier: Optional[Any], value: Optional[Any]) -> Optional[Any]:
        if value is not None:
            if isinstance(value_specifier, str) and (value_specifier := value_specifier.replace(" ", "")):
                if value_specifier.startswith("{value"):
                    if (value_specifier[len(value_specifier) - 1] == "}"):
                        if len(value_specifier) == 7:
                            return str(value)
                        if value_specifier[6] == ":":
                            if (value_specifier := value_specifier[7:-1]) in ["int", "integer"]:
                                return to_integer(value, fallback=value,
                                                  allow_commas=True, allow_multiplier_suffix=True)
                            elif value_specifier in ["float", "number"]:
                                return to_float(value, fallback=value,
                                                allow_commas=True, allow_multiplier_suffix=True)
                            elif value_specifier in ["bool", "boolean"]:
                                return to_boolean(value, fallback=value)
                        return str(value)
        return None


# This ExcelSheetName class is used to represent an Excel sheet name; it is simply a str type with an
# additional "original" property. The value of this will be given string with any prefix preceeding an
# underscore removed; and the "original" property will evaluate to the original/given string. This is
# used to support the use of sheet names of the form "XYZ_TypeName", where "XYZ" is an arbitrary string
# and "TypeName" is the virtual name of the sheet, which will be used by StructuredDataSet/etc, and which
# represents the (portal) type of (the items/rows within the) sheet. The purpose of all this is to allow
# multiple sheets within a spreadsheet of the same (portal object) type; since sheet names must be unique,
# this would otherwise not be possible; this provides a way for a spreadsheet to partition items/rows of
# a particular fixed type across multiple sheets.
#
# If this requirement was known at the beginning (or if we had more foresight) we would not support this
# feature this way; we would build it in from the start; this mechanism here merely provides a hook for
# this feature with minimal disruption (the only real tricky part being to make sure the original sheet
# name is reported in error messages); doing is this was minimizes risk of disruption.
#
class ExcelSheetName(str):
    def __new__(cls, value: str):
        value = value if isinstance(value, str) else str(value)
        original_value = value
        if ((delimiter := value.find("_")) > 0) and (delimiter < len(value) - 1):
            value = value[delimiter + 1:]
        instance = super().__new__(cls, value)
        setattr(instance, "original", original_value)
        return instance
