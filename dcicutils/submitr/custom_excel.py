from copy import deepcopy
import io
import json
import os
from typing import Any, List, Optional
from dcicutils.data_readers import Excel, ExcelSheetReader
from dcicutils.misc_utils import to_boolean, to_float, to_integer

# This module implements a custom Excel spreadsheet class which supports "custom column mappings",
# meaning that, at a very low/early level in processing, the columns/values in the spreadsheet
# can be redefined/remapped to different columns/values.
#
# The mapping config is fetched live from the portal via:
#
#   GET /search/?type=GenericQcConfig&tags=external_quality_metrics
#
# Each GenericQcConfig item returned carries the complete ready-to-use config in its "body"
# field, which already has the exact structure this module expects:
#
#   {
#     "sheet_mappings": {
#       "DuplexSeq_ExternalQualityMetric": "duplexseq_external_quality_metric",
#       "DSA_ExternalQualityMetric":       "dsa_external_quality_metric"
#     },
#     "column_mappings": {
#       "duplexseq_external_quality_metric": {
#         "total_raw_reads_sequenced": {
#           "qc_values#.derived_from": "{name}",
#           "qc_values#.value":        "{value:integer}",
#           "qc_values#.key":          "Total Raw Reads Sequenced",
#           "qc_values#.tooltip":      "# of reads (150bp)"
#         },
#         ...
#       },
#       ...
#     }
#   }
#
# If multiple GenericQcConfig items are returned the one with the highest "version"
# (integer-parsed) is used.  If the portal query fails or returns nothing, the bundled
# local JSON file (config/custom_column_mappings.json) is used as a fallback.
#
# The mapping can be thought of as a virtual preprocessing step on the spreadsheet.
# For EXAMPLE, so the spreadsheet author can specify single columns like this:
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
# The hook for this is to pass the CustomExcel type to StructuredDataSet in submission.py.
#
# ALSO ...
# This CustomExcel class also handles multiple sheets within a spreadsheet representing
# the same (portal) type; see comments below near the ExcelSheetName class definition.

CUSTOM_COLUMN_MAPPINGS_LOCAL_CONFIG = os.path.join(
    os.path.dirname(__file__), "config", "custom_column_mappings.json"
)

COLUMN_NAME_ARRAY_SUFFIX_CHAR = "#"
COLUMN_NAME_SEPARATOR = "."

# The portal search used to retrieve EQM column-mapping configs.
GENERIC_QC_CONFIG_SEARCH = "search/?type=GenericQcConfig&tags=external_quality_metrics"


def _get_most_recent_config_version(items: list) -> Optional[dict]:
    """Return the GenericQcConfig item with the highest integer version number."""
    def parse_version(item):
        try:
            return int(item.get("version", 0))
        except (ValueError, TypeError):
            return 0
    return max(items, key=parse_version, default=None)


class CustomExcel(Excel):

    def __init__(self, *args, portal=None, **kwargs):
        super().__init__(*args, **kwargs)
        self._custom_column_mappings = CustomExcel._get_custom_column_mappings(portal=portal)

    @classmethod
    def with_portal(cls, portal):
        """Return a subclass of CustomExcel with portal baked in.

        Use this when passing excel_class to StructuredDataSet, which requires
        a real class (it calls issubclass() on the argument internally):

            excel_class=CustomExcel.with_portal(portal)
        """
        class _CustomExcelWithPortal(cls):
            def __init__(self, *args, **kwargs):
                kwargs.setdefault("portal", portal)
                super().__init__(*args, **kwargs)
        _CustomExcelWithPortal.__name__ = "CustomExcel"
        _CustomExcelWithPortal.__qualname__ = "CustomExcel"
        return _CustomExcelWithPortal

    def sheet_reader(self, sheet_name: str) -> ExcelSheetReader:
        return CustomExcelSheetReader(self, sheet_name=sheet_name, workbook=self._workbook,
                                      custom_column_mappings=self._custom_column_mappings)

    @staticmethod
    def effective_sheet_name(sheet_name: str) -> str:
        if (underscore := sheet_name.find("_")) > 1:
            return sheet_name[underscore + 1:]
        return sheet_name

    @staticmethod
    def _get_custom_column_mappings(portal=None) -> Optional[dict]:

        def fetch_from_portal(portal) -> Optional[dict]:
            """Query the portal for GenericQcConfig items. The most recent item's
            "body" field is the complete ready-to-use config dict."""
            if portal is None:
                return None
            try:
                results = portal.get_metadata(GENERIC_QC_CONFIG_SEARCH)
                if not isinstance(results, dict):
                    return None
                items = results.get("@graph", [])
                if not items:
                    return None
                item = _get_most_recent_config_version(items)
                if item is None:
                    return None
                body = item.get("body")
                if not isinstance(body, dict):
                    return None
                return body
            except Exception:
                return None

        def fetch_from_local_json() -> Optional[dict]:
            """Fall back to the bundled static JSON config."""
            try:
                with io.open(CUSTOM_COLUMN_MAPPINGS_LOCAL_CONFIG, "r") as f:
                    return json.load(f)
            except Exception:
                return None

        def post_process(raw_config: dict) -> Optional[dict]:
            """Resolve sheet_mappings string references into the actual column-mapping dicts."""
            if not isinstance(raw_config, dict):
                return None
            column_mappings = raw_config.get("column_mappings")
            sheet_mappings = raw_config.get("sheet_mappings")
            if not isinstance(column_mappings, dict) or not isinstance(sheet_mappings, dict):
                return None
            for sheet_name in list(sheet_mappings.keys()):
                mapping_key = sheet_mappings[sheet_name]
                if isinstance(mapping_key, str):
                    resolved = column_mappings.get(mapping_key)
                    if isinstance(resolved, dict):
                        sheet_mappings[sheet_name] = resolved
                    else:
                        del sheet_mappings[sheet_name]
                elif not isinstance(mapping_key, dict):
                    del sheet_mappings[sheet_name]
            return sheet_mappings if sheet_mappings else None

        raw_config = fetch_from_portal(portal) or fetch_from_local_json()
        if not raw_config:
            return None
        return post_process(raw_config)


class CustomExcelSheetReader(ExcelSheetReader):

    def __init__(self, *args, **kwargs) -> None:
        ARGUMENT_NAME_SHEET_NAME = "sheet_name"
        ARGUMENT_NAME_CUSTOM_COLUMN_MAPPINGS = "custom_column_mappings"
        self._custom_column_mappings = None
        if ARGUMENT_NAME_CUSTOM_COLUMN_MAPPINGS in kwargs:
            def lookup_custom_column_mappings(custom_column_mappings: dict, sheet_name: str) -> Optional[dict]:
                if isinstance(custom_column_mappings, dict) and isinstance(sheet_name, str):
                    if isinstance(found := custom_column_mappings.get(sheet_name), dict):
                        return found
                    if (effective := CustomExcel.effective_sheet_name(sheet_name)) != sheet_name:
                        if isinstance(found := custom_column_mappings.get(effective), dict):
                            return found
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
            # Array indices (the N in qc_values#N.key) are intentionally left unassigned here.
            # They are assigned dynamically per-row in _iter_mapper so that only non-empty
            # columns receive an index, producing a compact 0-based array with no gaps.
            custom_column_mappings = deepcopy(custom_column_mappings)
            for custom_column_name in list(custom_column_mappings.keys()):
                if custom_column_name not in actual_column_names:
                    del custom_column_mappings[custom_column_name]
            return custom_column_mappings

        super()._define_header(header)
        if self._custom_column_mappings:
            self._custom_column_mappings = fixup_custom_column_mappings(self._custom_column_mappings, self.header)
            self._original_header = self.header
            # Build an expanded header that _StructuredRowTemplate will use to register
            # set_value functions.  Each source column that has a mapping contributes N
            # synthetic entries (one per mapped column in the sheet), with consecutive
            # numeric indices.  This is the *maximum* possible index range; _iter_mapper
            # will only populate the slots that correspond to non-empty source values, so
            # the actual array in any given row will be shorter — but every index it might
            # emit must be pre-registered here or structured_data silently drops the value.
            self.header = []
            index = 0
            for column_name in header:
                if column_name in self._custom_column_mappings:
                    for synthetic_key in self._custom_column_mappings[column_name]:
                        array_name, _, rest = synthetic_key.partition(COLUMN_NAME_ARRAY_SUFFIX_CHAR)
                        if rest:  # bare placeholder: "qc_values#.key" -> "qc_values#<index>.key"
                            self.header.append(
                                f"{array_name}{COLUMN_NAME_ARRAY_SUFFIX_CHAR}{index}{COLUMN_NAME_SEPARATOR}{rest.lstrip(COLUMN_NAME_SEPARATOR)}")
                        else:
                            self.header.append(synthetic_key)
                    index += 1
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
            # Track per-array-name indices so each non-empty column gets the next
            # available slot (e.g. qc_values#0, qc_values#1, ...).  Indices are
            # assigned here at row-processing time rather than statically at header
            # time so that empty columns are simply skipped and leave no gap.
            array_indices: dict = {}
            for column_name in row:
                if column_name not in self._custom_column_mappings:
                    continue
                columns_to_delete.append(column_name)
                if not row[column_name]:
                    continue
                column_mapping = self._custom_column_mappings[column_name]
                # Determine the array name (e.g. "qc_values") used by this mapping
                # group and assign it the next sequential index for this row.
                array_name = None
                for synthetic_column_name in column_mapping:
                    prefix, _, _ = synthetic_column_name.partition(COLUMN_NAME_ARRAY_SUFFIX_CHAR)
                    if _ and prefix:
                        array_name = prefix
                        break
                if array_name is not None:
                    index = array_indices.get(array_name, 0)
                    array_indices[array_name] = index + 1
                for synthetic_column_name, synthetic_column_value in column_mapping.items():
                    # Replace bare "array_name#" placeholder with the assigned index.
                    if array_name is not None:
                        synthetic_column_name = synthetic_column_name.replace(
                            f"{array_name}{COLUMN_NAME_ARRAY_SUFFIX_CHAR}",
                            f"{array_name}{COLUMN_NAME_ARRAY_SUFFIX_CHAR}{index}", 1)
                    if synthetic_column_value == "{name}":
                        synthetic_columns[synthetic_column_name] = column_name
                    elif (column_value := self._parse_value_specifier(synthetic_column_value,
                                                                      row[column_name])) is not None:
                        synthetic_columns[synthetic_column_name] = column_value
                    else:
                        synthetic_columns[synthetic_column_name] = synthetic_column_value
            for column_to_delete in columns_to_delete:
                del row[column_to_delete]
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
# additional "original" property. The value of this will be given string with any prefix preceding an
# underscore removed; and the "original" property will evaluate to the original/given string. This is
# used to support the use of sheet names of the form "XYZ_TypeName", where "XYZ" is an arbitrary string
# and "TypeName" is the virtual name of the sheet, which will be used by StructuredDataSet/etc, and which
# represents the (portal) type of (the items/rows within the) sheet. The purpose of all this is to allow
# multiple sheets within a spreadsheet of the same (portal object) type; since sheet names must be unique,
# this would otherwise not be possible; this provides a way for a spreadsheet to partition items/rows of
# a particular fixed type across multiple sheets.
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
