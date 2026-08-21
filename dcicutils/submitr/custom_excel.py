from copy import deepcopy
import io
import json
import os
import tempfile
from typing import Any, List, Optional
from dcicutils.data_readers import Excel, ExcelSheetReader
from dcicutils.misc_utils import to_boolean, to_float, to_integer
from dcicutils.submitr.donor_transformer import ProtectedDonorWorkbookTransformer

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


def _array_name_of(synthetic_column_name: str) -> Optional[str]:
    """Return the array name for a synthetic column key that carries an array placeholder,
    or None for a plain/scalar key. The COLUMN_NAME_ARRAY_SUFFIX_CHAR ("#") marks where a
    numeric array index will be inserted, e.g. "qc_values#.value" and the bare "qc_values#"
    both have array name "qc_values". This single definition is used by both the header
    registration (_define_header) and the per-row expansion (_iter_mapper) so that they
    always agree on which keys are array elements."""
    if isinstance(synthetic_column_name, str):
        prefix, separator, _ = synthetic_column_name.partition(COLUMN_NAME_ARRAY_SUFFIX_CHAR)
        if separator and prefix:
            return prefix
    return None


class CustomExcel(Excel):

    def __init__(self, *args, portal=None, transform_protected_donor: bool = False,
                 transformed_workbook_path: Optional[str] = None,
                 allow_existing_staging_path: bool = False, **kwargs):
        self._transform_protected_donor = bool(transform_protected_donor)
        self._transformed_workbook_path = transformed_workbook_path
        self._allow_existing_staging_path = bool(allow_existing_staging_path)
        super().__init__(*args, **kwargs)
        if self._transform_protected_donor:
            transformed = ProtectedDonorWorkbookTransformer(
                effective_sheet_name=self.effective_sheet_name
            ).transform(self._workbook, portal=portal)
            self.sheet_names = [sheet_name for sheet_name in self._workbook.sheetnames
                                if not self.is_hidden_sheet(self._workbook[sheet_name])]
            if transformed and self._transformed_workbook_path:
                self._save_transformed_workbook(
                    self._transformed_workbook_path,
                    allow_existing_staging_path=self._allow_existing_staging_path,
                )
        self._custom_column_mappings = CustomExcel._get_custom_column_mappings(portal=portal)

    @classmethod
    def with_portal(cls, portal, **options):
        """Return CustomExcel with the portal and worker options baked in.

        The submitr worker should pass ``transform_protected_donor=True`` when
        ProtectedDonor conversion is part of its upload flow.  This keeps the
        conversion opt-in for existing StructuredDataSet callers.  A submitr caller
        that pre-creates the transformed output may also pass
        ``allow_existing_staging_path=True``.
        """
        class _CustomExcelWithPortal(cls):
            def __init__(self, *args, **kwargs):
                kwargs.setdefault("portal", portal)
                for key, value in options.items():
                    kwargs.setdefault(key, value)
                super().__init__(*args, **kwargs)
        _CustomExcelWithPortal.__name__ = "CustomExcel"
        _CustomExcelWithPortal.__qualname__ = "CustomExcel"
        return _CustomExcelWithPortal

    def _save_transformed_workbook(self, path: str, overwrite: bool = False,
                                   allow_existing_staging_path: bool = False) -> None:
        """Save the transformed workbook without clobbering an existing file.

        ``overwrite`` is intentionally explicit and applies only to this call.  When it is
        true, the completed temporary workbook atomically replaces ``path``; this is for a
        caller that has already validated the transformed workbook.  The default is safe for
        ordinary conversion and leaves an existing target untouched.  The narrower
        ``allow_existing_staging_path`` option is for a path the caller pre-created as its
        owned temporary staging file; it also replaces that path atomically.
        """
        if overwrite and allow_existing_staging_path:
            raise ValueError("Choose either overwrite or allow_existing_staging_path, not both.")
        path = os.path.abspath(os.path.expanduser(path))
        input_path = os.path.abspath(os.path.expanduser(self._file)) if self._file else None
        if not path.lower().endswith(".xlsx"):
            raise ValueError(f"Transformed workbook output path must end with .xlsx: {path}")
        if input_path and path == input_path:
            raise ValueError(f"Transformed workbook output path must differ from input workbook path: {path}")
        directory = os.path.dirname(path)
        if not os.path.isdir(directory):
            raise ValueError(f"Directory for transformed workbook output does not exist: {directory}")
        if os.path.exists(path) and not (overwrite or allow_existing_staging_path):
            raise ValueError(f"Transformed workbook output path already exists: {path}")

        temporary_path = None
        try:
            temporary_fd, temporary_path = tempfile.mkstemp(
                dir=directory, prefix=f".{os.path.basename(path)}.", suffix=".xlsx"
            )
            os.close(temporary_fd)
            self._workbook.save(temporary_path)
            if not (overwrite or allow_existing_staging_path) and os.path.exists(path):
                raise ValueError(f"Transformed workbook output path already exists: {path}")
            os.replace(temporary_path, path)
            temporary_path = None
        except Exception as error:
            raise ValueError(f"Cannot save transformed workbook to {path}: {error}") from error
        finally:
            if temporary_path and os.path.exists(temporary_path):
                try:
                    os.unlink(temporary_path)
                except OSError:
                    pass

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

    @staticmethod
    def _expand_array_indices(column_mapping: dict, array_indices: dict) -> List[tuple]:
        # Assign concrete numeric array indices to a single source column's synthetic keys
        # using a shared per-array-name numbering scheme. This is the one place the index of
        # any synthetic array column is chosen; _define_header and _iter_mapper both call it
        # so that the registered header and the per-row output can never disagree.
        #
        # - array_indices maps array-name -> next index to assign; it is updated in place so
        #   that successive source columns referencing the same array-name get consecutive
        #   indices (qc_values#0, qc_values#1, ...).
        # - Within THIS source column every key that references a given array-name shares that
        #   column's single index for that array-name (they are fields of one array element);
        #   a column that references several array-names advances each of them once.
        # - Scalar (non-array) keys are returned unchanged.
        #
        # Returns a list of (concrete_synthetic_column_name, template_value) preserving the
        # mapping's key order.
        assigned = {}  # array-name -> index chosen for this source column
        expanded = []
        for synthetic_column_name, template_value in column_mapping.items():
            array_name = _array_name_of(synthetic_column_name)
            if array_name is None:
                expanded.append((synthetic_column_name, template_value))
                continue
            if array_name not in assigned:
                assigned[array_name] = array_indices.get(array_name, 0)
                array_indices[array_name] = assigned[array_name] + 1
            concrete_name = synthetic_column_name.replace(
                f"{array_name}{COLUMN_NAME_ARRAY_SUFFIX_CHAR}",
                f"{array_name}{COLUMN_NAME_ARRAY_SUFFIX_CHAR}{assigned[array_name]}", 1)
            expanded.append((concrete_name, template_value))
        return expanded

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
            # Register the *maximum* set of synthetic columns _iter_mapper could emit, using the
            # exact same per-array-name numbering (via _expand_array_indices). Every mapped source
            # column present in the sheet contributes one array element per array-name it uses, so
            # each array-name is registered contiguously as qc_values#0..qc_values#(N-1). At row
            # time empty cells are skipped, so the emitted indices are always a prefix of these;
            # every index a row can emit is therefore pre-registered here, which is required or
            # structured_data would silently drop the value (see _StructuredRowTemplate.set_value).
            self.header = []
            array_indices: dict = {}
            for column_name in header:
                if column_name in self._custom_column_mappings:
                    for concrete_name, _ in self._expand_array_indices(
                            self._custom_column_mappings[column_name], array_indices):
                        self.header.append(concrete_name)
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
            # Assign array indices with the SAME per-array-name scheme used to register the
            # header (via _expand_array_indices), but only for non-empty source cells, so each
            # array ends up a compact 0-based list with no gaps. Because empty columns are
            # skipped, the indices emitted here are always a prefix of those registered in
            # _define_header, guaranteeing every emitted synthetic column is pre-registered.
            array_indices: dict = {}
            for column_name in row:
                if column_name not in self._custom_column_mappings:
                    continue
                columns_to_delete.append(column_name)
                if not row[column_name]:
                    continue
                column_mapping = self._custom_column_mappings[column_name]
                for synthetic_column_name, synthetic_column_value in self._expand_array_indices(
                        column_mapping, array_indices):
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
