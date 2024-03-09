import copy
from functools import lru_cache
import json
from jsonschema import Draft7Validator as SchemaValidator
import os
from pyramid.router import Router
import re
import sys
from tqdm import tqdm
from typing import Any, Callable, List, Optional, Tuple, Type, Union
from webtest.app import TestApp
from dcicutils.common import OrchestratedApp
from dcicutils.data_readers import CsvReader, Excel, RowReader
from dcicutils.datetime_utils import normalize_date_string, normalize_datetime_string
from dcicutils.file_utils import search_for_file
from dcicutils.misc_utils import (create_dict, create_readonly_object, load_json_if,
                                  merge_objects, remove_empty_properties, right_trim,
                                  split_string, to_boolean, to_enum, to_float, to_integer, VirtualApp)
from dcicutils.portal_object_utils import PortalObject
from dcicutils.portal_utils import Portal as PortalBase
from dcicutils.schema_utils import Schema as SchemaBase
from dcicutils.zip_utils import unpack_gz_file_to_temporary_file, unpack_files


# Classes/functions to parse a CSV or Excel Spreadsheet into structured data, using a specialized
# syntax to allow structured object properties to be referenced by column specifiers. This syntax
# uses an (intuitive) dot notation to reference nested objects, and a (less intuitive) notation
# utilizing the "#" character to reference array elements. May also further coerce data types by
# consulting an optionally specified JSON schema.
#
# Alternate and semantically equivalent implementation of dcicutils.{sheet,bundle}_utils.
# Spare time exercise, with benefit of sheet_utils implementation experience.

ACCEPTABLE_FILE_SUFFIXES = [".csv", ".tsv", ".json", ".xls", ".xlsx", ".gz", ".tar", ".tar.gz", ".tgz", ".zip"]
ARRAY_VALUE_DELIMITER_CHAR = "|"
ARRAY_VALUE_DELIMITER_ESCAPE_CHAR = "\\"
ARRAY_NAME_SUFFIX_CHAR = "#"
ARRAY_NAME_SUFFIX_REGEX = re.compile(rf"{ARRAY_NAME_SUFFIX_CHAR}\d+")
DOTTED_NAME_DELIMITER_CHAR = "."
FILE_SCHEMA_NAME = "File"
FILE_SCHEMA_NAME_PROPERTY = "filename"

# Forward type references for type hints.
Portal = Type["Portal"]
Schema = Type["Schema"]
StructuredDataSet = Type["StructuredDataSet"]


class StructuredDataSet:

    # Reference (linkTo) lookup strategies; on a per-reference (type/value) basis;
    # controlled by optional ref_lookup_strategy callable; default is lookup at root path
    # but after the named reference (linkTo) type path lookup, and then lookup all subtypes;
    # can choose to lookup root path first, or not lookup root path at all, or not lookup
    # subtypes at all; the ref_lookup_strategy callable if specified should take a type_name
    # and value (string) arguements and return an integer of any of the below ORed together.
    # The main purpose of this is optimization; to minimize portal lookups; since for example,
    # currently at least, /{type}/{accession} does not work but /{accession} does; so we
    # currently (smaht-portal/.../ingestion_processors) use REF_LOOKUP_ROOT_FIRST for this.
    # And current usage NEVER has REF_LOOKUP_SUBTYPES turned OFF; but support just in case.
    REF_LOOKUP_SPECIFIED_TYPE = 0x0001
    REF_LOOKUP_ROOT = 0x0002
    REF_LOOKUP_ROOT_FIRST = 0x0004 | REF_LOOKUP_ROOT
    REF_LOOKUP_SUBTYPES = 0x0008
    REF_LOOKUP_DEFAULT = REF_LOOKUP_SPECIFIED_TYPE | REF_LOOKUP_ROOT | REF_LOOKUP_SUBTYPES

    def __init__(self, file: Optional[str] = None, portal: Optional[Union[VirtualApp, TestApp, Portal]] = None,
                 schemas: Optional[List[dict]] = None, autoadd: Optional[dict] = None,
                 order: Optional[List[str]] = None, prune: bool = True,
                 ref_lookup_strategy: Optional[Callable] = None,
                 ref_lookup_nocache: bool = False,
                 progress: bool = False) -> None:
        progress = False
        self._progress = progress
        self._data = {}
        self._portal = Portal(portal, data=self._data, schemas=schemas,
                              ref_lookup_strategy=ref_lookup_strategy,
                              ref_lookup_nocache=ref_lookup_nocache) if portal else None
        self._order = order
        self._prune = prune
        self._warnings = {}
        self._errors = {}
        self._resolved_refs = set()
        self._validated = False
        self._autoadd_properties = autoadd if isinstance(autoadd, dict) and autoadd else None
        self._load_file(file) if file else None

    def _progress_add(self, amount: Union[int, Callable]) -> None:
        if self._progress is not False and self._progress is not None:
            if callable(amount):
                amount = amount()
            if not isinstance(amount, int):
                return
            if self._progress is True:
                if amount > 0:
                    self._progress = tqdm(total=amount)
            elif isinstance(self._progress, tqdm):
                if amount > 0:
                    self._progress.total += amount
                elif amount < 0:
                    self._progress.update(-amount)

    @property
    def data(self) -> dict:
        return self._data

    @property
    def portal(self) -> Optional[Portal]:
        return self._portal

    @staticmethod
    def load(file: str, portal: Optional[Union[VirtualApp, TestApp, Portal]] = None,
             schemas: Optional[List[dict]] = None, autoadd: Optional[dict] = None,
             order: Optional[List[str]] = None, prune: bool = True,
             ref_lookup_strategy: Optional[Callable] = None,
             ref_lookup_nocache: bool = False) -> StructuredDataSet:
        return StructuredDataSet(file=file, portal=portal, schemas=schemas, autoadd=autoadd, order=order, prune=prune,
                                 ref_lookup_strategy=ref_lookup_strategy, ref_lookup_nocache=ref_lookup_nocache)

    def validate(self, force: bool = False) -> None:
        def data_without_deleted_properties(data: dict) -> dict:
            nonlocal self
            def isempty(value: Any) -> bool:  # noqa
                if value == RowReader.CELL_DELETION_SENTINEL:
                    return True
                return self._prune and value in [None, "", {}, []]
            def isempty_array_element(value: Any) -> bool:  # noqa
                return value == RowReader.CELL_DELETION_SENTINEL
            data = copy.deepcopy(data)
            remove_empty_properties(data, isempty=isempty, isempty_array_element=isempty_array_element)
            return data
        if self._validated and not force:
            return
        self._validated = True
        for type_name in self.data:
            if (schema := Schema.load_by_name(type_name, portal=self._portal)):
                row_number = 0
                for data in self.data[type_name]:
                    data = data_without_deleted_properties(data)
                    row_number += 1
                    if (validation_errors := schema.validate(data)) is not None:
                        for validation_error in validation_errors:
                            self._note_error({"src": create_dict(type=schema.type, row=row_number),
                                              "error": validation_error}, "validation")

    @property
    def warnings(self) -> dict:
        return self._warnings

    @property
    def reader_warnings(self) -> List[dict]:
        return self._warnings.get("reader") or []

    @property
    def errors(self) -> dict:
        return self._errors

    @property
    def ref_errors(self) -> List[dict]:
        return self._errors.get("ref") or []

    @property
    def validation_errors(self) -> List[dict]:
        return self._errors.get("validation") or []

    @property
    def resolved_refs(self) -> List[str]:
        return list([resolved_ref[0] for resolved_ref in self._resolved_refs])

    @property
    def resolved_refs_with_uuids(self) -> List[str]:
        return list([{"path": resolved_ref[0], "uuid": resolved_ref[1]} for resolved_ref in self._resolved_refs])

    @property
    def upload_files(self) -> List[str]:
        result = []
        if self._portal:
            for type_name in self.data:
                if self._portal.is_file_schema(type_name):
                    for item in self.data[type_name]:
                        if (file_name := item.get(FILE_SCHEMA_NAME_PROPERTY)):
                            result.append({"type": type_name, "file": file_name})
        return result

    def upload_files_located(self,
                             location: Union[str, Optional[List[str]]] = None, recursive: bool = False) -> List[str]:
        upload_files = copy.deepcopy(self.upload_files)
        for upload_file in upload_files:
            if file_path := search_for_file(upload_file["file"], location, recursive=recursive, single=True):
                upload_file["path"] = file_path
        return upload_files

    def compare(self) -> dict:
        diffs = {}
        if self.data or self.portal:
            refs = self.resolved_refs_with_uuids
            for object_type in self.data:
                if not diffs.get(object_type):
                    diffs[object_type] = []
                for portal_object in self.data[object_type]:
                    portal_object = PortalObject(portal_object, portal=self.portal, type=object_type)
                    existing_object, identifying_path = portal_object.lookup(include_identifying_path=True, raw=True)
                    if existing_object:
                        object_diffs = portal_object.compare(existing_object, consider_refs=True, resolved_refs=refs)
                        diffs[object_type].append(create_readonly_object(path=identifying_path,
                                                                         uuid=existing_object.uuid,
                                                                         diffs=object_diffs or None))
                    else:
                        # If there is no existing object we still create a record for this object
                        # but with no uuid which will be the indication that it does not exist.
                        diffs[object_type].append(create_readonly_object(path=identifying_path, uuid=None, diffs=None))
        return diffs

    def _load_file(self, file: str) -> None:
        # Returns a dictionary where each property is the name (i.e. the type) of the data,
        # and the value is array of dictionaries for the data itself. Handle these kinds of files:
        # 1.  Single CSV, TSV, or JSON file, where the (base) name of the file is the data type name.
        # 2.  Single Excel file containing one or more sheets, where each sheet
        #     represents (i.e. is named for, and contains data for) a different type.
        # 3.  Zip file (.zip or .tar.gz or .tgz or .tar), containing data files to load, where the
        #     base name of each contained file is the data type name; or any of above gzipped (.gz).
        if file.endswith(".gz") or file.endswith(".tgz"):
            with unpack_gz_file_to_temporary_file(file) as file:
                return self._load_normal_file(file)
        return self._load_normal_file(file)

    def _load_normal_file(self, file: str) -> None:
        if file.endswith(".csv") or file.endswith(".tsv"):
            self._load_csv_file(file)
        elif file.endswith(".xls") or file.endswith(".xlsx"):
            self._load_excel_file(file)
        elif file.endswith(".json"):
            self._load_json_file(file)
        elif file.endswith(".tar") or file.endswith(".zip"):
            self._load_packed_file(file)

    def _load_packed_file(self, file: str) -> None:
        for file in unpack_files(file, suffixes=ACCEPTABLE_FILE_SUFFIXES):
            self._load_file(file)

    def _load_csv_file(self, file: str) -> None:
        self._load_reader(CsvReader(file), type_name=Schema.type_name(file))

    def _load_excel_file(self, file: str) -> None:
        def calculate_total_rows_to_process():
            nonlocal file
            excel = Excel(file)
            nrows = 0
            for sheet_name in excel.sheet_names:
                for row in excel.sheet_reader(sheet_name):
                    nrows += 1
            return nrows
        self._progress_add(calculate_total_rows_to_process)
        excel = Excel(file)  # Order the sheet names by any specified ordering (e.g. ala snovault.loadxl).
        order = {Schema.type_name(key): index for index, key in enumerate(self._order)} if self._order else {}
        for sheet_name in sorted(excel.sheet_names, key=lambda key: order.get(Schema.type_name(key), sys.maxsize)):
            self._load_reader(excel.sheet_reader(sheet_name), type_name=Schema.type_name(sheet_name))
        # Check for unresolved reference errors which really are not because of ordering.
        # Yes such internal references will be handled correctly on actual database update via snovault.loadxl.
        if ref_errors := self.ref_errors:
            ref_errors_actual = []
            for ref_error in ref_errors:
                if not (resolved := self.portal.ref_exists(ref := ref_error["error"])):
                    ref_errors_actual.append(ref_error)
                else:
                    self._resolved_refs.add((ref, resolved[0].get("uuid")))
            if ref_errors_actual:
                self._errors["ref"] = ref_errors_actual
            else:
                del self._errors["ref"]

    def _load_json_file(self, file: str) -> None:
        with open(file) as f:
            self._add(Schema.type_name(file), json.load(f))

    def _load_reader(self, reader: RowReader, type_name: str) -> None:
        schema = None
        noschema = False
        structured_row_template = None
        for row in reader:
            if not structured_row_template:  # Delay creation just so we don't reference schema if there are no rows.
                if not schema and not noschema and not (schema := Schema.load_by_name(type_name, portal=self._portal)):
                    noschema = True
                elif schema and (schema_name := schema.type):
                    type_name = schema_name
                structured_row_template = _StructuredRowTemplate(reader.header, schema)
            structured_row = structured_row_template.create_row()
            for column_name, value in row.items():
                structured_row_template.set_value(structured_row, column_name, value, reader.file, reader.row_number)
                if self._autoadd_properties:
                    self._add_properties(structured_row, self._autoadd_properties, schema)
            self._add(type_name, structured_row)
            self._progress_add(-1)
        self._note_warning(reader.warnings, "reader")
        if schema:
            self._note_error(schema._unresolved_refs, "ref")
            self._resolved_refs.update(schema._resolved_refs)

    def _add(self, type_name: str, data: Union[dict, List[dict]]) -> None:
        if self._prune:
            remove_empty_properties(data)
        if type_name in self._data:
            self._data[type_name].extend([data] if isinstance(data, dict) else data)
        else:
            self._data[type_name] = [data] if isinstance(data, dict) else data

    def _add_properties(self, structured_row: dict, properties: dict, schema: Optional[dict] = None) -> None:
        for name in properties:
            if name not in structured_row and (not schema or schema.data.get("properties", {}).get(name)):
                structured_row[name] = properties[name]

    def _is_ref_lookup_specified_type(ref_lookup_flags: int) -> bool:
        return (ref_lookup_flags &
                StructuredDataSet.REF_LOOKUP_SPECIFIED_TYPE) == StructuredDataSet.REF_LOOKUP_SPECIFIED_TYPE

    def _is_ref_lookup_root(ref_lookup_flags: int) -> bool:
        return (ref_lookup_flags & StructuredDataSet.REF_LOOKUP_ROOT) == StructuredDataSet.REF_LOOKUP_ROOT

    def _is_ref_lookup_root_first(ref_lookup_flags: int) -> bool:
        return (ref_lookup_flags & StructuredDataSet.REF_LOOKUP_ROOT_FIRST) == StructuredDataSet.REF_LOOKUP_ROOT_FIRST

    def _is_ref_lookup_subtypes(ref_lookup_flags: int) -> bool:
        return (ref_lookup_flags & StructuredDataSet.REF_LOOKUP_SUBTYPES) == StructuredDataSet.REF_LOOKUP_SUBTYPES

    @property
    def ref_lookup_cache_hit_count(self) -> int:
        return self.portal.ref_lookup_cache_hit_count if self.portal else -1

    @property
    def ref_lookup_cache_miss_count(self) -> int:
        return self.portal.ref_lookup_cache_miss_count if self.portal else -1

    @property
    def ref_lookup_count(self) -> int:
        return self.portal.ref_lookup_count if self.portal else -1

    @property
    def ref_lookup_found_count(self) -> int:
        return self.portal.ref_lookup_found_count if self.portal else -1

    @property
    def ref_lookup_notfound_count(self) -> int:
        return self.portal.ref_lookup_notfound_count if self.portal else -1

    @property
    def ref_lookup_error_count(self) -> int:
        return self.portal.ref_lookup_error_count if self.portal else -1

    @property
    def ref_exists_internal_count(self) -> int:
        return self.portal.ref_exists_internal_count if self.portal else -1

    @property
    def ref_exists_cache_hit_count(self) -> int:
        return self.portal.ref_exists_cache_hit_count if self.portal else -1

    @property
    def ref_exists_cache_miss_count(self) -> int:
        return self.portal.ref_exists_cache_miss_count if self.portal else -1

    @property
    def ref_incorrect_identifying_property_count(self) -> int:
        return self.portal.ref_incorrect_identifying_property_count if self.portal else -1

    def _note_warning(self, item: Optional[Union[dict, List[dict]]], group: str) -> None:
        self._note_issue(self._warnings, item, group)

    def _note_error(self, item: Optional[Union[dict, List[dict]]], group: str) -> None:
        self._note_issue(self._errors, item, group)

    def _note_issue(self, issues: dict, item: Optional[Union[dict, List[dict]]], group: str) -> None:
        if isinstance(item, dict) and item:
            item = [item]
        if isinstance(item, list) and item:
            if not issues.get(group):
                issues[group] = []
            issues[group].extend(item)


class _StructuredRowTemplate:

    def __init__(self, column_names: List[str], schema: Optional[Schema] = None) -> None:
        self._schema = schema
        self._set_value_functions = {}
        self._template = self._create_row_template(column_names)

    def create_row(self) -> dict:
        return copy.deepcopy(self._template)

    def set_value(self, data: dict, column_name: str, value: str, file: Optional[str], row_number: int = -1) -> None:
        if (set_value_function := self._set_value_functions.get(column_name)):
            src = create_dict(type=self._schema.type if self._schema else None,
                              column=column_name, file=file, row=row_number)
            set_value_function(data, value, src)

    def _create_row_template(self, column_names: List[str]) -> dict:  # Surprisingly tricky code here.

        def parse_array_components(column_name: str, value: Optional[Any],
                                   path: List[Union[str, int]]) -> Tuple[Optional[str], Optional[List[Any]]]:
            array_name, array_indices = Schema.array_indices(column_name)
            if not array_name:
                return None, None
            array = None
            for array_index in array_indices[::-1]:  # Reverse iteration from the last/inner-most index to first.
                if not (array is None and value is None):
                    array_index = max(array_index, 0)
                path.insert(0, array_index)
                array_length = array_index + 1
                if array is None:
                    if value is None:
                        array = [None for _ in range(array_length)]
                    else:
                        array = [copy.deepcopy(value) for _ in range(array_length)]
                else:
                    array = [copy.deepcopy(array) for _ in range(array_length)]
            return array_name, array

        def parse_components(column_components: List[str], path: List[Union[str, int]]) -> dict:
            value = parse_components(column_components[1:], path) if len(column_components) > 1 else None
            array_name, array = parse_array_components(column_component := column_components[0], value, path)
            path.insert(0, array_name or column_component)
            return {array_name: array} if array_name else {column_component: value}

        def set_value_internal(data: Union[dict, list], value: Optional[Any], src: Optional[str],
                               path: List[Union[str, int]], typeinfo: Optional[dict], mapv: Optional[Callable]) -> None:

            def set_value_backtrack_object(path_index: int, path_element: str) -> None:
                nonlocal data, path, original_data
                backtrack_data = original_data
                for j in range(path_index - 1):
                    if not isinstance(path[j], str):
                        return
                    backtrack_data = backtrack_data[path[j]]
                data = backtrack_data[path[path_index - 1]] = {path_element: None}

            original_data = data
            json_value = None
            if isinstance(path[-1], int) and (json_value := load_json_if(value, is_array=True)):
                path = right_trim(path, remove=lambda value: isinstance(value, int))
            for i, p in enumerate(path[:-1]):
                if isinstance(p, str) and (not isinstance(data, dict) or p not in data):
                    set_value_backtrack_object(i, p)
                data = data[p]
            if (p := path[-1]) == -1 and isinstance(value, str):
                values = _split_array_string(value, unique=typeinfo.get("unique") if typeinfo else False)
                if mapv:
                    values = [mapv(value, src) for value in values]
                merge_objects(data, values)
            else:
                if json_value or (json_value := load_json_if(value, is_array=True, is_object=True)):
                    data[p] = json_value
                else:
                    if isinstance(p, str) and (not isinstance(data, dict) or p not in data):
                        set_value_backtrack_object(i + 1, p)
                    data[p] = mapv(value, src) if mapv else value

        def ensure_column_consistency(column_name: str) -> None:
            column_components = _split_dotted_string(Schema.normalize_column_name(column_name))
            for existing_column_name in self._set_value_functions:
                existing_column_components = _split_dotted_string(Schema.normalize_column_name(existing_column_name))
                if (Schema.unadorn_column_name(column_components[0]) !=
                    Schema.unadorn_column_name(existing_column_components[0])):  # noqa
                    break
                for i in range(min(len(column_components), len(existing_column_components))):
                    if i >= len(column_components) or i >= len(existing_column_components):
                        break
                    if ((column_components[i] != existing_column_components[i]) and
                        (column_components[i].endswith(ARRAY_NAME_SUFFIX_CHAR) or
                         existing_column_components[i].endswith(ARRAY_NAME_SUFFIX_CHAR))):
                        raise Exception(f"Inconsistent columns: {column_components[i]} {existing_column_components[i]}")

        structured_row_template = {}
        for column_name in column_names or []:
            ensure_column_consistency(column_name)
            rational_column_name = self._schema.rationalize_column_name(column_name) if self._schema else column_name
            column_typeinfo = self._schema.get_typeinfo(rational_column_name) if self._schema else None
            map_value_function = column_typeinfo.get("map") if column_typeinfo else None
            if (column_components := _split_dotted_string(rational_column_name)):
                merge_objects(structured_row_template, parse_components(column_components, path := []), True)
                self._set_value_functions[column_name] = (
                    lambda data, value, src, path=path, typeinfo=column_typeinfo, mapv=map_value_function:
                        set_value_internal(data, value, src, path, typeinfo, mapv))
        return structured_row_template


class Schema(SchemaBase):

    def __init__(self, schema_json: dict, portal: Optional[Portal] = None) -> None:
        super().__init__(schema_json)
        self._portal = portal  # Needed only to resolve linkTo references.
        self._map_value_functions = {
            "boolean": self._map_function_boolean,
            "enum": self._map_function_enum,
            "integer": self._map_function_integer,
            "number": self._map_function_number,
            "string": self._map_function_string,
            "date": self._map_function_date,
            "datetime": self._map_function_datetime
        }
        self._resolved_refs = set()
        self._unresolved_refs = []
        self._typeinfo = self._create_typeinfo(schema_json)

    @staticmethod
    def load_by_name(name: str, portal: Portal) -> Optional[dict]:
        schema_json = portal.get_schema(Schema.type_name(name)) if portal else None
        return Schema(schema_json, portal) if schema_json else None

    def validate(self, data: dict) -> List[str]:
        errors = []
        for error in SchemaValidator(self.data, format_checker=SchemaValidator.FORMAT_CHECKER).iter_errors(data):
            errors.append(f"Validation error at '{error.json_path}': {error.message}")
        return errors

    def get_typeinfo(self, column_name: str) -> Optional[dict]:
        if isinstance(info := self._typeinfo.get(column_name), str):
            info = self._typeinfo.get(info)
        if not info and isinstance(info := self._typeinfo.get(self.unadorn_column_name(column_name)), str):
            info = self._typeinfo.get(info)
        return info

    def _map_function(self, typeinfo: dict) -> Optional[Callable]:
        if isinstance(typeinfo, dict) and (typeinfo_type := typeinfo.get("type")) is not None:
            if isinstance(typeinfo_type, list):
                # The type specifier can actually be a list of acceptable types; for
                # example smaht-portal/schemas/mixins.json/meta_workflow_input#.value;
                # we will take the first one for which we have a mapping function.
                # TODO: Maybe more correct to get all map function and map to any for values.
                for acceptable_type in typeinfo_type:
                    if (map_function := self._map_value_functions.get(acceptable_type)) is not None:
                        break
            elif not isinstance(typeinfo_type, str):
                return None  # Invalid type specifier; ignore,
            elif isinstance(typeinfo.get("enum"), list):
                map_function = self._map_function_enum
            elif isinstance(typeinfo.get("linkTo"), str):
                map_function = self._map_function_ref
            elif (type_format := typeinfo.get("format")) == "date":
                map_function = self._map_function_date
            elif type_format == "date-time":
                map_function = self._map_function_datetime
            else:
                map_function = self._map_value_functions.get(typeinfo_type)
            return map_function(typeinfo) if map_function else None
        return None

    def _map_function_boolean(self, typeinfo: dict) -> Callable:
        def map_boolean(value: str, src: Optional[str]) -> Any:
            return to_boolean(value, value)
        return map_boolean

    def _map_function_enum(self, typeinfo: dict) -> Callable:
        def map_enum(value: str, enum_specifiers: dict, src: Optional[str]) -> Any:
            return to_enum(value, enum_specifiers)
        return lambda value, src: map_enum(value, typeinfo.get("enum", []), src)

    def _map_function_integer(self, typeinfo: dict) -> Callable:
        def map_integer(value: str, src: Optional[str]) -> Any:
            return to_integer(value, value)
        return map_integer

    def _map_function_number(self, typeinfo: dict) -> Callable:
        def map_number(value: str, src: Optional[str]) -> Any:
            return to_float(value, value)
        return map_number

    def _map_function_string(self, typeinfo: dict) -> Callable:
        def map_string(value: str, src: Optional[str]) -> str:
            return value if value is not None else ""
        return map_string

    def _map_function_date(self, typeinfo: dict) -> Callable:
        def map_date(value: str, src: Optional[str]) -> str:
            value = normalize_date_string(value)
            return value if value is not None else ""
        return map_date

    def _map_function_datetime(self, typeinfo: dict) -> Callable:
        def map_datetime(value: str, src: Optional[str]) -> str:
            value = normalize_datetime_string(value)
            return value if value is not None else ""
        return map_datetime

    def _map_function_ref(self, typeinfo: dict) -> Callable:
        def map_ref(value: str, link_to: str, portal: Optional[Portal], src: Optional[str]) -> Any:
            nonlocal self, typeinfo
            if not value:
                if (column := typeinfo.get("column")) and column in self.data.get("required", []):
                    # TODO: If think we do have the column (and type?) name(s) originating the ref yes?
                    self._unresolved_refs.append({"src": src, "error": f"/{link_to}/<null>"})
            elif portal:
                if not (resolved := portal.ref_exists(link_to, value)):
                    self._unresolved_refs.append({"src": src, "error": f"/{link_to}/{value}"})
                elif len(resolved) > 1:
                    # TODO: Don't think we need this anymore; see TODO on Portal.ref_exists.
                    self._unresolved_refs.append({
                        "src": src,
                        "error": f"/{link_to}/{value}",
                        "types": [resolved_ref["type"] for resolved_ref in resolved]})
                else:
                    # A resolved-ref set value is a tuple of the reference path and its uuid.
                    self._resolved_refs.add((f"/{link_to}/{value}", resolved[0].get("uuid")))
#                   self._resolved_refs.add((f"/{link_to}/{value}", resolved[0].get("uuid"), resolved[0].get("data")))
            return value
        return lambda value, src: map_ref(value, typeinfo.get("linkTo"), self._portal, src)

    def _create_typeinfo(self, schema_json: dict, parent_key: Optional[str] = None) -> dict:
        """
        Given a JSON schema return a dictionary of all the property names it defines, but with
        the names of any nested properties (i.e objects within objects) flattened into a single
        property name in dot notation; and set the value of each of these flat property names
        to the type of the terminal/leaf value of the (either) top-level or nested type. N.B. We
        do NOT currently support array-of-array or array-of-multiple-types. E.g. for this schema:

          { "properties": {
              "abc": {
                "type": "object",
                "properties": {
                  "def": { "type": "string" },
                  "ghi": {
                    "type": "object",
                    "properties": {
                      "mno": { "type": "number" }
                    }
                  }
                } },
              "stu": { "type": "array", "items": { "type": "string" } },
              "vw": {
                "type": "array",
                "items": {
                  "type": "object",
                  "properties": {
                    "xyz": { "type": "integer" }
                  } }
              } } }

        Then we will return this flat dictionary:

          { "abc.def":     { "type": "string", "map": <function:map_string> },
            "abc.ghi.mno": { "type": "number", "map": <function:map_number> },
            "stu#":        { "type": "string", "map": <function:map_string> },
            "vw#.xyz":     { "type": "integer", "map": <function:map_integer> } }
        """
        result = {}
        if (properties := schema_json.get("properties")) is None:
            if parent_key:
                if (schema_type := schema_json.get("type")) is None:
                    schema_type = "string"  # Undefined array type; should not happen; just make it a string.
                if schema_type == "array":
                    parent_key += ARRAY_NAME_SUFFIX_CHAR
                result[parent_key] = {"type": schema_type, "map": self._map_function(schema_json)}
                if ARRAY_NAME_SUFFIX_CHAR in parent_key:
                    result[parent_key.replace(ARRAY_NAME_SUFFIX_CHAR, "")] = parent_key
            return result
        for property_key, property_value in properties.items():
            if not isinstance(property_value, dict) or not property_value:
                continue  # Should not happen; every property within properties should be an object; no harm; ignore.
            key = property_key if parent_key is None else f"{parent_key}{DOTTED_NAME_DELIMITER_CHAR}{property_key}"
            if ARRAY_NAME_SUFFIX_CHAR in property_key:
                raise Exception(f"Property name with \"{ARRAY_NAME_SUFFIX_CHAR}\" in JSON schema NOT supported: {key}")
            if (property_value_type := property_value.get("type")) == "object" and "properties" in property_value:
                result.update(self._create_typeinfo(property_value, parent_key=key))
                continue
            if property_value_type == "array":
                while property_value_type == "array":  # Handle array of array here even though we don't in general.
                    if not isinstance((array_property_items := property_value.get("items")), dict):
                        if array_property_items is None or isinstance(array_property_items, list):
                            raise Exception(f"Array of undefined or multiple types in JSON schema NOT supported: {key}")
                        raise Exception(f"Invalid array type specifier in JSON schema: {key}")
                    key = key + ARRAY_NAME_SUFFIX_CHAR
                    if unique := (property_value.get("uniqueItems") is True):
                        pass
                    property_value = array_property_items
                    property_value_type = property_value.get("type")
                typeinfo = self._create_typeinfo(array_property_items, parent_key=key)
                if unique:
                    typeinfo[key]["unique"] = True
                result.update(typeinfo)
                continue
            result[key] = {"type": property_value_type, "map": self._map_function({**property_value, "column": key})}
            if ARRAY_NAME_SUFFIX_CHAR in key:
                result[key.replace(ARRAY_NAME_SUFFIX_CHAR, "")] = key
        return result

    def rationalize_column_name(self, column_name: str, schema_column_name: Optional[str] = None) -> str:
        """
        Replaces any (dot-separated) components of the given column_name which have array indicators/suffixes
        with the corresponding value from the (flattened) schema column names, but with any actual array
        indices from the given column name component. For example, if the (flattened) schema column name
        if "abc#.def##.ghi" and the given column name is "abc.def#1#2#.ghi" returns "abc#.def#1#2.ghi",
        or if the schema column name is "abc###" and the given column name is "abc#0#" then "abc#0##".
        This will "correct" specified columns name (with array indicators) according to the schema.
        """
        if not isinstance(schema_column_name := self._typeinfo.get(self.unadorn_column_name(column_name)), str):
            return column_name
        schema_column_components = _split_dotted_string(schema_column_name)
        for i in range(len(column_components := _split_dotted_string(column_name))):
            schema_array_name, schema_array_indices = Schema.array_indices(schema_column_components[i])
            if schema_array_indices:
                if (array_indices := Schema.array_indices(column_components[i])[1]):
                    if len(schema_array_indices) > len(array_indices):
                        schema_array_indices = array_indices + [-1] * (len(schema_array_indices) - len(array_indices))
                    else:
                        schema_array_indices = array_indices[:len(schema_array_indices)]
                array_qualifiers = "".join([(("#" + str(i)) if i >= 0 else "#") for i in schema_array_indices])
                column_components[i] = schema_array_name + array_qualifiers
        return DOTTED_NAME_DELIMITER_CHAR.join(column_components)

    @staticmethod
    def normalize_column_name(column_name: str) -> str:
        return Schema.unadorn_column_name(column_name, False)

    @staticmethod
    def unadorn_column_name(column_name: str, full: bool = True) -> str:
        """
        Given a string representing a flat column name, i.e possibly dot-separated name components,
        and where each component possibly ends with an array suffix (i.e. pound sign - #) followed
        by an optional integer, returns the unadorned column, without any array suffixes/specifiers.
        """
        unadorned_column_name = DOTTED_NAME_DELIMITER_CHAR.join(
            [ARRAY_NAME_SUFFIX_REGEX.sub(ARRAY_NAME_SUFFIX_CHAR, value)
             for value in _split_dotted_string(column_name)])
        return unadorned_column_name.replace(ARRAY_NAME_SUFFIX_CHAR, "") if full else unadorned_column_name

    @staticmethod
    def type_name(value: str) -> str:  # File or other name.
        name = os.path.basename(value).replace(" ", "") if isinstance(value, str) else ""
        return PortalBase.schema_name(name[0:dot] if (dot := name.rfind(".")) > 0 else name)

    @staticmethod
    def array_indices(name: str) -> Tuple[Optional[str], Optional[List[int]]]:
        indices = []
        while (array_indicator_position := name.rfind(ARRAY_NAME_SUFFIX_CHAR)) > 0:
            array_index = name[array_indicator_position + 1:] if array_indicator_position < len(name) - 1 else -1
            if (array_index := to_integer(array_index)) is None:
                break
            name = name[0:array_indicator_position]
            indices.insert(0, array_index)
        return (name, indices) if indices else (None, None)


class Portal(PortalBase):

    def __init__(self,
                 arg: Optional[Union[VirtualApp, TestApp, Router, Portal, dict, tuple, str]] = None,
                 env: Optional[str] = None, server: Optional[str] = None,
                 app: Optional[OrchestratedApp] = None,
                 data: Optional[dict] = None, schemas: Optional[List[dict]] = None,
                 ref_lookup_strategy: Optional[Callable] = None,
                 ref_lookup_nocache: bool = False,
                 raise_exception: bool = True) -> None:
        super().__init__(arg, env=env, server=server, app=app, raise_exception=raise_exception)
        if isinstance(arg, Portal):
            self._schemas = schemas if schemas is not None else arg._schemas  # Explicitly specified/known schemas.
            self._data = data if data is not None else arg._data  # Data set being loaded; e.g. by StructuredDataSet.
        else:
            self._schemas = schemas
            self._data = data
        if callable(ref_lookup_strategy):
            self._ref_lookup_strategy = ref_lookup_strategy
        else:
            self._ref_lookup_strategy = lambda type_name, schema, value: (StructuredDataSet.REF_LOOKUP_DEFAULT, None)
        if ref_lookup_nocache is True:
            self.ref_lookup = self.ref_lookup_nocache
            self._ref_cache = None
        else:
            self.ref_lookup = self.ref_lookup_cache
            self._ref_cache = {}
        self._ref_lookup_found_count = 0
        self._ref_lookup_notfound_count = 0
        self._ref_lookup_error_count = 0
        self._ref_exists_internal_count = 0
        self._ref_exists_cache_hit_count = 0
        self._ref_exists_cache_miss_count = 0
        self._ref_incorrect_identifying_property_count = 0

    @lru_cache(maxsize=8092)
    def ref_lookup_cache(self, object_name: str) -> Optional[dict]:
        return self.ref_lookup_nocache(object_name)

    def ref_lookup_nocache(self, object_name: str) -> Optional[dict]:
        try:
            result = super().get_metadata(object_name, raw=True)
            self._ref_lookup_found_count += 1
            return result
        except Exception as e:
            if "HTTPNotFound" in str(e):
                self._ref_lookup_notfound_count += 1
            else:
                self._ref_lookup_error_count += 1
            return None

    @lru_cache(maxsize=256)
    def get_schema(self, schema_name: str) -> Optional[dict]:
        if not (schemas := self.get_schemas()):
            return None
        if schema := schemas.get(schema_name := Schema.type_name(schema_name)):
            return schema
        if schema_name == schema_name.upper() and (schema := schemas.get(schema_name.lower().title())):
            return schema
        if schema_name == schema_name.lower() and (schema := schemas.get(schema_name.title())):
            return schema

    @lru_cache(maxsize=1)
    def get_schemas(self) -> Optional[dict]:
        if not (schemas := super().get_schemas()) or (schemas.get("status") == "error"):
            return None
        if self._schemas:
            schemas = copy.deepcopy(schemas)
            for user_specified_schema in self._schemas:
                if user_specified_schema.get("title"):
                    schemas[user_specified_schema["title"]] = user_specified_schema
        return schemas

    @lru_cache(maxsize=64)
    def _get_schema_subtypes(self, type_name: str) -> Optional[List[str]]:
        if not (schemas_super_type_map := self.get_schemas_super_type_map()):
            return []
        return schemas_super_type_map.get(type_name)

    def is_file_schema(self, schema_name: str) -> bool:
        """
        Returns True iff the given schema name isa File type, i.e. has an ancestor which is of type File.
        """
        return self.is_schema_type(schema_name, FILE_SCHEMA_NAME)

    def _ref_exists_from_cache(self, type_name: str, value: str) -> Optional[List[dict]]:
        if self._ref_cache is not None:
            return self._ref_cache.get(f"/{type_name}/{value}", None)
        return None

    def _cache_ref(self, type_name: str, value: str, resolved: List[str], subtype_names: Optional[List[str]]) -> None:
        if self._ref_cache is not None:
            for type_name in [type_name] + (subtype_names if subtype_names else []):
                self._ref_cache[f"/{type_name}/{value}"] = resolved

    def ref_exists(self, type_name: str, value: Optional[str] = None) -> List[dict]:
        if not value:
            if type_name.startswith("/") and len(parts := type_name[1:].split("/")) == 2:
                if not (type_name := parts[0]) or not (value := parts[1]):
                    return []
            else:
                return []
        if (resolved := self._ref_exists_from_cache(type_name, value)) is not None:
            # Found cached resolved reference.
            if not resolved:
                # Cached resolved reference is empty ([]).
                # It might NOW be found internally, since the portal self._data
                # can change, as the data (e.g. spreadsheet sheets) are parsed.
                ref_lookup_strategy, _ = self._ref_lookup_strategy(type_name, self.get_schema(type_name), value)
                is_ref_lookup_subtypes = StructuredDataSet._is_ref_lookup_subtypes(ref_lookup_strategy)
                subtype_names = self._get_schema_subtypes(type_name) if is_ref_lookup_subtypes else None
                is_resolved, identifying_property, resolved_uuid = (
                    self._ref_exists_internally(type_name, value, subtype_names))
                if is_resolved:
                    resolved = [{"type": type_name, "uuid": resolved_uuid}]
                    self._cache_ref(type_name, value, resolved, subtype_names)
                    return resolved
            self._ref_exists_cache_hit_count += 1
            return resolved
        # Not cached here.
        self._ref_exists_cache_miss_count += 1
        # Get the lookup strategy; i.e. should do we lookup by root path, and if so, should
        # we do this first, and do we lookup by subtypes; by default we lookup by root path
        # but not first, and we do lookup by subtypes.
        ref_lookup_strategy, incorrect_identifying_property = (
            self._ref_lookup_strategy(type_name, self.get_schema(type_name), value))
        is_ref_lookup_specified_type = StructuredDataSet._is_ref_lookup_specified_type(ref_lookup_strategy)
        is_ref_lookup_root = StructuredDataSet._is_ref_lookup_root(ref_lookup_strategy)
        is_ref_lookup_root_first = StructuredDataSet._is_ref_lookup_root_first(ref_lookup_strategy)
        is_ref_lookup_subtypes = StructuredDataSet._is_ref_lookup_subtypes(ref_lookup_strategy)
        subtype_names = self._get_schema_subtypes(type_name) if is_ref_lookup_subtypes else None
        # Lookup internally first (including at subtypes if desired; root lookup not applicable here).
        is_resolved, identifying_property, resolved_uuid = (
            self._ref_exists_internally(type_name, value, subtype_names,
                                        incorrect_identifying_property=incorrect_identifying_property))
        if is_resolved:
            if identifying_property == incorrect_identifying_property:
                # Not REALLY resolved as it resolved to a property which is NOT an identifying
                # property, but may be commonly mistaken for one (e.g. UnalignedReads.filename).
                self._ref_incorrect_identifying_property_count += 1
                return []
            resolved = [{"type": type_name, "uuid": resolved_uuid}]
            self._cache_ref(type_name, value, resolved, subtype_names)
            return resolved
        # Not found internally; perform actual portal lookup (including at root and subtypes if desired).
        # First construct the list of lookup paths at which to look for the referenced item.
        lookup_paths = []
        if is_ref_lookup_root_first:
            lookup_paths.append(f"/{value}")
        if is_ref_lookup_specified_type:
            lookup_paths.append(f"/{type_name}/{value}")
        if is_ref_lookup_root and not is_ref_lookup_root_first:
            lookup_paths.append(f"/{value}")
        if subtype_names:
            for subtype_name in subtype_names:
                lookup_paths.append(f"/{subtype_name}/{value}")
        if not lookup_paths:
            # No (i.e. zero) lookup strategy means no ref lookup at all.
            return []
        # Do the actual lookup in the portal for each of the desired lookup paths.
        for lookup_path in lookup_paths:
            if isinstance(item := self.ref_lookup(lookup_path), dict):
                resolved = [{"type": type_name, "uuid": item.get("uuid", None)}]
                self._cache_ref(type_name, value, resolved, subtype_names)
                return resolved
        # Not found at all; note that we cache this ([]) too; indicates lookup has been done.
        self._cache_ref(type_name, value, [], subtype_names)
        return []

    def _ref_exists_internally(
            self, type_name: str, value: str,
            subtype_names: Optional[List[str]] = None,
            incorrect_identifying_property: Optional[str] = None) -> Tuple[bool, Optional[str], Optional[str]]:
        for type_name in [type_name] + (subtype_names or []):
            is_resolved, identifying_property, resolved_uuid = self._ref_exists_single_internally(
                type_name, value, incorrect_identifying_property=incorrect_identifying_property)
            if is_resolved:
                return True, identifying_property, resolved_uuid
        return False, None, None

    def _ref_exists_single_internally(
            self, type_name: str, value: str,
            incorrect_identifying_property:
            Optional[str] = None) -> Tuple[bool, Optional[str], Optional[str]]:
        if self._data and (items := self._data.get(type_name)) and (schema := self.get_schema(type_name)):
            identifying_properties = set(schema.get("identifyingProperties", [])) | {"identifier", "uuid"}
            for item in items:
                for identifying_property in identifying_properties:
                    if (identifying_value := item.get(identifying_property, None)) is not None:
                        if ((identifying_value == value) or
                            (isinstance(identifying_value, list) and (value in identifying_value))):  # noqa
                            self._ref_exists_internal_count += 1
                            return True, identifying_property, item.get("uuid", None)
                if incorrect_identifying_property:
                    if (identifying_value := item.get(incorrect_identifying_property, None)) is not None:
                        if ((identifying_value == value) or
                            (isinstance(identifying_value, list) and (value in identifying_value))):  # noqa
                            return True, incorrect_identifying_property, item.get("uuid", None)
        return False, None, None

    @property
    def ref_lookup_cache_hit_count(self) -> int:
        if self._ref_cache is None:
            return 0
        try:
            return self.ref_lookup_cache.cache_info().hits
        except Exception:
            return -1

    @property
    def ref_lookup_cache_miss_count(self) -> int:
        if self._ref_cache is None:
            return self.ref_lookup_count
        try:
            return self.ref_lookup_cache.cache_info().misses
        except Exception:
            return -1

    @property
    def ref_lookup_count(self) -> int:
        return self._ref_lookup_found_count + self._ref_lookup_notfound_count + self._ref_lookup_error_count

    @property
    def ref_lookup_found_count(self) -> int:
        return self._ref_lookup_found_count

    @property
    def ref_lookup_notfound_count(self) -> int:
        return self._ref_lookup_notfound_count

    @property
    def ref_lookup_error_count(self) -> int:
        return self._ref_lookup_error_count

    @property
    def ref_exists_internal_count(self) -> int:
        return self._ref_exists_internal_count

    @property
    def ref_exists_cache_hit_count(self) -> int:
        return self._ref_exists_cache_hit_count

    @property
    def ref_exists_cache_miss_count(self) -> int:
        return self._ref_exists_cache_miss_count

    @property
    def ref_incorrect_identifying_property_count(self) -> int:
        return self._ref_incorrect_identifying_property_count

    @staticmethod
    def create_for_testing(arg: Optional[Union[str, bool, List[dict], dict, Callable]] = None,
                           schemas: Optional[List[dict]] = None) -> Portal:
        return Portal(PortalBase.create_for_testing(arg), schemas=schemas)


def _split_dotted_string(value: str):
    return split_string(value, DOTTED_NAME_DELIMITER_CHAR)


def _split_array_string(value: str, unique: bool = False):
    return split_string(value, ARRAY_VALUE_DELIMITER_CHAR, ARRAY_VALUE_DELIMITER_ESCAPE_CHAR, unique=unique)
