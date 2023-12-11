import copy
from functools import lru_cache
import json
from jsonschema import Draft7Validator as SchemaValidator
import os
from pyramid.router import Router
import re
import sys
from typing import Any, Callable, List, Optional, Tuple, Type, Union
from webtest.app import TestApp
from dcicutils.common import OrchestratedApp
from dcicutils.data_readers import CsvReader, Excel, RowReader
from dcicutils.misc_utils import (create_dict, load_json_if, merge_objects, remove_empty_properties, right_trim,
                                  split_string, to_boolean, to_enum, to_float, to_integer, VirtualApp)
from dcicutils.portal_utils import Portal as PortalBase
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
FILE_SCHEMA_NAME_PROPERTY = "filename"

# Forward type references for type hints.
Portal = Type["Portal"]
Schema = Type["Schema"]
StructuredDataSet = Type["StructuredDataSet"]


class StructuredDataSet:

    def __init__(self, file: Optional[str] = None, portal: Optional[Union[VirtualApp, TestApp, Portal]] = None,
                 schemas: Optional[List[dict]] = None, data: Optional[List[dict]] = None,
                 order: Optional[List[str]] = None, prune: bool = True) -> None:
        self.data = {} if not data else data  # If portal is None then no schemas nor refs.
        self._portal = Portal(portal, data=self.data, schemas=schemas) if portal else None
        self._order = order
        self._prune = prune
        self._warnings = {}
        self._errors = {}
        self._resolved_refs = []
        self._validated = False
        self._load_file(file) if file else None

    @staticmethod
    def load(file: str, portal: Optional[Union[VirtualApp, TestApp, Portal]] = None,
             schemas: Optional[List[dict]] = None,
             order: Optional[List[str]] = None, prune: bool = True) -> StructuredDataSet:
        return StructuredDataSet(file=file, portal=portal, schemas=schemas, order=order, prune=prune)

    def validate(self, force: bool = False) -> None:
        if self._validated and not force:
            return
        self._validated = True
        for type_name in self.data:
            if (schema := Schema.load_by_name(type_name, portal=self._portal)):
                row_number = 0
                for data in self.data[type_name]:
                    row_number += 1
                    if (validation_errors := schema.validate(data)) is not None:
                        for validation_error in validation_errors:
                            self._note_error({"src": create_dict(type=schema.name, row=row_number),
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
        return self._resolved_refs

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

    def _load_file(self, file: str) -> None:
        # Returns a dictionary where each property is the name (i.e. the type) of the data,
        # and the value is array of dictionaries for the data itself. Handle these kinds of files:
        # 1.  Single CSV of JSON file, where the (base) name of the file is the data type name.
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
        excel = Excel(file)  # Order the sheet names by any specified ordering (e.g. ala snovault.loadxl).
        order = {Schema.type_name(key): index for index, key in enumerate(self._order)} if self._order else {}
        for sheet_name in sorted(excel.sheet_names, key=lambda key: order.get(Schema.type_name(key), sys.maxsize)):
            self._load_reader(excel.sheet_reader(sheet_name), type_name=Schema.type_name(sheet_name))

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
                elif schema and (schema_name := schema.name):
                    type_name = schema_name
                structured_row_template = _StructuredRowTemplate(reader.header, schema)
            structured_row = structured_row_template.create_row()
            for column_name, value in row.items():
                structured_row_template.set_value(structured_row, column_name, value, reader.file, reader.row_number)
            self._add(type_name, structured_row)
        self._note_warning(reader.warnings, "reader")
        if schema:
            self._note_error(schema._unresolved_refs, "ref")
            self._resolved_refs = schema._resolved_refs

    def _add(self, type_name: str, data: Union[dict, List[dict]]) -> None:
        if self._prune:
            remove_empty_properties(data)
        if type_name in self.data:
            self.data[type_name].extend([data] if isinstance(data, dict) else data)
        else:
            self.data[type_name] = [data] if isinstance(data, dict) else data

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
            src = create_dict(type=self._schema.name if self._schema else None,
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
                               path: List[Union[str, int]], mapv: Optional[Callable]) -> None:

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
                values = _split_array_string(value)
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
            map_value_function = self._schema.get_map_value_function(rational_column_name) if self._schema else None
            if (column_components := _split_dotted_string(rational_column_name)):
                merge_objects(structured_row_template, parse_components(column_components, path := []), True)
                self._set_value_functions[column_name] = (lambda data, value, src, path=path, mapv=map_value_function:
                                                          set_value_internal(data, value, src, path, mapv))
        return structured_row_template


class Schema:

    def __init__(self, schema_json: dict, portal: Optional[Portal] = None) -> None:
        self.data = schema_json
        self.name = Schema.type_name(schema_json.get("title", "")) if schema_json else ""
        self._portal = portal  # Needed only to resolve linkTo references.
        self._map_value_functions = {
            "boolean": self._map_function_boolean,
            "enum": self._map_function_enum,
            "integer": self._map_function_integer,
            "number": self._map_function_number,
            "string": self._map_function_string
        }
        self._resolved_refs = set()
        self._unresolved_refs = []
        self._typeinfo = self._create_typeinfo(schema_json)

    @staticmethod
    def load_by_name(name: str, portal: Portal) -> Optional[dict]:
        return Schema(portal.get_schema(Schema.type_name(name)), portal) if portal else None

    def validate(self, data: dict) -> List[str]:
        errors = []
        for error in SchemaValidator(self.data, format_checker=SchemaValidator.FORMAT_CHECKER).iter_errors(data):
            errors.append(error.message)
        return errors

    @property
    def unresolved_refs(self) -> List[dict]:
        return self._unresolved_refs

    @property
    def resolved_refs(self) -> List[str]:
        return list(self._resolved_refs)

    def get_map_value_function(self, column_name: str) -> Optional[Any]:
        return (self._get_typeinfo(column_name) or {}).get("map")

    def _get_typeinfo(self, column_name: str) -> Optional[dict]:
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

    def _map_function_ref(self, typeinfo: dict) -> Callable:
        def map_ref(value: str, link_to: str, portal: Optional[Portal], src: Optional[str]) -> Any:
            nonlocal self, typeinfo
            if not value:
                if (column := typeinfo.get("column")) and column in self.data.get("required", []):
                    self._unresolved_refs.append({"src": src, "error": f"/{link_to}/<null>"})
            elif portal:
                if not (resolved := portal.ref_exists(link_to, value)):
                    self._unresolved_refs.append({"src": src, "error": f"/{link_to}/{value}"})
                elif len(resolved) > 1:
                    self._unresolved_refs.append({"src": src, "error": f"/{link_to}/{value}", "types": resolved})
                else:
                    self._resolved_refs.add(f"/{link_to}/{value}")
            return value
        return lambda value, src: map_ref(value, typeinfo.get("linkTo"), self._portal, src)

    def _create_typeinfo(self, schema_json: dict, parent_key: Optional[str] = None) -> dict:
        """
        Given a JSON schema return a dictionary of all the property names it defines, but with
        the names of any nested properties (i.e objects within objects) flattened into a single
        property name in dot notation; and set the value of each of these flat property names
        to the type of the terminal/leaf value of the (either) top-level or nested type. N.B. We
        do NOT currently support array-of-arry or array-of-multiple-types. E.g. for this schema:

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
                    property_value = array_property_items
                    property_value_type = property_value.get("type")
                result.update(self._create_typeinfo(array_property_items, parent_key=key))
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
                 env: Optional[str] = None, app: OrchestratedApp = None, server: Optional[str] = None,
                 key: Optional[Union[dict, tuple]] = None,
                 portal: Optional[Union[VirtualApp, TestApp, Router, Portal, str]] = None,
                 data: Optional[dict] = None, schemas: Optional[List[dict]] = None) -> Optional[Portal]:
        super().__init__(arg, env=env, app=app, server=server, key=key, portal=portal)
        if isinstance(arg, Portal) and not portal:
            portal = arg
        if isinstance(portal, Portal):
            self._schemas = schemas if schemas is not None else portal._schemas  # Explicitly specified/known schemas.
            self._data = data if data is not None else portal._data  # Data set being loaded; e.g. by StructuredDataSet.
        else:
            self._schemas = schemas
            self._data = data

    @lru_cache(maxsize=256)
    def get_metadata(self, object_name: str) -> Optional[dict]:
        try:
            return super().get_metadata(object_name)
        except Exception:
            return None

    @lru_cache(maxsize=256)
    def get_schema(self, schema_name: str) -> Optional[dict]:
        if (schemas := self.get_schemas()) and (schema := schemas.get(schema_name := Schema.type_name(schema_name))):
            return schema
        if schema_name == schema_name.upper() and (schema := schemas.get(schema_name.lower().title())):
            return schema
        if schema_name == schema_name.lower() and (schema := schemas.get(schema_name.title())):
            return schema

    @lru_cache(maxsize=1)
    def get_schemas(self) -> dict:
        schemas = super().get_schemas()
        if self._schemas:
            schemas = copy.deepcopy(schemas)
            for user_specified_schema in self._schemas:
                if user_specified_schema.get("title"):
                    schemas[user_specified_schema["title"]] = user_specified_schema
        return schemas

    @lru_cache(maxsize=1)
    def get_schemas_super_type_map(self) -> dict:
        return super().get_schemas_super_type_map()

    def ref_exists(self, type_name: str, value: str) -> List[str]:
        resolved = []
        if self._ref_exists_single(type_name, value):
            resolved.append(type_name)
        # Check for the given ref in all sub-types of the given type.
        if (schemas_super_type_map := self.get_schemas_super_type_map()):
            if (sub_type_names := schemas_super_type_map.get(type_name)):
                for sub_type_name in sub_type_names:
                    if self._ref_exists_single(sub_type_name, value):
                        resolved.append(type_name)
        return resolved

    def _ref_exists_single(self, type_name: str, value: str) -> bool:
        if self._data and (items := self._data.get(type_name)) and (schema := self.get_schema(type_name)):
            iproperties = set(schema.get("identifyingProperties", [])) | {"identifier", "uuid"}
            for item in items:
                if (ivalue := next((item[iproperty] for iproperty in iproperties if iproperty in item), None)):
                    if isinstance(ivalue, list) and value in ivalue or ivalue == value:
                        return True
        return self.get_metadata(f"/{type_name}/{value}") is not None

    @staticmethod
    def create_for_testing(ini_file: Optional[str] = None, schemas: Optional[List[dict]] = None) -> Portal:
        return Portal(PortalBase.create_for_testing(ini_file), schemas=schemas)

    @staticmethod
    def create_for_testing_local(ini_file: Optional[str] = None, schemas: Optional[List[dict]] = None) -> Portal:
        return Portal(PortalBase.create_for_testing_local(ini_file), schemas=schemas)


def _split_dotted_string(value: str):
    return split_string(value, DOTTED_NAME_DELIMITER_CHAR)


def _split_array_string(value: str):
    return split_string(value, ARRAY_VALUE_DELIMITER_CHAR, ARRAY_VALUE_DELIMITER_ESCAPE_CHAR)
