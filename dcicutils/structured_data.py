import copy
from functools import lru_cache
import json
from jsonschema import Draft7Validator as SchemaValidator
import os
from pyramid.paster import get_app
from pyramid.router import Router
import re
import requests
from requests.models import Response as RequestResponse
import sys
from typing import Any, Callable, List, Optional, Tuple, Type, Union
from webtest.app import TestApp, TestResponse
from dcicutils.common import OrchestratedApp, APP_CGAP, APP_FOURFRONT, APP_SMAHT, ORCHESTRATED_APPS
from dcicutils.creds_utils import CGAPKeyManager, FourfrontKeyManager, SMaHTKeyManager
from dcicutils.data_readers import CsvReader, Excel, RowReader
from dcicutils.ff_utils import get_metadata, get_schema, patch_metadata, post_metadata
from dcicutils.misc_utils import (load_json_if, merge_objects, remove_empty_properties, right_trim, split_string,
                                  to_boolean, to_camel_case, to_enum, to_float, to_integer, VirtualApp)
from dcicutils.zip_utils import temporary_file, unpack_gz_file_to_temporary_file, unpack_files


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

# Forward type references for type hints.
Portal = Type["Portal"]
PortalBase = Type["PortalBase"]
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
        self._issues = None
        self._load_file(file) if file else None

    @staticmethod
    def load(file: str, portal: Optional[Union[VirtualApp, TestApp, Portal]] = None,
             schemas: Optional[List[dict]] = None,
             order: Optional[List[str]] = None, prune: bool = True) -> StructuredDataSet:
        return StructuredDataSet(file=file, portal=portal, schemas=schemas, order=order, prune=prune)

    def validate(self) -> Optional[List[str]]:
        issues = []
        for type_name in self.data:
            if (schema := Schema.load_by_name(type_name, portal=self._portal)):
                item_number = 0
                for data in self.data[type_name]:
                    item_number += 1
                    if (validation_issues := schema.validate(data)) is not None:
                        issues.extend([f"{schema.name} [{item_number}]: {issue}" for issue in validation_issues])
        return issues + (self._issues or [])

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
        self._load_reader(reader := CsvReader(file), type_name=Schema.type_name(file))
        self._note_issues(reader.issues, os.path.basename(file))

    def _load_excel_file(self, file: str) -> None:
        excel = Excel(file)  # Order the sheet names by any specified ordering (e.g. ala snovault.loadxl).
        order = {Schema.type_name(key): index for index, key in enumerate(self._order)} if self._order else {}
        for sheet_name in sorted(excel.sheet_names, key=lambda key: order.get(Schema.type_name(key), sys.maxsize)):
            self._load_reader(reader := excel.sheet_reader(sheet_name), type_name=Schema.type_name(sheet_name))
            self._note_issues(reader.issues, f"{file}:{sheet_name}")

    def _load_json_file(self, file: str) -> None:
        with open(file) as f:
            self._add(Schema.type_name(file), json.load(f))

    def _load_reader(self, reader: RowReader, type_name: str) -> None:
        schema = None
        noschema = False
        structured_row_template = None
        for row in reader:
            if not structured_row_template:  # Delay creation just so we don't create it if there are no rows.
                if not schema and not noschema and not (schema := Schema.load_by_name(type_name, portal=self._portal)):
                    noschema = True
                structured_row_template = _StructuredRowTemplate(reader.header, schema)
            structured_row = structured_row_template.create_row()
            for column_name, value in row.items():
                structured_row_template.set_value(structured_row, column_name, value, reader.location)
            if schema and (schema_name := schema.name):
                type_name = schema_name
            self._add(type_name, structured_row)

    def _add(self, type_name: str, data: Union[dict, List[dict]]) -> None:
        if self._prune:
            remove_empty_properties(data)
        if type_name in self.data:
            self.data[type_name].extend([data] if isinstance(data, dict) else data)
        else:
            self.data[type_name] = [data] if isinstance(data, dict) else data

    def _note_issues(self, issues: Optional[List[str]], source: str) -> None:
        if issues:
            if not self._issues:
                self._issues = []
            self._issues.append({source: issues})


class _StructuredRowTemplate:

    def __init__(self, column_names: List[str], schema: Optional[Schema] = None) -> None:
        self._schema = schema
        self._set_value_functions = {}
        self._template = self._create_row_template(column_names)

    def create_row(self) -> dict:
        return copy.deepcopy(self._template)

    def set_value(self, data: dict, column_name: str, value: str, loc: int = -1) -> None:
        if (set_value_function := self._set_value_functions.get(column_name)):
            src = (f"{f'{self._schema.name}.' if self._schema else ''}" +
                   f"{f'{column_name}' if column_name else ''}{f' [{loc}]' if loc else ''}")
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
        self._typeinfo = self._create_typeinfo(schema_json)

    @staticmethod
    def load_by_name(name: str, portal: Portal) -> Optional[dict]:
        return Schema(portal.get_schema(Schema.type_name(name)), portal) if portal else None

    def validate(self, data: dict) -> Optional[List[str]]:
        issues = []
        for issue in SchemaValidator(self.data, format_checker=SchemaValidator.FORMAT_CHECKER).iter_errors(data):
            issues.append(issue.message)
        return issues if issues else None

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
            exception = None
            if not value:
                if (column := typeinfo.get("column")) and column in self.data.get("required", []):
                    exception = f"No required reference (linkTo) value for: {link_to}"
            elif portal and not portal.ref_exists(link_to, value):
                exception = f"Cannot resolve reference (linkTo) for: {link_to}"
            if exception:
                raise Exception(exception + f"{f'/{value}' if value else ''}{f' from {src}' if src else ''}")
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
        return to_camel_case(name[0:dot] if (dot := name.rfind(".")) > 0 else name)

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


class PortalBase:

    def __init__(self,
                 arg: Optional[Union[VirtualApp, TestApp, Router, Portal, dict, tuple, str]] = None,
                 env: Optional[str] = None, app: OrchestratedApp = APP_SMAHT, server: Optional[str] = None,
                 key: Optional[Union[dict, tuple]] = None,
                 portal: Optional[Union[VirtualApp, TestApp, Router, Portal, str]] = None) -> PortalBase:
        if isinstance(arg, VirtualApp) and not portal:
            portal = arg
        elif isinstance(arg, TestApp) and not portal:
            portal = arg
        elif isinstance(arg, Router) and not portal:
            portal = arg
        elif isinstance(arg, Portal) and not portal:
            portal = arg
        elif isinstance(arg, str) and arg.endswith(".ini") and not portal:
            portal = arg
        elif isinstance(arg, str) and not env:
            env = arg
        elif (isinstance(arg, dict) or isinstance(arg, tuple)) and not key:
            key = arg
        self._vapp = None
        self._key = None
        self._key_pair = None
        self._server = None
        if isinstance(portal, Portal):
            self._vapp = portal._vapp
            self._key = portal._key
            self._key_pair = portal._key_pair
            self._server = portal._server
        elif isinstance(portal, (VirtualApp, TestApp)):
            self._vapp = portal
        elif isinstance(portal, (Router, str)):
            self._vapp = PortalBase._create_testapp(portal)
        elif isinstance(key, dict):
            self._key = key
            self._key_pair = (key.get("key"), key.get("secret")) if key else None
        elif isinstance(key, tuple) and len(key) >= 2:
            self._key = {"key": key[0], "secret": key[1]}
            self._key_pair = key
        elif isinstance(env, str):
            key_managers = {APP_CGAP: CGAPKeyManager, APP_FOURFRONT: FourfrontKeyManager, APP_SMAHT: SMaHTKeyManager}
            if not (key_manager := key_managers.get(app)) or not (key_manager := key_manager()):
                raise Exception(f"Invalid app name: {app} (valid: {', '.join(ORCHESTRATED_APPS)}).")
            if isinstance(env, str):
                self._key = key_manager.get_keydict_for_env(env)
                self._server = self._key.get("server") if self._key else None
            elif isinstance(server, str):
                self._key = key_manager.get_keydict_for_server(server)
                self._server = server
            self._key_pair = key_manager.keydict_to_keypair(self._key) if self._key else None

    def get_metadata(self, object_id: str) -> Optional[dict]:
        return get_metadata(obj_id=object_id, vapp=self._vapp, key=self._key)

    def patch_metadata(self, object_id: str, data: str) -> Optional[dict]:
        if self._key:
            return patch_metadata(obj_id=object_id, patch_item=data, key=self._key)
        return self.patch(f"/{object_id}", data)

    def post_metadata(self, object_type: str, data: str) -> Optional[dict]:
        if self._key:
            return post_metadata(schema_name=object_type, post_item=data, key=self._key)
        return self.post(f"/{object_type}", data)

    def get_schema(self, schema_name: str) -> Optional[dict]:
        return get_schema(schema_name, portal_vapp=self._vapp, key=self._key)

    def get(self, uri: str, follow: bool = True, **kwargs) -> Optional[Union[RequestResponse, TestResponse]]:
        if isinstance(self._vapp, (VirtualApp, TestApp)):
            response = self._vapp.get(self._uri(uri), **self._kwargs(**kwargs))
            if response and response.status_code in [301, 302, 303, 307, 308] and follow:
                response = response.follow()
            return response
        return requests.get(self._uri(uri), allow_redirects=follow, **self._kwargs(**kwargs))

    def patch(self, uri: str, data: Optional[dict] = None,
              json: Optional[dict] = None, **kwargs) -> Optional[Union[RequestResponse, TestResponse]]:
        if isinstance(self._vapp, (VirtualApp, TestApp)):
            return self._vapp.patch_json(self._uri(uri), json or data, **self._kwargs(**kwargs))
        return requests.patch(self._uri(uri), json=json or data, **self._kwargs(**kwargs))

    def post(self, uri: str, data: Optional[dict] = None, json: Optional[dict] = None,
             files: Optional[dict] = None, **kwargs) -> Optional[Union[RequestResponse, TestResponse]]:
        if isinstance(self._vapp, (VirtualApp, TestApp)):
            if files:
                return self._vapp.post(self._uri(uri), json or data, upload_files=files, **self._kwargs(**kwargs))
            else:
                return self._vapp.post_json(self._uri(uri), json or data, upload_files=files, **self._kwargs(**kwargs))
        return requests.post(self._uri(uri), json=json or data, files=files, **self._kwargs(**kwargs))

    def _uri(self, uri: str) -> str:
        if not isinstance(uri, str) or not uri:
            return "/"
        if uri.lower().startswith("http://") or uri.lower().startswith("https://"):
            return uri
        uri = re.sub(r"/+", "/", uri)
        return (self._server + ("/" if uri.startswith("/") else "") + uri) if self._server else uri

    def _kwargs(self, **kwargs) -> dict:
        result_kwargs = {"headers":
                         kwargs.get("headers", {"Content-type": "application/json", "Accept": "application/json"})}
        if self._key_pair:
            result_kwargs["auth"] = self._key_pair
        if isinstance(timeout := kwargs.get("timeout"), int):
            result_kwargs["timeout"] = timeout
        return result_kwargs

    @staticmethod
    def create_for_testing(ini_file: Optional[str] = None) -> PortalBase:
        if isinstance(ini_file, str):
            return Portal(Portal._create_testapp(ini_file))
        minimal_ini_for_unit_testing = "[app:app]\nuse = egg:encoded\nsqlalchemy.url = postgresql://dummy\n"
        with temporary_file(content=minimal_ini_for_unit_testing, suffix=".ini") as ini_file:
            return Portal(Portal._create_testapp(ini_file))

    @staticmethod
    def create_for_testing_local(ini_file: Optional[str] = None) -> Portal:
        if isinstance(ini_file, str):
            return Portal(Portal._create_testapp(ini_file))
        minimal_ini_for_testing_local = "\n".join([
            "[app:app]\nuse = egg:encoded\nfile_upload_bucket = dummy",
            "sqlalchemy.url = postgresql://postgres@localhost:5441/postgres?host=/tmp/snovault/pgdata",
            "multiauth.groupfinder = encoded.authorization.smaht_groupfinder",
            "multiauth.policies = auth0 session remoteuser accesskey",
            "multiauth.policy.session.namespace = mailto",
            "multiauth.policy.session.use = encoded.authentication.NamespacedAuthenticationPolicy",
            "multiauth.policy.session.base = pyramid.authentication.SessionAuthenticationPolicy",
            "multiauth.policy.remoteuser.namespace = remoteuser",
            "multiauth.policy.remoteuser.use = encoded.authentication.NamespacedAuthenticationPolicy",
            "multiauth.policy.remoteuser.base = pyramid.authentication.RemoteUserAuthenticationPolicy",
            "multiauth.policy.accesskey.namespace = accesskey",
            "multiauth.policy.accesskey.use = encoded.authentication.NamespacedAuthenticationPolicy",
            "multiauth.policy.accesskey.base = encoded.authentication.BasicAuthAuthenticationPolicy",
            "multiauth.policy.accesskey.check = encoded.authentication.basic_auth_check",
            "multiauth.policy.auth0.use = encoded.authentication.NamespacedAuthenticationPolicy",
            "multiauth.policy.auth0.namespace = auth0",
            "multiauth.policy.auth0.base = encoded.authentication.Auth0AuthenticationPolicy"
        ])
        with temporary_file(content=minimal_ini_for_testing_local, suffix=".ini") as minimal_ini_file:
            return Portal(Portal._create_testapp(minimal_ini_file))

    @staticmethod
    def _create_testapp(value: Union[str, Router, TestApp] = "development.ini") -> TestApp:
        """
        Creates and returns a TestApp. Refactored out of above loadxl code to consolidate at a
        single point; also for use by the generate_local_access_key and view_local_object scripts.
        """
        if isinstance(value, TestApp):
            return value
        app = value if isinstance(value, Router) else get_app(value, "app")
        return TestApp(app, {"HTTP_ACCEPT": "application/json", "REMOTE_USER": "TEST"})


class Portal(PortalBase):

    def __init__(self,
                 arg: Optional[Union[VirtualApp, TestApp, Router, Portal, dict, tuple, str]] = None,
                 env: Optional[str] = None, app: OrchestratedApp = APP_SMAHT, server: Optional[str] = None,
                 key: Optional[Union[dict, tuple]] = None,
                 portal: Optional[Union[VirtualApp, TestApp, Router, Portal, str]] = None,
                 data: Optional[dict] = None, schemas: Optional[List[dict]] = None) -> Optional[Portal]:
        super(Portal, self).__init__(arg, env=env, app=app, server=server, key=key, portal=portal)
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
            return super(Portal, self).get_metadata(object_name)
        except Exception:
            return None

    @lru_cache(maxsize=256)
    def get_schema(self, schema_name: str) -> Optional[dict]:
        def get_schema_exact(schema_name: str) -> Optional[dict]:  # noqa
            return (next((schema for schema in self._schemas or []
                         if Schema.type_name(schema.get("title")) == Schema.type_name(schema_name)), None) or
                    super(Portal, self).get_schema(schema_name))
        try:
            if (schema := get_schema_exact(schema_name)):
                return schema
        except Exception:  # Try/force camel-case if all upper/lower-case.
            if schema_name == schema_name.upper():
                if (schema := get_schema_exact(schema_name.lower().title())):
                    return schema
            elif schema_name == schema_name.lower():
                if (schema := get_schema_exact(schema_name.title())):
                    return schema
            raise

    def ref_exists(self, type_name: str, value: str) -> bool:
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