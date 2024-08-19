# ------------------------------------------------------------------------------------------------------
# Command-line utility to retrieve and print the given object (UUID) from a SMaHT/CGAP/Fourfront Portal.
# ------------------------------------------------------------------------------------------------------
# Example command:
#  view-portal-object 4483b19d-62e7-4e7f-a211-0395343a35df --yaml
#
# Example output:
#  '@context': /terms/
#  '@id': /access-keys/3968e38e-c11f-472e-8531-8650e2e296d4/
#  '@type':
#  - AccessKey
#  - Item
#  access_key_id: NSVCZ75O
#  date_created: '2023-09-06T13:11:59.704005+00:00'
#  description: Manually generated local access-key for testing.
#  display_title: AccessKey from 2023-09-06
#  expiration_date: '2023-12-05T13:11:59.714106'
#  last_modified:
#    date_modified: '2023-09-06T13:11:59.711367+00:00'
#    modified_by:
#      '@id': /users/3202fd57-44d2-44fb-a131-afb1e43d8ae5/
#      '@type':
#      - User
#      - Item
#      status: current
#      uuid: 3202fd57-44d2-44fb-a131-afb1e43d8ae5
#  principals_allowed:
#    edit:
#    - group.admin
#    - userid.74fef71a-dfc1-4aa4-acc0-cedcb7ac1d68
#    view:
#    - group.admin
#    - group.read-only-admin
#    - userid.74fef71a-dfc1-4aa4-acc0-cedcb7ac1d68
#  schema_version: '1'
#  status: current
#  user:
#    '@id': /users/74fef71a-dfc1-4aa4-acc0-cedcb7ac1d68/
#    '@type':
#    - User
#    - Item
#    display_title: David Michaels
#    principals_allowed:
#      edit:
#      - group.admin
#      view:
#      - group.admin
#      - group.read-only-admin
#    status: current
#    uuid: 74fef71a-dfc1-4aa4-acc0-cedcb7ac1d68
#  uuid: 3968e38e-c11f-472e-8531-8650e2e296d4
#
# Note that instead of a uuid you can also actually use a path, for example:
#   view-local-object /file-formats/vcf_gz_tbi
#
# --------------------------------------------------------------------------------------------------

import argparse
from functools import lru_cache
import io
import json
import pyperclip
import os
import sys
from typing import Callable, List, Optional, TextIO, Tuple, Union
import yaml
from dcicutils.captured_output import captured_output, uncaptured_output
from dcicutils.command_utils import yes_or_no
from dcicutils.misc_utils import get_error_message, is_uuid, PRINT, to_snake_case
from dcicutils.portal_utils import Portal


# Schema properties to ignore (by default) for the view schema usage.
_SCHEMAS_IGNORE_PROPERTIES = [
    "date_created",
    "last_modified",
    "principals_allowed",
    "submitted_by",
    "schema_version"
]

_output_file: TextIO = None


def main():

    global _output_file

    parser = argparse.ArgumentParser(description="View Portal object.")
    parser.add_argument("uuid", nargs="?", type=str,
                        help=f"The uuid (or path) of the object to fetch and view. ")
    parser.add_argument("--ini", type=str, required=False, default=None,
                        help=f"Name of the application .ini file.")
    parser.add_argument("--env", "-e", type=str, required=False, default=None,
                        help=f"Environment name (key from ~/.smaht-keys.json).")
    parser.add_argument("--server", "-s", type=str, required=False, default=None,
                        help=f"Environment server name (server from key in ~/.smaht-keys.json).")
    parser.add_argument("--app", type=str, required=False, default=None,
                        help=f"Application name (one of: smaht, cgap, fourfront).")
    parser.add_argument("--schema", action="store_true", required=False, default=False,
                        help="View named schema rather than object.")
    parser.add_argument("--all", action="store_true", required=False, default=False,
                        help="Include all properties for schema usage.")
    parser.add_argument("--raw", action="store_true", required=False, default=False, help="Raw output.")
    parser.add_argument("--inserts", action="store_true", required=False, default=False,
                        help="Format output for subsequent inserts.")
    parser.add_argument("--insert-files", action="store_true", required=False, default=False,
                        help="Output for to insert files.")
    parser.add_argument("--ignore", nargs="+", help="Ignore these fields for --inserts.")
    parser.add_argument("--tree", action="store_true", required=False, default=False, help="Tree output for schemas.")
    parser.add_argument("--database", action="store_true", required=False, default=False,
                        help="Read from database output.")
    parser.add_argument("--bool", action="store_true", required=False,
                        default=False, help="Only return whether found or not.")
    parser.add_argument("--yaml", action="store_true", required=False, default=False, help="YAML output.")
    parser.add_argument("--copy", "-c", action="store_true", required=False, default=False,
                        help="Copy object data to clipboard.")
    parser.add_argument("--output", required=False, help="Output file.", type=str)
    parser.add_argument("--indent", required=False, default=False, help="Indent output.", type=int)
    parser.add_argument("--summary", action="store_true", required=False, default=False,
                        help="Summary output (for schema only).")
    parser.add_argument("--force", action="store_true", required=False, default=False, help="Debugging output.")
    parser.add_argument("--terse", action="store_true", required=False, default=False, help="Terse output.")
    parser.add_argument("--verbose", action="store_true", required=False, default=False, help="Verbose output.")
    parser.add_argument("--noheader", action="store_true", required=False, default=False, help="Supress header output.")
    parser.add_argument("--debug", action="store_true", required=False, default=False, help="Debugging output.")
    args = parser.parse_args()

    portal = _create_portal(ini=args.ini, env=args.env or os.environ.get("SMAHT_ENV"),
                            server=args.server, app=args.app,
                            verbose=args.verbose and not args.noheader, debug=args.debug)

    if not args.uuid:
        _print("UUID or schema or path required.")
        _exit(1)

    if args.insert_files:
        args.inserts = True
        if args.output:
            if not os.path.isdir(args.output):
                _print(f"Specified output directory for insert files does not exist: {args.output}")
                exit(1)
            args.insert_files = args.output
            args.output = None

    if args.output:
        if os.path.exists(args.output):
            if os.path.isdir(args.output):
                _print(f"Specified output file already exists as a directory: {args.output}")
                _exit(1)
            elif os.path.isfile(args.output):
                _print(f"Specified output file already exists: {args.output}")
                if (not args.force) and not yes_or_no(f"Do you want to overwrite this file?"):
                    _exit(0)
        _output_file = io.open(args.output, "w")

    if args.uuid and ((args.uuid.lower() == "schemas") or (args.uuid.lower() == "schema")):
        _print_all_schema_names(portal=portal, terse=args.terse, all=args.all,
                                tree=args.tree, summary=args.summary, yaml=args.yaml)
        return
    elif args.uuid and (args.uuid.lower() == "info"):
        if consortia := portal.get_metadata("/consortia?limit=1000"):
            _print_output("Known Consortia:")
            consortia = sorted(consortia.get("@graph", []), key=lambda key: key.get("identifier"))
            for consortium in consortia:
                if ((consortium_name := consortium.get("identifier")) and
                    (consortium_uuid := consortium.get("uuid"))):  # noqa
                    _print_output(f"- {consortium_name}: {consortium_uuid}")
        if submission_centers := portal.get_metadata("/submission-centers?limit=1000"):
            _print_output("Known Submission Centers:")
            submission_centers = sorted(submission_centers.get("@graph", []), key=lambda key: key.get("identifier"))
            for submission_center in submission_centers:
                if ((submission_center_name := submission_center.get("identifier")) and
                    (submission_center_uuid := submission_center.get("uuid"))):  # noqa
                    _print_output(f"- {submission_center_name}: {submission_center_uuid}")
        try:
            if file_formats := portal.get_metadata("/file-formats?limit=1000"):
                _print_output("Known File Formats:")
                file_formats = sorted(file_formats.get("@graph", []), key=lambda key: key.get("identifier"))
                for file_format in file_formats:
                    if ((file_format_name := file_format.get("identifier")) and
                        (file_format_uuid := file_format.get("uuid"))):  # noqa
                        _print_output(f"- {file_format_name}: {file_format_uuid}")
        except Exception:
            _print_output("Known File Formats: None")
        return

    if _is_maybe_schema_name(args.uuid):
        args.schema = True

    if args.schema:
        schema, schema_name = _get_schema(portal, args.uuid)
        if schema:
            if args.copy:
                pyperclip.copy(json.dumps(schema, indent=4))
            if args.summary:
                if parent_schema_name := _get_parent_schema_name(schema):
                    if schema.get("isAbstract") is True:
                        _print_output(f"{schema_name} | parent: {parent_schema_name} | abstract")
                    else:
                        _print_output(f"{schema_name} | parent: {parent_schema_name}")
                else:
                    _print_output(schema_name)
            _print_schema(schema, terse=args.terse,
                          all=args.all, summary=args.summary, yaml=args.yaml)
            return

    data = _get_portal_object(portal=portal, uuid=args.uuid, raw=args.raw, database=args.database,
                              inserts=args.inserts, insert_files=args.insert_files,
                              ignore=args.ignore, check=args.bool,
                              force=args.force, verbose=args.verbose, debug=args.debug)
    if args.insert_files:
        return

    if args.bool:
        if data:
            _print(f"{args.uuid}: found")
            _exit(0)
        else:
            _print(f"{args.uuid}: not found")
            _exit(1)
    if args.copy:
        pyperclip.copy(json.dumps(data, indent=4))
    if args.yaml:
        _print_output(yaml.dump(data))
    else:
        if args.indent > 0:
            _print_output(_format_json_with_indent(data, indent=args.indent))
        else:
            _print_output(json.dumps(data, default=str, indent=4))


def _format_json_with_indent(value: dict, indent: int = 0) -> Optional[str]:
    if isinstance(value, dict):
        result = json.dumps(value, indent=4)
        if indent > 0:
            result = f"{indent * ' '}{result}"
            result = result.replace("\n", f"\n{indent * ' '}")
        return result


def _create_portal(ini: str, env: Optional[str] = None,
                   server: Optional[str] = None, app: Optional[str] = None,
                   verbose: bool = False, debug: bool = False) -> Portal:
    portal = None
    with captured_output(not debug):
        portal = Portal(env, server=server, app=app) if env or app else Portal(ini)
    if portal:
        if verbose:
            if portal.env:
                _print(f"Portal environment: {portal.env}")
            if portal.keys_file:
                _print(f"Portal keys file: {portal.keys_file}")
            if portal.key_id:
                _print(f"Portal key prefix: {portal.key_id[0:2]}******")
            if portal.ini_file:
                _print(f"Portal ini file: {portal.ini_file}")
            if portal.server:
                _print(f"Portal server: {portal.server}")
        return portal


def _get_portal_object(portal: Portal, uuid: str,
                       raw: bool = False, database: bool = False,
                       inserts: bool = False, insert_files: bool = False,
                       ignore: Optional[List[str]] = None,
                       check: bool = False, force: bool = False,
                       verbose: bool = False, debug: bool = False) -> dict:

    def prune_data(data: dict) -> dict:
        nonlocal ignore
        if not isinstance(ignore, list) or not ignore:
            return data
        return {key: value for key, value in data.items() if key not in ignore}

    def get_metadata_for_individual_result_type(uuid: str) -> Optional[dict]:  # noqa
        # There can be a lot of individual results for which we may need to get the actual type,
        # so do this in a function we were can give verbose output feedback.
        nonlocal portal, results_index, results_total, verbose
        if verbose:
            _print(f"Getting actual type for {results_type} result:"
                   f" {uuid} [{results_index} of {results_total}]", end="")
        result = portal.get_metadata(uuid, raise_exception=False)
        if (isinstance(result_types := result.get("@type"), list) and
            result_types and (result_type := result_types[0])):  # noqa
            if verbose:
                _print(f" -> {result_type}")
            return result_type
        if verbose:
            _print()
        return None

    def get_metadata_types(path: str) -> Optional[dict]:
        nonlocal portal, debug
        metadata_types = {}
        try:
            if verbose:
                _print(f"Executing separted query to get actual metadata types for raw/inserts query.")
            if ((response := portal.get(path)) and (response.status_code in [200, 307]) and
                (response := response.json()) and (results := response.get("@graph"))):  # noqa
                for result in results:
                    if (result_type := result.get("@type")) and (result_uuid := result.get("uuid")):
                        if ((isinstance(result_type, list) and (result_type := result_type[0])) or
                            isinstance(result_type, str)):  # noqa
                            metadata_types[result_uuid] = result_type
        except Exception:
            return None
        return metadata_types

    def write_insert_files(response: dict) -> None:
        nonlocal insert_files, force
        output_directory = insert_files if isinstance(insert_files, str) else os.getcwd()
        for schema_name in response:
            schema_data = response[schema_name]
            file_name = f"{to_snake_case(schema_name)}.json"
            file_path = os.path.join(output_directory, file_name)
            message_verb = "Writing"
            if os.path.exists(file_path):
                message_verb = "Overwriting"
                if os.path.isdir(file_path):
                    _print(f"WARNING: Output file already exists as a directory. SKIPPING: {file_path}")
                    continue
                if not force:
                    _print(f"Output file already exists: {file_path}")
                    if not yes_or_no(f"Overwrite this file?"):
                        continue
            if verbose:
                _print(f"{message_verb} {schema_name} (object{'s' if len(schema_data) != 1 else ''}:"
                       f" {len(schema_data)}) file: {file_path}")
            with io.open(file_path, "w") as f:
                json.dump(schema_data, f, indent=4)

    if os.path.exists(uuid) and inserts:
        # Very special case: If given "uuid" (or other path) as actually a file then assume it
        # contains a list of references (e.g. /Donor/3039a6ca-9849-432d-ad49-2c5630bcbee7) to fetch.
        response = {}
        if verbose:
            _print(f"Reading references from file: {uuid}")
        with io.open(uuid) as f:
            for line in f:
                if ((line := line.strip()) and (components := line.split("/")) and (len(components) > 1) and
                    (schema_name := components[1]) and (schema_name := _get_schema(portal, schema_name)[1])):  # noqa
                    try:
                        if ((result := portal.get(line, raw=True, database=database)) and
                            (result.status_code in [200, 307]) and (result := result.json())):  # noqa
                            if not response.get(schema_name):
                                response[schema_name] = []
                            response[schema_name].append(result)
                            continue
                    except Exception:
                        pass
                    _print(f"Cannot get reference: {line}")
            if insert_files:
                write_insert_files(response)
            return response
    else:
        response = None
        try:
            if not uuid.startswith("/"):
                path = f"/{uuid}"
            else:
                path = uuid
            response = portal.get(path, raw=raw or inserts, database=database)
        except Exception as e:
            if "404" in str(e) and "not found" in str(e).lower():
                _print(f"Portal object not found at {portal.server}: {uuid}")
                _exit()
            _exit(f"Exception getting Portal object from {portal.server}: {uuid}\n{get_error_message(e)}")
        if not response:
            if check:
                return None
            _exit(f"Null response getting Portal object from {portal.server}: {uuid}")
        if response.status_code not in [200, 307]:
            # TODO: Understand why the /me endpoint returns HTTP status code 307, which is only why we mention it above.
            _exit(f"Invalid status code ({response.status_code}) getting Portal object from {portal.server}: {uuid}")
        if not response.json:
            _exit(f"Invalid JSON getting Portal object: {uuid}")
        response = response.json()

    response_types = {}
    if inserts:
        # Format results as suitable for inserts (e.g. via update-portal-object).
        response.pop("schema_version", None)
        if ((isinstance(results := response.get("@graph"), list) and results) and
            (isinstance(results_type := response.get("@type"), list) and results_type) and
            (isinstance(results_type := results_type[0], str) and results_type.endswith("SearchResults")) and
            (results_type := results_type[0:-len("SearchResults")])):  # noqa
            # For (raw frame) search results, the type (from XyzSearchResults, above) may not be precisely correct
            # for each of the results; it may be the supertype (e.g. QualityMetric vs QualityMetricWorkflowRun);
            # so for types which are supertypes (gotten via Portal.get_schemas_super_type_map) we actually
            # lookup each result individually to determine its actual precise type. Although, if we have
            # more than (say) 5 results to do this for, then do a separate query (get_metadata_types)
            # to get the result types all at once.
            if not ((supertypes := portal.get_schemas_super_type_map()) and (subtypes := supertypes.get(results_type))):
                subtypes = None
            response = {}
            results_index = 0
            results_total = len(results)
            for result in results:
                results_index += 1
                if debug:
                    print(f"Processing result: {results_index}")
                result.pop("schema_version", None)
                result = prune_data(result)
                if (subtypes and one_or_more_objects_of_types_exists(portal, subtypes, debug=debug) and
                    (result_uuid := result.get("uuid"))):  # noqa
                    # If we have more than (say) 5 results for which we need to determine that actual result type,
                    # then get them all at once via separate query (get_metadata_types)) which is not the raw frame.
                    if (results_total > 5) and (not response_types):
                        response_types = get_metadata_types(path)
                    if not (response_types and (result_type := response_types.get(result_uuid))):
                        if individual_result_type := get_metadata_for_individual_result_type(result_uuid):
                            result_type = individual_result_type
                        else:
                            result_type = results_type
                else:
                    result_type = results_type
                if response.get(result_type):
                    response[result_type].append(result)
                else:
                    response[result_type] = [result]
        # Get the result as non-raw so we can get its type.
        elif ((response_cooked := portal.get(path, database=database)) and
              (isinstance(response_type := response_cooked.json().get("@type"), list) and response_type)):
            response = {f"{response_type[0]}": [prune_data(response)]}
        if insert_files:
            write_insert_files(response)
#           output_directory = insert_files if isinstance(insert_files, str) else os.getcwd()
#           for schema_name in response:
#               schema_data = response[schema_name]
#               file_name = f"{to_snake_case(schema_name)}.json"
#               file_path = os.path.join(output_directory, file_name)
#               message_verb = "Writing"
#               if os.path.exists(file_path):
#                   message_verb = "Overwriting"
#                   if os.path.isdir(file_path):
#                       _print(f"WARNING: Output file already exists as a directory. SKIPPING: {file_path}")
#                       continue
#                   if not force:
#                       _print(f"Output file already exists: {file_path}")
#                       if not yes_or_no(f"Overwrite this file?"):
#                           continue
#               if verbose:
#                   _print(f"{message_verb} {schema_name} (object{'s' if len(schema_data) != 1 else ''}:"
#                          f" {len(schema_data)}) file: {file_path}")
#               with io.open(file_path, "w") as f:
#                   json.dump(schema_data, f, indent=4)
    elif raw:
        response.pop("schema_version", None)
    return response


def one_or_more_objects_of_types_exists(portal: Portal, schema_types: List[str], debug: bool = False) -> bool:
    for schema_type in schema_types:
        if one_or_more_objects_of_type_exists(portal, schema_type, debug=debug):
            return True
    return False


@lru_cache(maxsize=64)
def one_or_more_objects_of_type_exists(portal: Portal, schema_type: str, debug: bool = False) -> bool:
    try:
        if debug:
            _print(f"Checking if there are actually any objects of type: {schema_type}")
        if portal.get(f"/{schema_type}").status_code == 404:
            if debug:
                _print(f"No objects of type actually exist: {schema_type}")
            return False
        else:
            if debug:
                _print(f"One or more objects of type exist: {schema_type}")
    except Exception as e:
        _print(f"ERROR: Cannot determine if there are actually any objects of type: {schema_type}")
        _print(e)
    return True


@lru_cache(maxsize=1)
def _get_schemas(portal: Portal) -> Optional[dict]:
    return portal.get_schemas()


def _get_schema(portal: Portal, name: str) -> Tuple[Optional[dict], Optional[str]]:
    if portal and name and (name := name.replace("_", "").replace("-", "").strip().lower()):
        if schemas := _get_schemas(portal):
            for schema_name in schemas:
                if schema_name.replace("_", "").replace("-", "").strip().lower() == name.lower():
                    return schemas[schema_name], schema_name
    return None, None


def _is_maybe_schema_name(value: str) -> bool:
    if value and not is_uuid(value) and not value.startswith("/"):
        return True
    return False


def _is_schema_name(portal: Portal, value: str) -> bool:
    try:
        return _get_schema(portal, value)[0] is not None
    except Exception:
        return False


def _is_schema_named_json_file_name(portal: Portal, value: str) -> bool:
    try:
        return value.endswith(".json") and _is_schema_name(portal, os.path.basename(value[:-5]))
    except Exception:
        return False


def _get_schema_name_from_schema_named_json_file_name(portal: Portal, value: str) -> Optional[str]:
    try:
        if not value.endswith(".json"):
            return None
        _, schema_name = _get_schema(portal, os.path.basename(value[:-5]))
        return schema_name
    except Exception:
        return False


def _print_schema(schema: dict, terse: bool = False, all: bool = False,
                  summary: bool = False, yaml: bool = False) -> None:
    if summary is not True:
        if yaml:
            _print_output(yaml.dump(schema))
        else:
            _print_output(json.dumps(schema, indent=4))
        return
    _print_schema_info(schema, terse=terse, all=all)


def _print_schema_info(schema: dict, level: int = 0,
                       terse: bool = False, all: bool = False,
                       required: Optional[List[str]] = None) -> None:
    if not schema or not isinstance(schema, dict):
        return
    identifying_properties = schema.get("identifyingProperties")
    if level == 0:
        if required_properties := schema.get("required"):
            _print_output("- required properties:")
            for required_property in sorted(list(set(required_properties))):
                if not all and required_property in _SCHEMAS_IGNORE_PROPERTIES:
                    continue
                if property_type := (info := schema.get("properties", {}).get(required_property, {})).get("type"):
                    if property_type == "array" and (array_type := info.get("items", {}).get("type")):
                        _print_output(f"  - {required_property}: {property_type} of {array_type}")
                    else:
                        _print_output(f"  - {required_property}: {property_type}")
                else:
                    _print_output(f"  - {required_property}")
            if isinstance(any_of := schema.get("anyOf"), list):
                if ((any_of == [{"required": ["submission_centers"]}, {"required": ["consortia"]}]) or
                    (any_of == [{"required": ["consortia"]}, {"required": ["submission_centers"]}])):  # noqa
                    # Very very special case.
                    _print_output(f"  - at least one of:")
                    _print_output(f"    - consortia: array of string")
                    _print_output(f"    - submission_centers: array of string")
            required = required_properties
        if identifying_properties := schema.get("identifyingProperties"):
            _print_output("- identifying properties:")
            for identifying_property in sorted(list(set(identifying_properties))):
                if not all and identifying_property in _SCHEMAS_IGNORE_PROPERTIES:
                    continue
                if property_type := (info := schema.get("properties", {}).get(identifying_property, {})).get("type"):
                    if property_type == "array" and (array_type := info.get("items", {}).get("type")):
                        _print_output(f"  - {identifying_property}: {property_type} of {array_type}")
                    else:
                        _print_output(f"  - {identifying_property}: {property_type}")
                else:
                    _print_output(f"  - {identifying_property}")
        if properties := schema.get("properties"):
            reference_properties = []
            for property_name in properties:
                if not all and property_name in _SCHEMAS_IGNORE_PROPERTIES:
                    continue
                property = properties[property_name]
                if link_to := property.get("linkTo"):
                    reference_properties.append({"name": property_name, "ref": link_to})
            if reference_properties:
                _print_output("- reference properties:")
                for reference_property in sorted(reference_properties, key=lambda key: key["name"]):
                    _print_output(f"  - {reference_property['name']}: {reference_property['ref']}")
        if schema.get("additionalProperties") is True:
            _print_output(f"  - additional properties are allowed")
    if terse:
        return
    if properties := (schema.get("properties") if level == 0 else schema):
        if level == 0:
            _print_output("- properties:")
        for property_name in sorted(properties):
            if not all and property_name in _SCHEMAS_IGNORE_PROPERTIES:
                continue
            if property_name.startswith("@"):
                continue
            spaces = f"{' ' * (level + 1) * 2}"
            property = properties[property_name]
            property_required = required and property_name in required
            if property_type := property.get("type"):
                if property_type == "object":
                    suffix = ""
                    if not (object_properties := property.get("properties")):
                        if property.get("additionalProperties") is True:
                            property_type = "any object"
                        else:
                            property_type = "undefined object"
                    elif property.get("additionalProperties") is True:
                        property_type = "open ended object"
                    if property.get("calculatedProperty"):
                        suffix += f" | calculated"
                    _print_output(f"{spaces}- {property_name}: {property_type}{suffix}")
                    _print_schema_info(object_properties, level=level + 1, terse=terse, all=all,
                                       required=property.get("required"))
                elif property_type == "array":
                    suffix = ""
                    if property_required:
                        suffix += f" | required"
                    if property.get("uniqueItems"):
                        suffix += f" | unique"
                    if property.get("calculatedProperty"):
                        suffix += f" | calculated"
                    if property_items := property.get("items"):
                        if (enumeration := property_items.get("enum")) is not None:
                            suffix = f" | enum" + suffix
                        if pattern := property_items.get("pattern"):
                            suffix += f" | pattern: {pattern}"
                        if (format := property_items.get("format")) and (format != "uuid"):
                            suffix += f" | format: {format}"
                        if (max_length := property_items.get("maxLength")) is not None:
                            suffix += f" | max items: {max_length}"
                        if property_type := property_items.get("type"):
                            if property_type == "object":
                                suffix = ""
                                _print_output(f"{spaces}- {property_name}: array of object{suffix}")
                                _print_schema_info(property_items.get("properties"), level=level + 1,
                                                   terse=terse, all=all,
                                                   required=property_items.get("required"))
                            elif property_type == "array":
                                # This (array-of-array) never happens to occur at this time (February 2024).
                                _print_output(f"{spaces}- {property_name}: array of array{suffix}")
                            else:
                                _print_output(f"{spaces}- {property_name}: array of {property_type}{suffix}")
                        else:
                            _print_output(f"{spaces}- {property_name}: array{suffix}")
                    else:
                        _print_output(f"{spaces}- {property_name}: array{suffix}")
                    if enumeration:
                        nenums = 0
                        maxenums = 15
                        for enum in sorted(enumeration):
                            if (nenums := nenums + 1) >= maxenums:
                                if (remaining := len(enumeration) - nenums) > 0:
                                    _print_output(f"{spaces}  - [{remaining} more ...]")
                                break
                            _print_output(f"{spaces}  - {enum}")
                else:
                    if isinstance(property_type, list):
                        property_type = " or ".join(sorted(property_type))
                    suffix = ""
                    if (enumeration := property.get("enum")) is not None:
                        suffix += f" | enum"
                    if property_required:
                        suffix += f" | required"
                    if property_name in (identifying_properties or []):
                        suffix += f" | identifying"
                    if property.get("uniqueKey"):
                        suffix += f" | unique"
                    if pattern := property.get("pattern"):
                        suffix += f" | pattern: {pattern}"
                    if (format := property.get("format")) and (format != "uuid"):
                        suffix += f" | format: {format}"
                    if isinstance(any_of := property.get("anyOf"), list):
                        if ((any_of == [{"format": "date"}, {"format": "date-time"}]) or
                            (any_of == [{"format": "date-time"}, {"format": "date"}])):  # noqa
                            # Very special case.
                            suffix += f" | format: date or date-time"
                    if link_to := property.get("linkTo"):
                        suffix += f" | reference: {link_to}"
                    if property.get("calculatedProperty"):
                        suffix += f" | calculated"
                    if (default := property.get("default")) is not None:
                        suffix += f" | default:"
                        if isinstance(default, dict):
                            suffix += f" object"
                        elif isinstance(default, list):
                            suffix += f" array"
                        else:
                            suffix += f" {default}"
                    if (minimum := property.get("minimum")) is not None:
                        suffix += f" | min: {minimum}"
                    if (maximum := property.get("maximum")) is not None:
                        suffix += f" | max: {maximum}"
                    if (max_length := property.get("maxLength")) is not None:
                        suffix += f" | max length: {max_length}"
                    if (min_length := property.get("minLength")) is not None:
                        suffix += f" | min length: {min_length}"
                    _print_output(f"{spaces}- {property_name}: {property_type}{suffix}")
                    if enumeration:
                        nenums = 0
                        maxenums = 15
                        for enum in sorted(enumeration):
                            if (nenums := nenums + 1) >= maxenums:
                                if (remaining := len(enumeration) - nenums) > 0:
                                    _print_output(f"{spaces}  - [{remaining} more ...]")
                                break
                            _print_output(f"{spaces}  - {enum}")
            else:
                _print_output(f"{spaces}- {property_name}")


def _print_all_schema_names(portal: Portal,
                            terse: bool = False, all: bool = False,
                            tree: bool = False, summary: bool = False, yaml: bool = False) -> None:
    if not (schemas := _get_schemas(portal)):
        return

    if summary is not True:
        if yaml:
            _print_output(yaml.dump(schemas))
        else:
            _print_output(json.dumps(schemas, indent=4))
        return

    if tree:
        _print_schemas_tree(schemas)
        return

    for schema_name in sorted(schemas.keys()):
        if parent_schema_name := _get_parent_schema_name(schemas[schema_name]):
            if schemas[schema_name].get("isAbstract") is True:
                _print_output(f"{schema_name} | parent: {parent_schema_name} | abstract")
            else:
                _print_output(f"{schema_name} | parent: {parent_schema_name}")
        else:
            if schemas[schema_name].get("isAbstract") is True:
                _print_output(f"{schema_name} | abstract")
            else:
                _print_output(schema_name)
        if not terse:
            _print_schema(schemas[schema_name], terse=terse, all=all)


def _get_parent_schema_name(schema: dict) -> Optional[str]:
    if (isinstance(schema, dict) and
        (parent_schema_name := schema.get("rdfs:subClassOf")) and
        (parent_schema_name := parent_schema_name.replace("/profiles/", "").replace(".json", "")) and
        (parent_schema_name != "Item")):  # noqa
        return parent_schema_name
    return None


def _print_schemas_tree(schemas: dict) -> None:
    def children_of(name: str) -> List[str]:
        nonlocal schemas
        children = []
        if not (name is None or isinstance(name, str)):
            return children
        if name and name.lower() == "schemas":
            name = None
        for schema_name in (schemas if isinstance(schemas, dict) else {}):
            if _get_parent_schema_name(schemas[schema_name]) == name:
                children.append(schema_name)
        return sorted(children)
    def name_of(name: str) -> str:  # noqa
        nonlocal schemas
        if not (name is None or isinstance(name, str)):
            return name
        if (schema := schemas.get(name)) and schema.get("isAbstract") is True:
            return f"{name} (abstact)"
        return name
    _print_tree(root_name="Schemas", children_of=children_of, name_of=name_of)


def _print_tree(root_name: Optional[str],
                children_of: Callable,
                has_children: Optional[Callable] = None,
                name_of: Optional[Callable] = None) -> None:
    """
    Recursively prints as a tree structure the given root name and any of its
    children (again, recursively) as specified by the given children_of callable;
    the has_children may be specified, for efficiency, though if not specified
    it will use the children_of function to determine this; the name_of callable
    may be specified to modify the name before printing.
    """
    first = "└─ "
    space = "    "
    branch = "│   "
    tee = "├── "
    last = "└── "

    if not callable(children_of):
        return
    if not callable(has_children):
        has_children = lambda name: children_of(name) is not None  # noqa

    # This function adapted from stackoverflow.
    # Ref: https://stackoverflow.com/questions/9727673/list-directory-tree-structure-in-python
    def tree_generator(name: str, prefix: str = ""):
        contents = children_of(name)
        pointers = [tee] * (len(contents) - 1) + [last]
        for pointer, path in zip(pointers, contents):
            yield prefix + pointer + (name_of(path) if callable(name_of) else path)
            if has_children(path):
                extension = branch if pointer == tee else space
                yield from tree_generator(path, prefix=prefix+extension)
    _print_output(first + ((name_of(root_name) if callable(name_of) else root_name) or "root"))
    for line in tree_generator(root_name, prefix="   "):
        _print_output(line)


def _read_json_from_file(file: str) -> Optional[dict]:
    if not os.path.exists(file):
        _print(f"Cannot find file: {file}")
        _exit(1)
    try:
        with io.open(file, "r") as f:
            try:
                return json.load(f)
            except Exception:
                _print(f"Cannot parse JSON in file: {file}")
                _exit(1)
    except Exception as e:
        _print(e)
        _print(f"Cannot open file: {file}")
        _exit(1)


def _print(*args, **kwargs):
    with uncaptured_output():
        PRINT(*args, **kwargs)
    sys.stdout.flush()


def _print_output(value: str):
    global _output_file
    if _output_file:
        _output_file.write(value)
        _output_file.write("\n")
    else:
        with uncaptured_output():
            PRINT(value)
        sys.stdout.flush()


def _exit(message: Optional[Union[str, int]] = None, status: Optional[int] = None) -> None:
    global _output_file
    if isinstance(message, str):
        _print(f"ERROR: {message}")
    elif isinstance(message, int) and not isinstance(status, int):
        status = message
    if _output_file:
        _output_file.close()
    sys.exit(status if isinstance(status, int) else (0 if status is None else 1))


if __name__ == "__main__":
    main()
