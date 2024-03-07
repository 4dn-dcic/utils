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
import json
import pyperclip
import os
import sys
from typing import Callable, List, Optional, Tuple
import yaml
from dcicutils.captured_output import captured_output, uncaptured_output
from dcicutils.misc_utils import get_error_message, is_uuid, PRINT
from dcicutils.portal_utils import Portal


# Schema properties to ignore (by default) for the view schema usage.
_SCHEMAS_IGNORE_PROPERTIES = [
    "date_created",
    "last_modified",
    "principals_allowed",
    "submitted_by",
    "schema_version"
]


def main():

    parser = argparse.ArgumentParser(description="View Portal object.")
    parser.add_argument("uuid", type=str,
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
    parser.add_argument("--tree", action="store_true", required=False, default=False, help="Tree output for schemas.")
    parser.add_argument("--database", action="store_true", required=False, default=False,
                        help="Read from database output.")
    parser.add_argument("--yaml", action="store_true", required=False, default=False, help="YAML output.")
    parser.add_argument("--copy", "-c", action="store_true", required=False, default=False,
                        help="Copy object data to clipboard.")
    parser.add_argument("--details", action="store_true", required=False, default=False, help="Detailed output.")
    parser.add_argument("--more-details", action="store_true", required=False, default=False,
                        help="More detailed output.")
    parser.add_argument("--verbose", action="store_true", required=False, default=False, help="Verbose output.")
    parser.add_argument("--debug", action="store_true", required=False, default=False, help="Debugging output.")
    args = parser.parse_args()

    if args.more_details:
        args.details = True

    portal = _create_portal(ini=args.ini, env=args.env or os.environ.get("SMAHT_ENV"),
                            server=args.server, app=args.app, verbose=args.verbose, debug=args.debug)

    if args.uuid.lower() == "schemas" or args.uuid.lower() == "schema":
        _print_all_schema_names(portal=portal, details=args.details,
                                more_details=args.more_details, all=args.all,
                                tree=args.tree, raw=args.raw, raw_yaml=args.yaml)
        return
    elif args.uuid.lower() == "info":  # TODO: need word for what consortiums and submission centers are collectively
        if consortia := portal.get_metadata("/consortia?limit=1000"):
            _print("Known Consortia:")
            consortia = sorted(consortia.get("@graph", []), key=lambda key: key.get("identifier"))
            for consortium in consortia:
                if ((consortium_name := consortium.get("identifier")) and
                    (consortium_uuid := consortium.get("uuid"))):  # noqa
                    _print(f"- {consortium_name}: {consortium_uuid}")
        if submission_centers := portal.get_metadata("/submission-centers?limit=1000"):
            _print("Known Submission Centers:")
            submission_centers = sorted(submission_centers.get("@graph", []), key=lambda key: key.get("identifier"))
            for submission_center in submission_centers:
                if ((submission_center_name := submission_center.get("identifier")) and
                    (submission_center_uuid := submission_center.get("uuid"))):  # noqa
                    _print(f"- {submission_center_name}: {submission_center_uuid}")
        try:
            if file_formats := portal.get_metadata("/file-formats?limit=1000"):
                _print("Known File Formats:")
                file_formats = sorted(file_formats.get("@graph", []), key=lambda key: key.get("identifier"))
                for file_format in file_formats:
                    if ((file_format_name := file_format.get("identifier")) and
                        (file_format_uuid := file_format.get("uuid"))):  # noqa
                        _print(f"- {file_format_name}: {file_format_uuid}")
        except Exception:
            _print("Known File Formats: None")
        return

    if _is_maybe_schema_name(args.uuid):
        args.schema = True

    if args.schema:
        schema, schema_name = _get_schema(portal, args.uuid)
        if schema:
            if args.copy:
                pyperclip.copy(json.dumps(schema, indent=4))
            if not args.raw:
                if parent_schema_name := _get_parent_schema_name(schema):
                    if schema.get("isAbstract") is True:
                        _print(f"{schema_name} | parent: {parent_schema_name} | abstract")
                    else:
                        _print(f"{schema_name} | parent: {parent_schema_name}")
                else:
                    _print(schema_name)
            _print_schema(schema, details=args.details, more_details=args.details,
                          all=args.all, raw=args.raw, raw_yaml=args.yaml)
            return

    data = _get_portal_object(portal=portal, uuid=args.uuid, raw=args.raw, database=args.database, verbose=args.verbose)
    if args.copy:
        pyperclip.copy(json.dumps(data, indent=4))
    if args.yaml:
        _print(yaml.dump(data))
    else:
        _print(json.dumps(data, default=str, indent=4))


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
                       raw: bool = False, database: bool = False, verbose: bool = False) -> dict:
    response = None
    try:
        if not uuid.startswith("/"):
            path = f"/{uuid}"
        else:
            path = uuid
        response = portal.get(path, raw=raw, database=database)
    except Exception as e:
        if "404" in str(e) and "not found" in str(e).lower():
            _print(f"Portal object not found at {portal.server}: {uuid}")
            _exit()
        _exit(f"Exception getting Portal object from {portal.server}: {uuid}\n{get_error_message(e)}")
    if not response:
        _exit(f"Null response getting Portal object from {portal.server}: {uuid}")
    if response.status_code not in [200, 307]:
        # TODO: Understand why the /me endpoint returns HTTP status code 307, which is only why we mention it above.
        _exit(f"Invalid status code ({response.status_code}) getting Portal object from {portal.server}: {uuid}")
    if not response.json:
        _exit(f"Invalid JSON getting Portal object: {uuid}")
    return response.json()


@lru_cache(maxsize=1)
def _get_schemas(portal: Portal) -> Optional[dict]:
    return portal.get_schemas()


def _get_schema(portal: Portal, name: str) -> Tuple[Optional[dict], Optional[str]]:
    if portal and name and (name := name.replace("_", "").replace("-", "").strip().lower()):
        if schemas := _get_schemas(portal):
            for schema_name in schemas:
                if schema_name.replace("_", "").replace("-", "").strip().lower() == name:
                    return schemas[schema_name], schema_name
    return None, None


def _is_maybe_schema_name(value: str) -> bool:
    if value and not is_uuid(value) and not value.startswith("/"):
        return True
    return False


def _print_schema(schema: dict, details: bool = False, more_details: bool = False, all: bool = False,
                  raw: bool = False, raw_yaml: bool = False) -> None:
    if raw:
        if raw_yaml:
            _print(yaml.dump(schema))
        else:
            _print(json.dumps(schema, indent=4))
        return
    _print_schema_info(schema, details=details, more_details=more_details, all=all)


def _print_schema_info(schema: dict, level: int = 0,
                       details: bool = False, more_details: bool = False, all: bool = False,
                       required: Optional[List[str]] = None) -> None:
    if not schema or not isinstance(schema, dict):
        return
    if level == 0:
        if required_properties := schema.get("required"):
            _print("- required properties:")
            for required_property in sorted(list(set(required_properties))):
                if not all and required_property in _SCHEMAS_IGNORE_PROPERTIES:
                    continue
                if property_type := (info := schema.get("properties", {}).get(required_property, {})).get("type"):
                    if property_type == "array" and (array_type := info.get("items", {}).get("type")):
                        _print(f"  - {required_property}: {property_type} of {array_type}")
                    else:
                        _print(f"  - {required_property}: {property_type}")
                else:
                    _print(f"  - {required_property}")
            if isinstance(any_of := schema.get("anyOf"), list):
                if ((any_of == [{"required": ["submission_centers"]}, {"required": ["consortia"]}]) or
                    (any_of == [{"required": ["consortia"]}, {"required": ["submission_centers"]}])):  # noqa
                    # Very very special case.
                    _print(f"  - at least one of:")
                    _print(f"    - consortia: array of string")
                    _print(f"    - submission_centers: array of string")
            required = required_properties
        if identifying_properties := schema.get("identifyingProperties"):
            _print("- identifying properties:")
            for identifying_property in sorted(list(set(identifying_properties))):
                if not all and identifying_property in _SCHEMAS_IGNORE_PROPERTIES:
                    continue
                if property_type := (info := schema.get("properties", {}).get(identifying_property, {})).get("type"):
                    if property_type == "array" and (array_type := info.get("items", {}).get("type")):
                        _print(f"  - {identifying_property}: {property_type} of {array_type}")
                    else:
                        _print(f"  - {identifying_property}: {property_type}")
                else:
                    _print(f"  - {identifying_property}")
        if properties := schema.get("properties"):
            reference_properties = []
            for property_name in properties:
                if not all and property_name in _SCHEMAS_IGNORE_PROPERTIES:
                    continue
                property = properties[property_name]
                if link_to := property.get("linkTo"):
                    reference_properties.append({"name": property_name, "ref": link_to})
            if reference_properties:
                _print("- reference properties:")
                for reference_property in sorted(reference_properties, key=lambda key: key["name"]):
                    _print(f"  - {reference_property['name']}: {reference_property['ref']}")
        if schema.get("additionalProperties") is True:
            _print(f"  - additional properties are allowed")
    if not more_details:
        return
    if properties := (schema.get("properties") if level == 0 else schema):
        if level == 0:
            _print("- properties:")
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
                    _print(f"{spaces}- {property_name}: {property_type}{suffix}")
                    _print_schema_info(object_properties, level=level + 1,
                                       details=details, more_details=more_details, all=all,
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
                                _print(f"{spaces}- {property_name}: array of object{suffix}")
                                _print_schema_info(property_items.get("properties"), level=level + 1,
                                                   details=details, more_details=more_details, all=all,
                                                   required=property_items.get("required"))
                            elif property_type == "array":
                                # This (array-of-array) never happens to occur at this time (February 2024).
                                _print(f"{spaces}- {property_name}: array of array{suffix}")
                            else:
                                _print(f"{spaces}- {property_name}: array of {property_type}{suffix}")
                        else:
                            _print(f"{spaces}- {property_name}: array{suffix}")
                    else:
                        _print(f"{spaces}- {property_name}: array{suffix}")
                    if enumeration:
                        nenums = 0
                        maxenums = 15
                        for enum in sorted(enumeration):
                            if (nenums := nenums + 1) >= maxenums:
                                if (remaining := len(enumeration) - nenums) > 0:
                                    _print(f"{spaces}  - [{remaining} more ...]")
                                break
                            _print(f"{spaces}  - {enum}")
                else:
                    if isinstance(property_type, list):
                        property_type = " or ".join(sorted(property_type))
                    suffix = ""
                    if (enumeration := property.get("enum")) is not None:
                        suffix += f" | enum"
                    if property_required:
                        suffix += f" | required"
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
                    _print(f"{spaces}- {property_name}: {property_type}{suffix}")
                    if enumeration:
                        nenums = 0
                        maxenums = 15
                        for enum in sorted(enumeration):
                            if (nenums := nenums + 1) >= maxenums:
                                if (remaining := len(enumeration) - nenums) > 0:
                                    _print(f"{spaces}  - [{remaining} more ...]")
                                break
                            _print(f"{spaces}  - {enum}")
            else:
                _print(f"{spaces}- {property_name}")


def _print_all_schema_names(portal: Portal,
                            details: bool = False, more_details: bool = False, all: bool = False,
                            tree: bool = False, raw: bool = False, raw_yaml: bool = False) -> None:
    if not (schemas := _get_schemas(portal)):
        return

    if raw:
        if raw_yaml:
            _print(yaml.dump(schemas))
        else:
            _print(json.dumps(schemas, indent=4))
        return

    if tree:
        _print_schemas_tree(schemas)
        return

    for schema_name in sorted(schemas.keys()):
        if parent_schema_name := _get_parent_schema_name(schemas[schema_name]):
            if schemas[schema_name].get("isAbstract") is True:
                _print(f"{schema_name} | parent: {parent_schema_name} | abstract")
            else:
                _print(f"{schema_name} | parent: {parent_schema_name}")
        else:
            if schemas[schema_name].get("isAbstract") is True:
                _print(f"{schema_name} | abstract")
            else:
                _print(schema_name)
        if details:
            _print_schema(schemas[schema_name], details=details, more_details=more_details, all=all)


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
                name_of: Optional[Callable] = None,
                print: Callable = print) -> None:
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
    print(first + ((name_of(root_name) if callable(name_of) else root_name) or "root"))
    for line in tree_generator(root_name, prefix="   "):
        print(line)


def _print(*args, **kwargs):
    with uncaptured_output():
        PRINT(*args, **kwargs)
    sys.stdout.flush()


def _exit(message: Optional[str] = None) -> None:
    if message:
        _print(f"ERROR: {message}")
    exit(1)


if __name__ == "__main__":
    main()
