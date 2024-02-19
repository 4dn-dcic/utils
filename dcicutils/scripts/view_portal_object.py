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
import sys
from typing import List, Optional, Tuple
import yaml
from dcicutils.captured_output import captured_output, uncaptured_output
from dcicutils.misc_utils import get_error_message, is_uuid, PRINT
from dcicutils.portal_utils import Portal


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
    parser.add_argument("--raw", action="store_true", required=False, default=False, help="Raw output.")
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

    portal = _create_portal(ini=args.ini, env=args.env, server=args.server,
                            app=args.app, verbose=args.verbose, debug=args.debug)

    if args.uuid.lower() == "schemas" or args.uuid.lower() == "schema":
        _print_all_schema_names(portal=portal, details=args.details, more_details=args.more_details, raw=args.raw)
        return

    if _is_maybe_schema_name(args.uuid):
        args.schema = True

    if args.schema:
        schema, schema_name = _get_schema(portal, args.uuid)
        if schema:
            if args.copy:
                pyperclip.copy(json.dumps(schema, indent=4))
            if not args.raw:
                _print(schema_name)
            _print_schema(schema, details=args.details, more_details=args.details, raw=args.raw)
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


def _print_schema(schema: dict, details: bool = False, more_details: bool = False, raw: bool = False) -> None:
    if raw:
        _print(json.dumps(schema, indent=4))
        return
    _print_schema_info(schema, details=details, more_details=more_details)


def _print_schema_info(schema: dict, level: int = 0,
                       details: bool = False, more_details: bool = False,
                       required: Optional[List[str]] = None) -> None:
    if not schema or not isinstance(schema, dict):
        return
    if level == 0:
        if required_properties := schema.get("required"):
            _print("- required properties:")
            for required_property in sorted(list(set(required_properties))):
                if property_type := (info := schema.get("properties", {}).get(required_property, {})).get("type"):
                    if property_type == "array" and (array_type := info.get("items", {}).get("type")):
                        _print(f"  - {required_property}: {property_type} of {array_type}")
                    else:
                        _print(f"  - {required_property}: {property_type}")
                else:
                    _print(f"  - {required_property}")
            required = required_properties
        if identifying_properties := schema.get("identifyingProperties"):
            _print("- identifying properties:")
            for identifying_property in sorted(list(set(identifying_properties))):
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
                property = properties[property_name]
                if link_to := property.get("linkTo"):
                    reference_properties.append({"name": property_name, "ref": link_to})
            if reference_properties:
                _print("- reference properties:")
                for reference_property in sorted(reference_properties, key=lambda key: key["name"]):
                    _print(f"  - {reference_property['name']}: {reference_property['ref']}")
        if schema.get("additionalProperties") is True:
            _print(f"  - additional properties are allowed")
            pass
    if not more_details:
        return
    if properties := (schema.get("properties") if level == 0 else schema):
        if level == 0:
            _print("- properties:")
        for property_name in sorted(properties):
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
                    _print(f"{spaces}- {property_name}: {property_type}{suffix}")
                    _print_schema_info(object_properties, level=level + 1,
                                       details=details, more_details=more_details,
                                       required=property.get("required"))
                elif property_type == "array":
                    suffix = ""
                    if property_required:
                        suffix += f" | required"
                    if property_items := property.get("items"):
                        if property_type := property_items.get("type"):
                            if property_type == "object":
                                suffix = ""
                                _print(f"{spaces}- {property_name}: array of object{suffix}")
                                _print_schema_info(property_items.get("properties"),
                                                   details=details, more_details=more_details, level=level + 1)
                            elif property_type == "array":
                                # This (array-of-array) never happens to occur at this time (February 2024).
                                _print(f"{spaces}- {property_name}: array of array{suffix}")
                            else:
                                _print(f"{spaces}- {property_name}: array of {property_type}{suffix}")
                        else:
                            _print(f"{spaces}- {property_name}: array{suffix}")
                    else:
                        _print(f"{spaces}- {property_name}: array{suffix}")
                else:
                    if isinstance(property_type, list):
                        property_type = " | ".join(property_type)
                    suffix = ""
                    if (enumeration := property.get("enum")) is not None:
                        suffix += f" | enum"
                    if property_required:
                        suffix += f" | required"
                    if pattern := property.get("pattern"):
                        suffix += f" | pattern: {pattern}"
                    if link_to := property.get("linkTo"):
                        suffix += f" | reference: {link_to}"
                    if property.get("calculatedProperty"):
                        suffix += f" | calculated"
                    if default := property.get("default"):
                        suffix += f" | default:"
                        if isinstance(default, dict):
                            suffix += f" object"
                        elif isinstance(default, list):
                            suffix += f" array"
                        else:
                            suffix += f" {default}"
                    _print(f"{spaces}- {property_name}: {property_type}{suffix}")
                    if enumeration:
                        nenums = 0
                        maxenums = 15
                        for enum in enumeration:
                            if (nenums := nenums + 1) >= maxenums:
                                if (remaining := len(enumeration) - nenums) > 0:
                                    _print(f"{spaces}  - [{remaining} more ...]")
                                break
                            _print(f"{spaces}  - {enum}")
            else:
                _print(f"{spaces}- {property_name}")


def _print_all_schema_names(portal: Portal,
                            details: bool = False, more_details: bool = False,
                            raw: bool = False) -> None:
    if schemas := _get_schemas(portal):
        if raw:
            _print(json.dumps(schemas, indent=4))
            return
        for schema in sorted(schemas.keys()):
            _print(schema)
            if details:
                _print_schema(schemas[schema], details=details, more_details=more_details)


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
