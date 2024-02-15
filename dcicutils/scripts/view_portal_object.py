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
import json
import pyperclip
import sys
from typing import Optional
import yaml
from dcicutils.captured_output import captured_output, uncaptured_output
from dcicutils.misc_utils import get_error_message
from dcicutils.portal_utils import Portal
from dcicutils.structured_data import Schema


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
    parser.add_argument("--verbose", action="store_true", required=False, default=False, help="Verbose output.")
    parser.add_argument("--debug", action="store_true", required=False, default=False, help="Debugging output.")
    args = parser.parse_args()

    portal = _create_portal(ini=args.ini, env=args.env, server=args.server, app=args.app, debug=args.debug)
    if args.uuid == "schemas":
        _print_all_schema_names(portal=portal, verbose=args.verbose)
        return
    elif args.schema:
        data = _get_schema(portal=portal, schema_name=args.uuid)
    else:
        data = _get_portal_object(portal=portal, uuid=args.uuid, raw=args.raw,
                                  database=args.database, verbose=args.verbose)

    if args.copy:
        pyperclip.copy(json.dumps(data, indent=4))
    if args.yaml:
        _print(yaml.dump(data))
    else:
        _print(json.dumps(data, default=str, indent=4))


def _create_portal(ini: str, env: Optional[str] = None,
                   server: Optional[str] = None, app: Optional[str] = None, debug: bool = False) -> Portal:
    with captured_output(not debug):
        return Portal(env, server=server, app=app) if env or app else Portal(ini)


def _get_portal_object(portal: Portal, uuid: str,
                       raw: bool = False, database: bool = False, verbose: bool = False) -> dict:
    if verbose:
        _print(f"Getting object from Portal: {uuid}")
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
    response = None
    try:
        if not uuid.startswith("/"):
            path = f"/{uuid}"
        else:
            path = uuid
        response = portal.get(path, raw=raw, database=database)
    except Exception as e:
        if "404" in str(e) and "not found" in str(e).lower():
            _print(f"Portal object not found: {uuid}")
            _exit_without_action()
        _exit_without_action(f"Exception getting Portal object: {uuid}\n{get_error_message(e)}")
    if not response:
        _exit_without_action(f"Null response getting Portal object: {uuid}")
    if response.status_code not in [200, 307]:
        # TODO: Understand why the /me endpoint returns HTTP status code 307, which is only why we mention it above.
        _exit_without_action(f"Invalid status code ({response.status_code}) getting Portal object: {uuid}")
    if not response.json:
        _exit_without_action(f"Invalid JSON getting Portal object: {uuid}")
    if verbose:
        _print("OK")
    return response.json()


def _get_schema(portal: Portal, schema_name: str) -> Optional[dict]:
    def rummage_for_schema_name(portal: Portal, schema_name: str) -> Optional[str]:  # noqa
        if schemas := portal.get_schemas():
            for schema in schemas:
                if schema.lower() == schema_name.lower():
                    return schema
    schema = Schema.load_by_name(schema_name, portal)
    if not schema:
        if schema_name := rummage_for_schema_name(portal, schema_name):
            schema = Schema.load_by_name(schema_name, portal)
    return schema.data if schema else None


def _print_all_schema_names(portal: Portal, verbose: bool = False) -> None:
    if schemas := portal.get_schemas():
        for schema in sorted(schemas.keys()):
            _print(schema)
            if verbose:
                if identifying_properties := schemas[schema].get("identifyingProperties"):
                    _print("- identifying properties:")
                    for identifying_property in sorted(identifying_properties):
                        _print(f"  - {identifying_property}")
                if required_properties := schemas[schema].get("required"):
                    _print("- required properties:")
                    for required_property in sorted(required_properties):
                        _print(f"  - {required_property}")


def _print(*args, **kwargs):
    with uncaptured_output():
        print(*args, **kwargs)
    sys.stdout.flush()


def _exit_without_action(message: Optional[str] = None) -> None:
    if message:
        _print(f"ERROR: {message}")
    exit(1)


if __name__ == "__main__":
    main()
