# ------------------------------------------------------------------------------------------------------
# Command-line utility to update (post, patch, upsert) portal objects for SMaHT/CGAP/Fourfront.
# ------------------------------------------------------------------------------------------------------
# Example commands:
# update-portal-object --post file_format.json
# update-portal-object --upsert directory-with-schema-named-dot-json-files
# update-portal-object --patch file-not-named-for-schema-name.json --schema UnalignedReads
# --------------------------------------------------------------------------------------------------

import argparse
from functools import lru_cache
import glob
import io
import json
import os
import sys
from typing import Callable, List, Optional, Tuple, Union
from dcicutils.command_utils import yes_or_no
from dcicutils.common import ORCHESTRATED_APPS, APP_SMAHT
from dcicutils.ff_utils import delete_metadata, purge_metadata
from dcicutils.misc_utils import get_error_message, PRINT
from dcicutils.portal_utils import Portal as PortalFromUtils


class Portal(PortalFromUtils):

    def delete_metadata(self, object_id: str) -> Optional[dict]:
        if isinstance(object_id, str) and object_id and self.key:
            return delete_metadata(obj_id=object_id, key=self.key)
        return None

    def purge_metadata(self, object_id: str) -> Optional[dict]:
        if isinstance(object_id, str) and object_id and self.key:
            return purge_metadata(obj_id=object_id, key=self.key)
        return None


_DEFAULT_APP = "smaht"
_SMAHT_ENV_ENVIRON_NAME = "SMAHT_ENV"

# Schema properties to ignore (by default) for the view schema usage.
_SCHEMAS_IGNORE_PROPERTIES = [
    "date_created",
    "last_modified",
    "principals_allowed",
    "submitted_by",
    "schema_version"
]

_SCHEMA_ORDER = [  # See: smaht-portal/src/encoded/project/loadxl.py
    "access_key",
    "user",
    "consortium",
    "submission_center",
    "file_format",
    "quality_metric",
    "output_file",
    "reference_file",
    "reference_genome",
    "software",
    "tracking_item",
    "workflow",
    "workflow_run",
    "meta_workflow",
    "meta_workflow_run",
    "image",
    "document",
    "static_section",
    "page",
    "filter_set",
    "higlass_view_config",
    "ingestion_submission",
    "ontology_term",
    "protocol",
    "donor",
    "demographic",
    "medical_history",
    "diagnosis",
    "exposure",
    "family_history",
    "medical_treatment",
    "death_circumstances",
    "tissue_collection",
    "tissue",
    "histology",
    "cell_line",
    "cell_culture",
    "cell_culture_mixture",
    "preparation_kit",
    "treatment",
    "sample_preparation",
    "tissue_sample",
    "cell_culture_sample",
    "cell_sample",
    "analyte",
    "analyte_preparation",
    "assay",
    "library",
    "library_preparation",
    "sequencer",
    "basecalling",
    "sequencing",
    "file_set",
    "unaligned_reads",
    "aligned_reads",
    "variant_calls",
]


def main():

    parser = argparse.ArgumentParser(description="View Portal object.")
    parser.add_argument("--env", "-e", type=str, required=False, default=None,
                        help=f"Environment name (key from ~/.smaht-keys.json).")
    parser.add_argument("--app", type=str, required=False, default=None,
                        help=f"Application name (one of: smaht, cgap, fourfront).")
    parser.add_argument("--schema", type=str, required=False, default=None,
                        help="Use named schema rather than infer from post/patch/upsert file name.")
    parser.add_argument("--post", type=str, required=False, default=None, help="POST data.")
    parser.add_argument("--patch", type=str, required=False, default=None, help="PATCH data.")
    parser.add_argument("--upsert", type=str, required=False, default=None, help="Upsert data.")
    parser.add_argument("--delete", type=str, required=False, default=None, help="Delete data.")
    parser.add_argument("--purge", type=str, required=False, default=None, help="Purge data.")
    parser.add_argument("--confirm", action="store_true", required=False, default=False, help="Confirm before action.")
    parser.add_argument("--verbose", action="store_true", required=False, default=False, help="Verbose output.")
    parser.add_argument("--quiet", action="store_true", required=False, default=False, help="Quiet output.")
    parser.add_argument("--debug", action="store_true", required=False, default=False, help="Debugging output.")
    args = parser.parse_args()

    def usage(message: Optional[str] = None) -> None:
        nonlocal parser
        _print(message) if isinstance(message, str) else None
        parser.print_help()
        sys.exit(1)

    if app := args.app:
        if (app not in ORCHESTRATED_APPS) and ((app := app.lower()) not in ORCHESTRATED_APPS):
            usage(f"ERROR: Unknown app name; must be one of: {' | '.join(ORCHESTRATED_APPS)}")
    else:
        app = APP_SMAHT

    portal = _create_portal(env=args.env, app=app, verbose=args.verbose, debug=args.debug)

    if explicit_schema_name := args.schema:
        schema, explicit_schema_name = _get_schema(portal, explicit_schema_name)
        if not schema:
            usage(f"ERROR: Unknown schema name: {args.schema}")

    if not (args.post or args.patch or args.upsert or args.delete or args.purge):
        usage()

    if args.post:
        _post_or_patch_or_upsert(portal=portal,
                                 file_or_directory=args.post,
                                 explicit_schema_name=explicit_schema_name,
                                 update_function=post_data,
                                 update_action_name="POST",
                                 confirm=args.confirm, verbose=args.verbose, quiet=args.quiet, debug=args.debug)
    if args.patch:
        _post_or_patch_or_upsert(portal=portal,
                                 file_or_directory=args.patch,
                                 explicit_schema_name=explicit_schema_name,
                                 update_function=patch_data,
                                 update_action_name="PATCH",
                                 confirm=args.confirm, verbose=args.verbose, quiet=args.quiet, debug=args.debug)
    if args.upsert:
        _post_or_patch_or_upsert(portal=portal,
                                 file_or_directory=args.upsert,
                                 explicit_schema_name=explicit_schema_name,
                                 update_function=upsert_data,
                                 update_action_name="UPSERT",
                                 confirm=args.confirm, verbose=args.verbose, quiet=args.quiet, debug=args.debug)

    if args.delete:
        if not portal.get_metadata(args.delete, raise_exception=False):
            _print(f"Cannot find given object: {args.delete}")
            sys.exit(1)
        if yes_or_no(f"Do you really want to delete this item: {args.delete} ?"):
            portal.delete_metadata(args.delete)

    if args.purge:
        if not portal.get_metadata(args.purge, raise_exception=False):
            _print(f"Cannot find given object: {args.purge}")
            sys.exit(1)
        if yes_or_no(f"Do you really want to purge this item: {args.purge} ?"):
            portal.delete_metadata(args.purge)
            portal.purge_metadata(args.purge)


def _post_or_patch_or_upsert(portal: Portal, file_or_directory: str,
                             explicit_schema_name: str,
                             update_function: Callable, update_action_name: str,
                             confirm: bool = False, verbose: bool = False,
                             quiet: bool = False, debug: bool = False) -> None:

    def is_schema_name_list(portal: Portal, keys: list) -> bool:
        if isinstance(keys, list):
            for key in keys:
                if portal.get_schema(key) is None:
                    return False
                return True
        return False

    def post_or_patch_or_upsert(portal: Portal, file: str, schema_name: Optional[str],
                                confirm: bool = False, verbose: bool = False,
                                quiet: bool = False, debug: bool = False) -> None:

        nonlocal update_function, update_action_name
        if not quiet:
            _print(f"Processing {update_action_name} file: {file}")
        if data := _read_json_from_file(file):
            if isinstance(data, dict):
                if isinstance(schema_name, str) and schema_name:
                    if debug:
                        _print(f"DEBUG: File ({file}) contains an object of type: {schema_name}")
                    update_function(portal, data, schema_name, confirm=confirm,
                                    file=file, verbose=verbose, debug=debug)
                elif is_schema_name_list(portal, list(data.keys())):
                    if debug:
                        _print(f"DEBUG: File ({file}) contains a dictionary of schema names.")
                    for schema_name in data:
                        if isinstance(schema_data := data[schema_name], list):
                            if debug:
                                _print(f"DEBUG: Processing {update_action_name}s for type: {schema_name}")
                            for index, item in enumerate(schema_data):
                                update_function(portal, item, schema_name, confirm=confirm,
                                                file=file, index=index, verbose=verbose, debug=debug)
                        else:
                            _print(f"WARNING: File ({file}) contains schema item which is not a list: {schema_name}")
                else:
                    _print(f"WARNING: File ({file}) contains unknown item type.")
            elif isinstance(data, list):
                if debug:
                    _print(f"DEBUG: File ({file}) contains a list of objects of type: {schema_name}")
                for index, item in enumerate(data):
                    update_function(portal, item, schema_name, confirm=confirm,
                                    file=file, index=index, verbose=verbose, debug=debug)
            if debug:
                _print(f"DEBUG: Processing {update_action_name} file done: {file}")

    if os.path.isdir(file_or_directory):
        if ((files := glob.glob(os.path.join(file_or_directory, "*.json"))) and
            (files_and_schemas := _file_names_to_ordered_file_and_schema_names(portal, files))):  # noqa
            for file_and_schema in files_and_schemas:
                if not (file := file_and_schema[0]):
                    continue
                if not (schema_name := file_and_schema[1]) and not (schema_name := explicit_schema_name):
                    _print(f"ERROR: Schema cannot be inferred from file name and --schema not specified: {file}")
                    continue
                post_or_patch_or_upsert(portal, file_and_schema[0], schema_name=schema_name,
                                        confirm=confirm, quiet=quiet, verbose=verbose, debug=debug)
    elif os.path.isfile(file := file_or_directory):
        if ((schema_name := _get_schema_name_from_schema_named_json_file_name(portal, file)) or
            (schema_name := explicit_schema_name)):  # noqa
            post_or_patch_or_upsert(portal, file, schema_name=schema_name,
                                    confirm=confirm, quiet=quiet, verbose=verbose, debug=debug)
        else:
            post_or_patch_or_upsert(portal, file, schema_name=schema_name,
                                    confirm=confirm, quiet=quiet, verbose=verbose, debug=debug)
            # _print(f"ERROR: Schema cannot be inferred from file name and --schema not specified: {file}")
            # return
    else:
        _print(f"ERROR: Cannot find file or directory: {file_or_directory}")


def post_data(portal: Portal, data: dict, schema_name: str, confirm: bool = False,
              file: Optional[str] = None, index: int = 0,
              verbose: bool = False, debug: bool = False) -> None:
    if not (identifying_path := portal.get_identifying_path(data, portal_type=schema_name)):
        if isinstance(file, str) and isinstance(index, int):
            _print(f"ERROR: Item for POST has no identifying property: {file} (#{index + 1})")
        else:
            _print(f"ERROR: Item for POST has no identifying property.")
        return
    if portal.get_metadata(identifying_path, raise_exception=False):
        _print(f"ERROR: Item for POST already exists: {identifying_path}")
        return
    if (confirm is True) and not yes_or_no(f"POST data for: {identifying_path} ?"):
        return
    if verbose:
        _print(f"POST {schema_name} item: {identifying_path}")
    try:
        portal.post_metadata(schema_name, data)
        if debug:
            _print(f"DEBUG: POST {schema_name} item done: {identifying_path}")
    except Exception as e:
        _print(f"ERROR: Cannot POST {schema_name} item: {identifying_path}")
        _print(get_error_message(e))
        return


def patch_data(portal: Portal, data: dict, schema_name: str, confirm: bool = False,
               file: Optional[str] = None, index: int = 0,
               verbose: bool = False, debug: bool = False) -> None:
    if not (identifying_path := portal.get_identifying_path(data, portal_type=schema_name)):
        if isinstance(file, str) and isinstance(index, int):
            _print(f"ERROR: Item for PATCH has no identifying property: {file} (#{index + 1})")
        else:
            _print(f"ERROR: Item for PATCH has no identifying property.")
        return
    if not portal.get_metadata(identifying_path, raise_exception=False):
        _print(f"ERROR: Item for PATCH does not already exist: {identifying_path}")
        return
    if (confirm is True) and not yes_or_no(f"PATCH data for: {identifying_path}"):
        return
    if verbose:
        _print(f"PATCH {schema_name} item: {identifying_path}")
    try:
        portal.patch_metadata(identifying_path, data)
        if debug:
            _print(f"DEBUG: PATCH {schema_name} item OK: {identifying_path}")
    except Exception as e:
        _print(f"ERROR: Cannot PATCH {schema_name} item: {identifying_path}")
        _print(e)
        return


def upsert_data(portal: Portal, data: dict, schema_name: str, confirm: bool = False,
                file: Optional[str] = None, index: int = 0,
                verbose: bool = False, debug: bool = False) -> None:
    if not (identifying_path := portal.get_identifying_path(data, portal_type=schema_name)):
        if isinstance(file, str) and isinstance(index, int):
            _print(f"ERROR: Item for UPSERT has no identifying property: {file} (#{index + 1})")
        else:
            _print(f"ERROR: Item for UPSERT has no identifying property.")
        return
    exists = portal.get_metadata(identifying_path, raise_exception=False)
    if ((confirm is True) and not yes_or_no(f"{'PATCH' if exists else 'POST'} data for: {identifying_path} ?")):
        return
    if verbose:
        _print(f"{'PATCH' if exists else 'POST'} {schema_name} item: {identifying_path}")
    try:
        portal.post_metadata(schema_name, data) if not exists else portal.patch_metadata(identifying_path, data)
        if debug:
            _print(f"DEBUG: UPSERT {schema_name} item OK: {identifying_path}")
    except Exception as e:
        _print(f"ERROR: Cannot UPSERT {schema_name} item: {identifying_path}")
        _print(e)
        return


def _create_portal(env: Optional[str] = None, app: Optional[str] = None,
                   verbose: bool = False, debug: bool = False) -> Optional[Portal]:

    env_from_environ = None
    if not env and (app == APP_SMAHT):
        if env := os.environ.get(_SMAHT_ENV_ENVIRON_NAME):
            env_from_environ = True
    if not (portal := Portal(env, app=app) if env or app else None):
        return None
    if verbose:
        if (env := portal.env) or (env := os.environ(_SMAHT_ENV_ENVIRON_NAME)):
            _print(f"Portal environment"
                   f"{f' (from {_SMAHT_ENV_ENVIRON_NAME})' if env_from_environ else ''}: {portal.env}")
        if portal.keys_file:
            _print(f"Portal keys file: {portal.keys_file}")
        if portal.key_id:
            _print(f"Portal key prefix: {portal.key_id[0:2]}******")
        if portal.server:
            _print(f"Portal server: {portal.server}")
    return portal


def _read_json_from_file(file: str) -> Optional[dict]:
    try:
        if not os.path.exists(file):
            return None
        with io.open(file, "r") as f:
            try:
                return json.load(f)
            except Exception:
                _print(f"ERROR: Cannot load JSON from file: {file}")
                return None
    except Exception:
        _print(f"ERROR: Cannot open file: {file}")
        return None


def _file_names_to_ordered_file_and_schema_names(portal: Portal,
                                                 files: Union[List[str], str]) -> List[Tuple[str, Optional[str]]]:
    results = []
    if isinstance(files, str):
        files = [files]
    if not isinstance(files, list):
        return results
    for file in files:
        if isinstance(file, str) and file:
            results.append((file, _get_schema_name_from_schema_named_json_file_name(portal, file)))
    ordered_results = []
    for schema_name in _SCHEMA_ORDER:
        schema_name = portal.schema_name(schema_name)
        if result := next((item for item in results if item[1] == schema_name), None):
            ordered_results.append(result)
            results.remove(result)
    ordered_results.extend(results) if results else None
    return ordered_results


def _get_schema_name_from_schema_named_json_file_name(portal: Portal, value: str) -> Optional[str]:
    try:
        if not value.endswith(".json"):
            return None
        _, schema_name = _get_schema(portal, os.path.basename(value[:-5]))
        return schema_name
    except Exception:
        return False


@lru_cache(maxsize=1)
def _get_schemas(portal: Portal) -> Optional[dict]:
    return portal.get_schemas()


@lru_cache(maxsize=100)
def _get_schema(portal: Portal, name: str) -> Tuple[Optional[dict], Optional[str]]:
    if portal and name and (name := name.replace("_", "").replace("-", "").strip().lower()):
        if schemas := _get_schemas(portal):
            for schema_name in schemas:
                if schema_name.replace("_", "").replace("-", "").strip().lower() == name.lower():
                    return schemas[schema_name], schema_name
    return None, None


def _print(*args, **kwargs) -> None:
    PRINT(*args, **kwargs)
    sys.stdout.flush()


if __name__ == "__main__":
    main()
