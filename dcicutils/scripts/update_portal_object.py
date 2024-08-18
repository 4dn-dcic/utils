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
import re
import shutil
import sys
from typing import Callable, List, Optional, Tuple, Union
from dcicutils.command_utils import yes_or_no
from dcicutils.common import ORCHESTRATED_APPS, APP_SMAHT
from dcicutils.ff_utils import delete_metadata, purge_metadata
from dcicutils.misc_utils import get_error_message, ignored, PRINT, to_camel_case, to_snake_case
from dcicutils.portal_utils import Portal as PortalFromUtils
from dcicutils.tmpfile_utils import temporary_directory


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
_DEFAULT_INI_FILE_FOR_LOAD = "development.ini"

# Schema properties to ignore (by default) for the view schema usage.
_IGNORE_PROPERTIES_ON_UPDATE = [
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
    parser.add_argument("--load", type=str, required=False, default=None, help="Load data via snovault.loadxl.")
    parser.add_argument("--ini", type=str, required=False, default=None, help="INI file for data via snovault.loadxl.")
    parser.add_argument("--delete", type=str, required=False, default=None, help="Delete data.")
    parser.add_argument("--purge", type=str, required=False, default=None, help="Purge data.")
    parser.add_argument("--noignore", action="store_true", required=False, default=False,
                        help="Do not ignore standard fields on update(s).")
    parser.add_argument("--ignore", nargs="+", help="Ignore these additional fields.")
    parser.add_argument("--confirm", action="store_true", required=False, default=False, help="Confirm before action.")
    parser.add_argument("--verbose", action="store_true", required=False, default=False, help="Verbose output.")
    parser.add_argument("--quiet", action="store_true", required=False, default=False, help="Quiet output.")
    parser.add_argument("--noprogress", action="store_true", required=False, default=False,
                        help="No progress bar output for --load.")
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

    if not (args.post or args.patch or args.upsert or args.delete or args.purge or args.load):
        usage()

    if args.load:
        if args.post or args.patch or args.upsert or args.delete or args.purge:
            _print("Cannot use any other update option"
                   "when using the --load option (to load data via snovault.loadxl).")
            exit(1)
        if args.env:
            _print("The --env is not used for the --load option (to load data via snovault.loadxl).")
        if args.schema:
            _print("The --schema is not used for the --load option (to load data via snovault.loadxl).")
        _load_data(load=args.load, ini_file=args.ini,
                   verbose=args.verbose, debug=args.debug, noprogress=args.noprogress)
        exit(0)

    portal = _create_portal(env=args.env, app=app, verbose=args.verbose, debug=args.debug)

    if explicit_schema_name := args.schema:
        schema, explicit_schema_name = _get_schema(portal, explicit_schema_name)
        if not schema:
            usage(f"ERROR: Unknown schema name: {args.schema}")

    if args.post:
        _post_or_patch_or_upsert(portal=portal,
                                 file_or_directory=args.post,
                                 explicit_schema_name=explicit_schema_name,
                                 update_function=_post_data,
                                 update_action_name="POST",
                                 noignore=args.noignore, ignore=args.ignore,
                                 confirm=args.confirm, verbose=args.verbose, quiet=args.quiet, debug=args.debug)
    if args.patch:
        _post_or_patch_or_upsert(portal=portal,
                                 file_or_directory=args.patch,
                                 explicit_schema_name=explicit_schema_name,
                                 update_function=_patch_data,
                                 update_action_name="PATCH",
                                 patch_delete_fields=args.delete,
                                 noignore=args.noignore, ignore=args.ignore,
                                 confirm=args.confirm, verbose=args.verbose, quiet=args.quiet, debug=args.debug)
        args.delete = None
    if args.upsert:
        _post_or_patch_or_upsert(portal=portal,
                                 file_or_directory=args.upsert,
                                 explicit_schema_name=explicit_schema_name,
                                 update_function=_upsert_data,
                                 update_action_name="UPSERT",
                                 patch_delete_fields=args.delete,
                                 noignore=args.noignore, ignore=args.ignore,
                                 confirm=args.confirm, verbose=args.verbose, quiet=args.quiet, debug=args.debug)
        args.delete = None

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
                             patch_delete_fields: Optional[str] = None,
                             noignore: bool = False, ignore: Optional[List[str]] = None,
                             confirm: bool = False, verbose: bool = False,
                             quiet: bool = False, debug: bool = False) -> None:

    def post_or_patch_or_upsert(portal: Portal, file: str, schema_name: Optional[str],
                                patch_delete_fields: Optional[str] = None,
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
                    update_function(portal, data, schema_name, file=file,
                                    patch_delete_fields=patch_delete_fields,
                                    noignore=noignore, ignore=ignore,
                                    confirm=confirm, verbose=verbose, debug=debug)
                elif _is_schema_name_list(portal, list(data.keys())):
                    if debug:
                        _print(f"DEBUG: File ({file}) contains a dictionary of schema names.")
                    for schema_name in data:
                        if isinstance(schema_data := data[schema_name], list):
                            schema_data = _impose_special_ordering(schema_data, schema_name)
                            if debug:
                                _print(f"DEBUG: Processing {update_action_name}s for type: {schema_name}")
                            for index, item in enumerate(schema_data):
                                update_function(portal, item, schema_name, file=file, index=index,
                                                patch_delete_fields=patch_delete_fields,
                                                noignore=noignore, ignore=ignore,
                                                confirm=confirm, verbose=verbose, debug=debug)
                        else:
                            _print(f"WARNING: File ({file}) contains schema item which is not a list: {schema_name}")
                else:
                    _print(f"WARNING: File ({file}) contains unknown item type.")
            elif isinstance(data, list):
                if debug:
                    _print(f"DEBUG: File ({file}) contains a list of objects of type: {schema_name}")
                data = _impose_special_ordering(data, schema_name)
                for index, item in enumerate(data):
                    update_function(portal, item, schema_name, file=file, index=index,
                                    patch_delete_fields=patch_delete_fields,
                                    noignore=noignore, ignore=ignore,
                                    confirm=confirm, verbose=verbose, debug=debug)
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
                                        patch_delete_fields=patch_delete_fields,
                                        confirm=confirm, quiet=quiet, verbose=verbose, debug=debug)
    elif os.path.isfile(file := file_or_directory):
        if ((schema_name := _get_schema_name_from_schema_named_json_file_name(portal, file)) or
            (schema_name := explicit_schema_name)):  # noqa
            post_or_patch_or_upsert(portal, file, schema_name=schema_name,
                                    patch_delete_fields=patch_delete_fields,
                                    confirm=confirm, quiet=quiet, verbose=verbose, debug=debug)
        else:
            post_or_patch_or_upsert(portal, file, schema_name=schema_name,
                                    patch_delete_fields=patch_delete_fields,
                                    confirm=confirm, quiet=quiet, verbose=verbose, debug=debug)
            # _print(f"ERROR: Schema cannot be inferred from file name and --schema not specified: {file}")
            # return
    else:
        _print(f"ERROR: Cannot find file or directory: {file_or_directory}")


def _impose_special_ordering(data: List[dict], schema_name: str) -> List[dict]:
    if schema_name == "FileFormat":
        return sorted(data, key=lambda item: "extra_file_formats" in item)
    return data


def _post_data(portal: Portal, data: dict, schema_name: str,
               file: Optional[str] = None, index: int = 0,
               patch_delete_fields: Optional[str] = None,
               noignore: bool = False, ignore: Optional[List[str]] = None,
               confirm: bool = False, verbose: bool = False, debug: bool = False) -> None:
    ignored(patch_delete_fields)
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
        data = _prune_data_for_update(data, noignore=noignore, ignore=ignore)
        portal.post_metadata(schema_name, data)
        if debug:
            _print(f"DEBUG: POST {schema_name} item done: {identifying_path}")
    except Exception as e:
        _print(f"ERROR: Cannot POST {schema_name} item: {identifying_path}")
        _print(get_error_message(e))
        return


def _patch_data(portal: Portal, data: dict, schema_name: str,
                file: Optional[str] = None, index: int = 0,
                patch_delete_fields: Optional[str] = None,
                noignore: bool = False, ignore: Optional[List[str]] = None,
                confirm: bool = False, verbose: bool = False, debug: bool = False) -> None:
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
        if delete_fields := _parse_delete_fields(patch_delete_fields):
            identifying_path += f"?delete_fields={delete_fields}"
        data = _prune_data_for_update(data, noignore=noignore, ignore=ignore)
        portal.patch_metadata(identifying_path, data)
        if debug:
            _print(f"DEBUG: PATCH {schema_name} item OK: {identifying_path}")
    except Exception as e:
        _print(f"ERROR: Cannot PATCH {schema_name} item: {identifying_path}")
        _print(e)
        return


def _upsert_data(portal: Portal, data: dict, schema_name: str,
                 file: Optional[str] = None, index: int = 0,
                 patch_delete_fields: Optional[str] = None,
                 noignore: bool = False, ignore: Optional[List[str]] = None,
                 confirm: bool = False, verbose: bool = False, debug: bool = False) -> None:
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
        if not exists:
            data = _prune_data_for_update(data, noignore=noignore, ignore=ignore)
            portal.post_metadata(schema_name, data)
        else:
            if delete_fields := _parse_delete_fields(patch_delete_fields):
                identifying_path += f"?delete_fields={delete_fields}"
            data = _prune_data_for_update(data, noignore=noignore, ignore=ignore)
            portal.patch_metadata(identifying_path, data)
        if debug:
            _print(f"DEBUG: UPSERT {schema_name} item OK: {identifying_path}")
    except Exception as e:
        _print(f"ERROR: Cannot UPSERT {schema_name} item: {identifying_path}")
        _print(e)
        return


def _load_data(load: str, ini_file: str, explicit_schema_name: Optional[str] = None,
               verbose: bool = False, debug: bool = False, noprogress: bool = False) -> bool:

    from snovault.loadxl import load_all_gen, LoadGenWrapper
    from dcicutils.captured_output import captured_output
    from dcicutils.progress_bar import ProgressBar

    def loadxl(portal: Portal, inserts_directory: str, schema_names_to_load: dict):

        nonlocal LoadGenWrapper, load_all_gen, verbose, debug
        progress_total = sum(schema_names_to_load.values()) * 2  # loadxl does two passes
        progress_bar = ProgressBar(progress_total) if not noprogress else None

        def decode_bytes(str_or_bytes: Union[str, bytes], *, encoding: str = "utf-8") -> str:
            if not isinstance(encoding, str):
                encoding = "utf-8"
            if isinstance(str_or_bytes, bytes):
                return str_or_bytes.decode(encoding).strip()
            elif isinstance(str_or_bytes, str):
                return str_or_bytes.strip()
            return ""

        LOADXL_RESPONSE_PATTERN = re.compile(r"^([A-Z]+):\s*([a-zA-Z\/\d_-]+)\s*(\S+)\s*(\S+)?\s*(.*)$")
        LOADXL_ACTION_NAME = {"POST": "Create", "PATCH": "Update", "SKIP": "Check",
                              "CHECK": "Validate", "ERROR": "Error"}
        current_item_type = None
        current_item_count = 0
        current_item_total = 0
        total_item_count = 0
        for item in LoadGenWrapper(load_all_gen(testapp=portal.vapp, inserts=inserts_directory,
                                                docsdir=None, overwrite=True, verbose=True)):
            total_item_count += 1
            item = decode_bytes(item)
            match = LOADXL_RESPONSE_PATTERN.match(item)
            if not match or match.re.groups < 3:
                continue
            action = LOADXL_ACTION_NAME[match.group(1).upper()]
            # response_value = match.group(0)
            # identifying_value = match.group(2)
            item_type = match.group(3)
            if current_item_type != item_type:
                if noprogress and debug and current_item_type is not None:
                    print()
                current_item_type = item_type
                current_item_count = 0
                current_item_total = schema_names_to_load[item_type]
                if progress_bar:
                    progress_bar.set_description(f"▶ {to_camel_case(current_item_type)}: {action}")
            current_item_count += 1
            if progress_bar:
                progress_bar.set_progress(total_item_count)
            elif debug:
                print(f"{current_item_type}: {current_item_count} or {current_item_total} ({action})")
        if progress_bar:
            progress_bar.set_description("▶ Load Complete")
            print()

    if not ini_file:
        ini_file = _DEFAULT_INI_FILE_FOR_LOAD
    if not os.path.isabs(ini_file := os.path.expanduser(ini_file)):
        ini_file = os.path.join(os.getcwd(), ini_file)
    if not os.path.exists(ini_file):
        _print(f"The INI file required for --load is not found: {ini_file}")
        exit(1)

    if not os.path.isabs(load := os.path.expanduser(load)):
        load = os.path.join(os.getcwd(), load)
    if not os.path.exists(load):
        return False

    if os.path.isdir(load):
        inserts_directory = load
        inserts_file = None
    else:
        inserts_directory = None
        inserts_file = load

    portal = None
    with captured_output(not debug):
        portal = Portal(ini_file)

    if inserts_file:
        with io.open(inserts_file, "r") as f:
            try:
                data = json.load(f)
            except Exception as e:
                _print(f"Cannot load JSON data from file: {inserts_file}")
                return False
            if isinstance(data, list):
                if not (schema_name := explicit_schema_name):
                    if not (schema_name := _get_schema_name_from_schema_named_json_file_name(portal, inserts_file)):
                        _print("Unable to determine schema name for JSON data file: {inserts_file}")
                        return False
                with temporary_directory() as tmpdir:
                    file_name = os.path.join(tmpdir, f"{to_snake_case(schema_name)}.json")
                    with io.open(file_name, "w") as f:
                        json.dump(data, f)
                    return _load_data(load=tmpdir, ini_file=ini_file, explicit_schema_name=explicit_schema_name,
                                      verbose=verbose, debug=debug, noprogress=noprogress)
            elif isinstance(data, dict):
                _print("DICT IN FILE FOR LOAD NOT YET SUPPPORTED")
                if not _is_schema_name_list(portal, schema_names := list(data.keys())):
                    _print(f"Unrecognized types in JSON data file: {inserts_file}")
                    return False
                with temporary_directory() as tmpdir:
                    nfiles = 0
                    for schema_name in schema_names:
                        if not isinstance(schema_data := data[schema_name], list):
                            _print(f"Unexpected value for data type ({schema_name})"
                                f" in JSON data file: {inserts_file} ▶ ignoring")
                            continue
                        file_name = os.path.join(tmpdir, f"{to_snake_case(schema_name)}.json")
                        with io.open(file_name, "w") as f:
                            json.dump(schema_data, f)
                        nfiles += 1
                    if nfiles > 0:
                        return _load_data(load=tmpdir, ini_file=ini_file,
                                          verbose=verbose, debug=debug, noprogress=noprogress)
                # TODO
                return True
            else:
                _print(f"Unrecognized JSON data in file: {inserts_file}")
                return False
        return True
    if verbose:
        _print(f"Loading data files into Portal (via snovault.loadxl) from: {inserts_directory}")
        _print(f"Portal INI file for load is: {ini_file}")

    schema_names = list(_get_schemas(portal).keys())
    schema_snake_case_names = [to_snake_case(item) for item in schema_names]
    schema_names_to_load = {}

    copy_to_temporary_directory = False
    for json_file_path in glob.glob(os.path.join(inserts_directory, "*.json")):
        json_file_name = os.path.basename(json_file_path)
        schema_name = os.path.basename(json_file_name)[:-len(".json")]
        if (schema_name not in schema_snake_case_names) and (schema_name not in schema_names):
            _print(f"File is not named for a known schema: {json_file_name} ▶ ignoring")
            copy_to_temporary_directory = True
        else:
            try:
                with io.open(json_file_path, "r") as f:
                    if not isinstance(data := json.load(f), list):
                        _print("Data JSON file does not contain an array: {json_file_path} ▶ ignoring")
                        copy_to_temporary_directory = True
                    elif (nobjects := len(data)) < 1:
                        _print("Data JSON file contains no items: {json_file_path} ▶ ignoring")
                        copy_to_temporary_directory = True
                    else:
                        schema_names_to_load[schema_name] = nobjects
            except Exception:
                _print("Cannot load JSON data from file: {json_file_path} ▶ ignoring")
                copy_to_temporary_directory = True
    if not schema_names_to_load:
        _print("Directory contains no valid data: {inserts_directory}")
        return False
    if copy_to_temporary_directory:
        with temporary_directory() as tmpdir:
            if debug:
                _print(f"Using temporary directory: {tmpdir}")
            for json_file_path in glob.glob(os.path.join(inserts_directory, "*.json")):
                json_file_name = os.path.basename(json_file_path)
                schema_name = os.path.basename(json_file_name)[:-len(".json")]
                if (schema_name in schema_snake_case_names) or (schema_name in schema_names):
                    shutil.copy(json_file_path, tmpdir)
            loadxl(portal=portal, inserts_directory=tmpdir, schema_names_to_load=schema_names_to_load)
    else:
        loadxl(portal=portal, inserts_directory=inserts_directory, schema_names_to_load=schema_names_to_load)
    if verbose:
        _print(f"Done loading data into Portal (via snovault.loadxl) files from: {inserts_directory}")
    return True


def _is_schema_name_list(portal: Portal, keys: list) -> bool:
    if isinstance(keys, list):
        for key in keys:
            if portal.get_schema(key) is None:
                return False
            return True
    return False

def _prune_data_for_update(data: dict, noignore: bool = False, ignore: Optional[List[str]] = None) -> dict:
    ignore_these_properties = [] if noignore is True else _IGNORE_PROPERTIES_ON_UPDATE
    if isinstance(ignore, list):
        ignore_these_properties = ignore_these_properties + ignore
    if not ignore_these_properties:
        return data
    return {key: value for key, value in data.items() if key not in ignore_these_properties}


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


def _parse_delete_fields(value: str) -> str:
    if not isinstance(value, str):
        value = []
    else:
        value = list(set([part.strip() for part in re.split(r'[,;|\s]+', value) if part.strip()]))
    return ",".join(value)


def _get_schema_name_from_schema_named_json_file_name(portal: Portal, value: str) -> Optional[str]:
    if isinstance(value, str) and value:
        try:
            if value.endswith(".json"):
                value = value[:-5]
            _, schema_name = _get_schema(portal, os.path.basename(value))
            return schema_name
        except Exception:
            pass
    return False


@lru_cache(maxsize=1)
def _get_schemas(portal: Portal) -> Optional[dict]:
    if portal.vapp:
        return portal.vapp.get("/profiles/?frame=raw").json
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
