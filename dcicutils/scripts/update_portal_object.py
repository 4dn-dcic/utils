# ------------------------------------------------------------------------------------------------------
# Command-line utility to update (post, patch, upsert) portal objects for SMaHT/CGAP/Fourfront.
# ------------------------------------------------------------------------------------------------------
# Example commands:
#
# update-portal-object --load {json-file | directory-with-json-files}
# update-portal-object --post {json-file | directory-with-json-files}
# update-portal-object --upsert {json-file | directory-with-json-files}
# update-portal-object --patch {json-file | directory-with-json-files}
#
# The specified json-file or file withing directory-with-jaon-files must be JSON containing either
# a list of objects, which which case the file name for the target schema name, or if not, then
# the --schema option must be used to specified the target schema; or the JSON must be a dictionary
# of schema names, where the value of each is a list of objects for that schema.
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
from dcicutils.captured_output import captured_output
from dcicutils.command_utils import yes_or_no
from dcicutils.common import ORCHESTRATED_APPS, APP_CGAP, APP_FOURFRONT, APP_SMAHT
from dcicutils.ff_utils import delete_metadata, purge_metadata
from dcicutils.misc_utils import get_error_message, ignored, normalize_string, PRINT, to_camel_case, to_snake_case
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
    parser.add_argument("--load", "--loadxl", type=str, required=False, default=None,
                        help="Load data via snovault.loadxl.")
    parser.add_argument("--ini", type=str, required=False, default=None, help="INI file for data via snovault.loadxl.")
    parser.add_argument("--delete", type=str, required=False, default=None, help="Delete data.")
    parser.add_argument("--purge", type=str, required=False, default=None, help="Purge data.")
    parser.add_argument("--noignore", action="store_true", required=False, default=False,
                        help="Do not ignore standard fields on update(s).")
    parser.add_argument("--ignore", nargs="+", help="Ignore these additional fields.")
    parser.add_argument("--unresolved-output", "--unresolved", type=str,
                        help="Output file to write unresolved references to for --load only.")
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

    if not (args.post or args.patch or args.upsert or args.delete or args.purge or args.load):
        usage()

    if not (portal := _create_portal(env=args.env, ini=args.ini, app=args.app, load=args.load,
                                     verbose=args.verbose, debug=args.debug, quiet=args.quiet)):
        exit(1)

    if args.load:
        _load_data(portal=portal, load=args.load, ini_file=args.ini, explicit_schema_name=args.schema,
                   unresolved_output=args.unresolved_output,
                   verbose=args.verbose, debug=args.debug, noprogress=args.noprogress)

    if explicit_schema_name := args.schema:
        schema, explicit_schema_name = _get_schema(portal, explicit_schema_name)
        if not schema:
            usage(f"Unknown specified schema name: {args.schema}")

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


def _load_data(portal: Portal, load: str, ini_file: str, explicit_schema_name: Optional[str] = None,
               unresolved_output: Optional[str] = False,
               verbose: bool = False, debug: bool = False, noprogress: bool = False,
               _single_insert_file: Optional[str] = None) -> bool:

    import snovault.loadxl
    from snovault.loadxl import load_all_gen, LoadGenWrapper
    from dcicutils.progress_bar import ProgressBar

    loadxl_summary = {}
    loadxl_unresolved = {}
    loadxl_output = []
    loadxl_total_item_count = 0
    loadxl_total_error_count = 0

    def loadxl(portal: Portal, inserts_directory: str, schema_names_to_load: dict):

        nonlocal LoadGenWrapper, load_all_gen, loadxl_summary, verbose, debug
        nonlocal loadxl_total_item_count, loadxl_total_error_count
        progress_total = sum(schema_names_to_load.values()) * 2  # loadxl does two passes
        progress_bar = ProgressBar(progress_total, interrupt_exit=True) if not noprogress else None

        def decode_bytes(str_or_bytes: Union[str, bytes], *, encoding: str = "utf-8") -> str:
            if not isinstance(encoding, str):
                encoding = "utf-8"
            if isinstance(str_or_bytes, bytes):
                return str_or_bytes.decode(encoding).strip()
            elif isinstance(str_or_bytes, str):
                return str_or_bytes.strip()
            return ""

        def loadxl_print(arg):
            if arg:
                loadxl_output.append(normalize_string(str(arg)))

        snovault.loadxl.print = loadxl_print

        LOADXL_RESPONSE_PATTERN = re.compile(r"^([A-Z]+):\s*([a-zA-Z\/\d_-]+)\s*(\S+)\s*(\S+)?\s*(.*)$")
        LOADXL_ACTION_NAME = {"POST": "Create", "PATCH": "Update", "SKIP": "Check",
                              "CHECK": "Validate", "ERROR": "Error"}
        current_item_type = None
        current_item_count = 0
        current_item_total = 0

        for item in LoadGenWrapper(load_all_gen(testapp=portal.vapp, inserts=inserts_directory,
                                                docsdir=None, overwrite=True, verbose=True,
                                                continue_on_exception=True)):
            loadxl_total_item_count += 1
            item = decode_bytes(item)
            match = LOADXL_RESPONSE_PATTERN.match(item)
            if not match or match.re.groups < 3:
                continue
            if (action := LOADXL_ACTION_NAME[match.group(1).upper()]) == "Error":
                loadxl_total_error_count += 1
                identifying_value = match.group(2)
                #
                # Example message for unresolved link ...
                #
                # ERROR: /22813a02-906b-4b60-b2b2-4afaea24aa28 Bad response: 422 Unprocessable Entity
                # (not 200 OK or 3xx redirect for http://localhost/file_set?skip_indexing=true)b\'{"@type":
                # ["ValidationFailure", "Error"], "status": "error", "code": # 422, "title": "Unprocessable Entity",
                # "description": "Failed validation", "errors": [{"location": "body", "name": # "Schema: ",
                # "description": "Unable to resolve link: /Library/a4e8f79f-4d47-4e85-9707-c343c940a315"},
                # {"location": "body", "name": "Schema: libraries.0",
                # "description": "\\\'a4e8f79f-4d47-4e85-9707-c343c940a315\\\' not found"}]}\'
                #
                # OR ...
                #
                # ERROR: /22813a02-906b-4b60-b2b2-4afaea24aa28 Bad response: 404 Not Found (not 200 OK or 3xx
                # redirect for http://localhost/22813a02-906b-4b60-b2b2-4afaea24aa28)b\'{"@type": ["HTTPNotFound",
                # "Error"], "status": "error", "code": 404, "title": "Not Found", "description": "The resource
                # could not be found.", "detail": "debug_notfound of url http://localhost/22813a02-906b-4b60-b2b2-4afaea24aa28; # noqa
                # path_info: \\\'/22813a02-906b-4b60-b2b2-4afaea24aa28\\\', context: <encoded.root.SMAHTRoot object at 0x136d41460>, # noqa
                # view_name: \\\'22813a02-906b-4b60-b2b2-4afaea24aa28\\\', subpath: (), traversed: (), root:
                # <encoded.root.SMAHTRoot object at 0x136d41460>, vroot: <encoded.root.SMAHTRoot object at 0x136d41460>, vroot_path: ()"}\' # noqa
                #
                if (item_type := re.search(r"https?://.*/(.*)\?skip_indexing=.*", item)) and (len(item_type.groups()) == 1):  # noqa
                    item_type = to_snake_case(item_type.group(1))
                    identifying_value = f"/{to_camel_case(item_type)}{identifying_value}"
                unresolved_link_error_message_prefix = "Unable to resolve link:"
                if (i := item.find(unresolved_link_error_message_prefix)) > 0:
                    unresolved_link = item[i + len(unresolved_link_error_message_prefix):].strip()
                    if (i := unresolved_link.find("\"")) > 0:
                        if (unresolved_link := unresolved_link[0:i]):
                            if not loadxl_unresolved.get(unresolved_link):
                                loadxl_unresolved[unresolved_link] = []
                            if identifying_value not in loadxl_unresolved[unresolved_link]:
                                loadxl_unresolved[unresolved_link].append(identifying_value)
                if not item_type:
                    continue
            else:
                item_type = match.group(3)
            if current_item_type != item_type:
                if noprogress and debug and current_item_type is not None:
                    _print()
                current_item_type = item_type
                current_item_count = 0
                current_item_total = schema_names_to_load[item_type]
                if progress_bar:
                    progress_bar.set_description(f"▶ {to_camel_case(current_item_type)}: {action}")
            current_item_count += 1
            if loadxl_summary.get(current_item_type, None) is None:
                loadxl_summary[current_item_type] = 0
            loadxl_summary[current_item_type] += 1
            if progress_bar:
                progress_bar.set_progress(loadxl_total_item_count)
            elif debug:
                _print(f"{current_item_type}: {current_item_count} or {current_item_total} ({action})")
        if progress_bar:
            progress_bar.set_description("▶ Load Complete")
            progress_bar.set_progress(progress_total)
            if loadxl_total_item_count > loadxl_total_error_count:
                _print()

    if not portal.vapp:
        _print("Must using INI based Portal object with --load (use --ini option to specify an INI file).")
        return False
    if not os.path.isabs(load := os.path.normpath(os.path.expanduser(load))):
        load = os.path.normpath(os.path.join(os.getcwd(), load))
    if not os.path.exists(load):
        _print(f"Specified JSON data file not found: {load}")
        return False

    if os.path.isdir(load):
        inserts_directory = load
        inserts_file = None
    else:
        inserts_directory = None
        inserts_file = load

    if inserts_file:
        with io.open(inserts_file, "r") as f:
            try:
                data = json.load(f)
            except Exception:
                _print(f"Cannot load JSON data from file: {inserts_file}")
                return False
            if isinstance(data, list):
                if not (schema_name := explicit_schema_name):
                    if not (schema_name := _get_schema_name_from_schema_named_json_file_name(portal, inserts_file)):
                        _print(f"Unable to determine schema name for JSON data file: {inserts_file}")
                        return False
                elif not (schema_name := _get_schema(portal, explicit_schema_name)[1]):
                    _print(f"Unknown specified schema name: {explicit_schema_name}")
                    return False
                with temporary_directory() as tmpdir:
                    file_name = os.path.join(tmpdir, f"{to_snake_case(schema_name)}.json")
                    with io.open(file_name, "w") as f:
                        json.dump(data, f)
                    return _load_data(portal=portal, load=tmpdir, ini_file=ini_file, explicit_schema_name=schema_name,
                                      unresolved_output=unresolved_output,
                                      verbose=verbose, debug=debug, noprogress=noprogress,
                                      _single_insert_file=inserts_file)
            elif isinstance(data, dict):
                if schema_name := explicit_schema_name:
                    if _is_schema_name_list(portal, schema_names := list(data.keys())):
                        _print(f"Ignoring specify --schema: {schema_name}")
                    elif not (schema_name := _get_schema(portal, schema_name)[1]):
                        _print(f"Unknown specified schema name: {explicit_schema_name}")
                        return False
                    else:
                        data = {schema_name: [data]}
                if not _is_schema_name_list(portal, schema_names := list(data.keys())):
                    if not (schema_name := _get_schema_name_from_schema_named_json_file_name(portal, inserts_file)):
                        _print(f"Unrecognized types in JSON data file: {inserts_file}")
                    # Assume simple object of type from the JSON file name.
                    schema_names = [schema_name]
                    data = {schema_name: [data]}
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
                        return _load_data(portal=portal, load=tmpdir, ini_file=ini_file,
                                          unresolved_output=unresolved_output,
                                          verbose=verbose, debug=debug, noprogress=noprogress,
                                          _single_insert_file=inserts_file)
                return True
            else:
                _print(f"Unrecognized JSON data in file: {inserts_file}")
                return False
        return True

    if verbose:
        if _single_insert_file:
            _print(f"Loading data into Portal (via snovault.loadxl) from file: {_single_insert_file}")
        else:
            _print(f"Loading data into Portal (via snovault.loadxl) from directory: {inserts_directory}")

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
        if _single_insert_file:
            _print(f"Done loading data into Portal (via snovault.loadxl) from file: {_single_insert_file}")
        else:
            _print(f"Done loading data into Portal (via snovault.loadxl) from directory: {inserts_directory}")
        _print(f"Total items loaded: {loadxl_total_item_count // 2}"  # TODO: straightend out this arithmetic
               f"{f' (errors: {loadxl_total_error_count})' if loadxl_total_error_count else ''}")
        for item in sorted(loadxl_summary.keys()):
            _print(f"▷ {to_camel_case(item)}: {loadxl_summary[item] // 2}")  # TODO: straightend out this arithmetic
    if loadxl_unresolved:
        _print("✗ Unresolved references:")
        for item in loadxl_unresolved:
            _print(f"  ✗ {item}: {len(loadxl_unresolved[item])}")
            for subitem in loadxl_unresolved[item]:
                _print(f"     ▶ {subitem}")
        if unresolved_output:
            if unresolved_output:
                if not os.path.isabs(unresolved_output := os.path.normpath(os.path.expanduser(unresolved_output))):
                    unresolved_output = os.path.normpath(os.path.join(os.getcwd(), unresolved_output))
                if os.path.exists(unresolved_output):
                    if os.path.isdir(unresolved_output):
                        _print("Unresolved output file exists as a directory: {unresolved_output}")
                        return False
                    _print(f"Unresolved output file already exists: {unresolved_output}")
                    if yes_or_no(f"Do you want to overwrite this file?"):
                        with io.open(unresolved_output, "w") as f:
                            for item in loadxl_unresolved:
                                f.write(f"{item}\n")
    if debug and loadxl_output:
        _print("✗ Output from loadxl:")
        for item in loadxl_output:
            _print(f"  ▶ {item}")

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


def _create_portal(env: Optional[str] = None, ini: Optional[str] = None, app: Optional[str] = None,
                   load: Optional[str] = None, verbose: bool = False, debug: bool = False,
                   quiet: bool = False) -> Optional[Portal]:

    if app:
        if (app not in ORCHESTRATED_APPS) and ((app := app.lower()) not in ORCHESTRATED_APPS):
            _print(f"Unknown app name; must be one of: {' | '.join(ORCHESTRATED_APPS)}")
            return None
    elif APP_SMAHT in (env or os.environ.get(_SMAHT_ENV_ENVIRON_NAME) or ""):
        app = APP_SMAHT
    elif APP_CGAP in (env or ""):
        app = APP_CGAP
    elif APP_FOURFRONT in (env or ""):
        app = APP_FOURFRONT

    if ini:
        if env:
            if not quiet:
                _print("Ignoring --env option when --ini option is given.")
        elif (app == _SMAHT_ENV_ENVIRON_NAME) and (env := os.environ.get(_SMAHT_ENV_ENVIRON_NAME)):
            if not quiet:
                _print(f"Ignoring SMAHT_ENV environment variable ({env}) when --ini option is given.")
        if not os.path.isabs(ini_file := os.path.normpath(os.path.expanduser(ini))):
            ini_file = os.path.normpath(os.path.join(os.getcwd(), ini_file))
        if not os.path.exists(ini_file):
            _print(f"Specified Portal INI file not found: {ini_file}")
            return None
        with captured_output(not debug):
            if not (portal := Portal(ini_file, app=app)):
                _print(f"Cannot create INI based Portal object: {env} ({app})")
                return None
    else:
        env_from_environ = False
        if not env and app:
            # If the --load option is specified, and no --ini option is specified, then do NOT default
            # to using the SMAHT_ENV environment variable (if set) for an access-key based Portal
            # object; rather default to the default INI file (i.e. development.ini).
            if (not load) and (app == APP_SMAHT) and (env := os.environ.get(_SMAHT_ENV_ENVIRON_NAME)):
                env_from_environ = True
        if not env:
            if not os.path.exists(ini_file := os.path.normpath(os.path.join(os.getcwd(), _DEFAULT_INI_FILE_FOR_LOAD))):
                _print("Must specify --ini or --env option in order to create a Portal object.")
                return None
            return _create_portal(ini=ini_file, app=app, verbose=verbose, debug=debug)
        if not (portal := Portal(env, app=app) if env or app else None):
            _print(f"Cannot create access-key based Portal object: {env}{f' ({app})' if app else ''}")
            return None

    if (ini_file := portal.ini_file):
        if not quiet:
            _print(f"Portal environment: {ini_file}")
    elif (env := portal.env) or (env := os.environ.get(_SMAHT_ENV_ENVIRON_NAME)):
        _print(f"Portal environment"
               f"{f' (from {_SMAHT_ENV_ENVIRON_NAME})' if env_from_environ else ''}: {portal.env}")
        if verbose:
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
