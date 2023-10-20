import contextlib
import json
import jsonschema
import re

from typing import Dict, List, Optional
from .common import AnyJsonData
from .ff_utils import get_schema
from .env_utils import EnvUtils, public_env_name
from .lang_utils import there_are, maybe_pluralize, disjoined_list
from .misc_utils import AbstractVirtualApp, PRINT
from .sheet_utils import JsonSchema, TabbedJsonSchemas, SheetData, TabbedSheetData
from .task_utils import pmap


class SchemaManager:

    SCHEMA_CACHE = {}  # Shared cache. Do not override. Use .clear_schema_cache() to clear it.

    @classmethod
    @contextlib.contextmanager
    def fresh_schema_manager_context_for_testing(cls):
        old_schema_cache = cls.SCHEMA_CACHE
        try:
            cls.SCHEMA_CACHE = {}
            yield
        finally:
            cls.SCHEMA_CACHE = old_schema_cache

    def __init__(self, schemas: Optional[TabbedJsonSchemas] = None,
                 portal_env: Optional[str] = None, portal_vapp: Optional[AbstractVirtualApp] = None):
        if portal_env is None and portal_vapp is None:
            portal_env = public_env_name(EnvUtils.PRD_ENV_NAME)
            PRINT(f"The portal_env was not explicitly supplied. Schemas will come from portal_env={portal_env!r}.")
        self.portal_env = portal_env
        self.portal_vapp = portal_vapp
        self.schemas = {} if schemas is None else schemas.copy()

    def fetch_relevant_schemas(self, schema_names: List[str]):  # , schemas: Optional[TabbedSchemas] = None):
        # if schemas is None:
        #     schemas = self.schemas
        # The schema_names argument is not normally given, but it is there for easier testing
        def name_and_schema(schema_name):
            # cached_schema = self.schemas.get(schema_name)  # schemas.get(schema_name)
            # schema = self.fetch_schema(schema_name) if cached_schema is None else cached_schema
            return schema_name, self.fetch_schema(schema_name)
        return {schema_name: schema
                for schema_name, schema in pmap(name_and_schema, schema_names)}

    def schema_exists(self, schema_name: str):
        return bool(self.fetch_schema(schema_name=schema_name))

    def fetch_schema(self, schema_name: str):
        override_schema = self.schemas.get(schema_name)
        if override_schema is not None:
            return override_schema
        schema: Optional[AnyJsonData] = self.SCHEMA_CACHE.get(schema_name)
        if schema is None and schema_name not in self.SCHEMA_CACHE:  # If None is already stored, don't look it up again
            schema = get_schema(schema_name, portal_env=self.portal_env, portal_vapp=self.portal_vapp)
            self.SCHEMA_CACHE[schema_name] = schema
        return schema

    @classmethod
    def clear_schema_cache(cls):
        for key in list(cls.SCHEMA_CACHE.keys()):  # important to get the list of keys as a separate object first
            cls.SCHEMA_CACHE.pop(key, None)

    def identifying_properties(self, schema: Optional[JsonSchema] = None, schema_name: Optional[str] = None,
                               among: Optional[List[str]] = None):
        schema = schema if schema is not None else self.fetch_schema(schema_name)
        possible_identifying_properties = set(schema.get("identifyingProperties") or []) | {'uuid'}
        identifying_properties = sorted(possible_identifying_properties
                                        if among is None
                                        else (prop
                                              for prop in among
                                              if prop in possible_identifying_properties))
        return identifying_properties

    @classmethod
    def identifying_value(cls, data_item: Dict[str, AnyJsonData], identifying_properties) -> AnyJsonData:
        if not identifying_properties:
            raise ValueError("No identifying properties were specified.")
        for identifying_property in identifying_properties:
            if identifying_property in data_item:
                return data_item[identifying_property]
        raise ValueError(f'{there_are(identifying_properties, just_are=True)}'
                         f' no {maybe_pluralize(identifying_properties, "identifying property")}'
                         f' {disjoined_list([repr(x) for x in identifying_properties])}'
                         f' in {json.dumps(data_item)}.')


def validate_data_against_schemas(data: TabbedSheetData,
                                  portal_vapp: Optional[AbstractVirtualApp] = None,
                                  schemas: Optional[TabbedJsonSchemas] = None) -> Optional[Dict]:
    """
    Validates the given data against the corresponding schema(s). The given data is assumed to
    be in a format as returned by sheet_utils, i.e. a dictionary of lists of objects where each
    top-level dictionary property is the name of a data type for the contained list of objects.
    If no schemas are passed then they will be fetched from the Portal using the given portal_vapp
    to access them; the schemas are in a form similar to the data - a dictionary of schema objects,
    where each top-level dictionary property is the name of the data type for the contained schema.
    These data types are (strings) assumed to be in snake-case form, e.g. "file_submitted".

    If there are any absent required properties, any extraneous properties, or any undentified
    items in the data, then returns a dictionary with an itemized description of each of these errors,
    otherwise returns None if there are no problems. Note that an unidentified item is one which has
    no value for uuid nor any of the other identifying property values as defined by the schema.

    For example given data that looks something like this:
        {
            "file_format": [
                <object-for-this-type>,
                <another-object-for-this-type>,
                <et-cetera>
            ],
            "file_submitted": [
                <object-for-this-type>,
                <another-object-for-this-type>,
                <et-cetera>
            ]
        }

    This function might return someting like this (assuming these errors existed):
        {
            "errors": [
                {   "type": "file_format",
                    "unidentified": true,
                    "index": 2
                    "identifying_properties": [ "uuid", "file_format" ]
                },
                {   "type": "file_format",
                    "item": "vcf_gz",
                    "index": 1
                    "missing_properties": [ "standard_file_format" ]
                },
                {   "type": "file_submitted",
                    "item": "ebcfa32f-8eea-4591-a784-449fa5cd9ae9",
                    "index": 3
                    "extraneous_properties": [ "xyzzy", "foobar" ]
                }
                {   "error": "No schema found for: some_undefined_type"
                }
            ]
        }

    The "item" is the identifying value for the specified object (uuid or another defined by the schema).
    The "index" is the (0-indexed) ordinal position of the object within the list within the type within
    the given data, which can be useful in identifying the object in the source data if it is unidentified.
    """

    schema_manager = SchemaManager(portal_vapp=portal_vapp, schemas=schemas)

    # def fetch_relevant_schemas(schema_names: List, portal_vapp: VirtualApp) -> List:
    #     def fetch_schema(schema_name: str) -> Optional[Dict]:
    #         return schema_name, get_schema(schema_name, portal_vapp=portal_vapp)
    #     return {schema_name: schema for schema_name, schema in pmap(fetch_schema, schema_names)}
    #
    # errors = []
    #
    # if not schemas:
    #     if not portal_vapp:
    #         raise Exception("Must specify portal_vapp if no schemas specified.")
    #     try:
    #         schema_names = [data_type for data_type in data]
    #         schemas = fetch_relevant_schemas(schema_names, portal_vapp=portal_vapp)
    #     except Exception as e:
    #         errors.append({"exception": f"Exception fetching relevant schemas: {get_error_message(e)}"})
    #         schemas = {}

    errors = []
    schemas = schema_manager.fetch_relevant_schemas(list(data.keys()))

    for data_type in data:
        schema = schemas.get(data_type)
        if not schema:
            errors.append({"error": f"No schema found for: {data_type}"})
            continue
        data_errors = validate_data_items_against_schemas(data[data_type], data_type, schema)
        errors.extend(data_errors)
    return {"errors": errors} if errors else None


def validate_data_items_against_schemas(data_items: SheetData, data_type: str, schema: JsonSchema) -> List[Dict]:
    """
    Like validate_data_against_schemas but for a simple list of data items each of the same given data type.
    """
    errors = []
    for data_item_index, data_item in enumerate(data_items):
        data_item_errors = validate_data_item_against_schemas(data_item, data_type, data_item_index, schema)
        errors.extend(data_item_errors)
    return errors


def validate_data_item_against_schemas(data_item: AnyJsonData, data_type: str,
                                       data_item_index: Optional[int], schema: JsonSchema) -> List[Dict]:
    """
    Like validate_data_against_schemas but for a single data item of the given data type.
    The given data item index is just for informational purposes; it corresponds to the
    ordinal index of the data item in its containing list. Uses the standard jsonschema
    package to do the heavy lifting of actual schema validation, but exerts extra effort to
    specifically itemize/aggregate the most common (missing and extraneous properties) errors.
    """
    errors = []

    identifying_properties = schema.get("identifyingProperties", [])
    identifying_value = SchemaManager.identifying_value(data_item, identifying_properties)
    if not identifying_value:
        errors.append({
            "type": data_type,
            "unidentified": True,
            "index": data_item_index,
            "identifying_properties": identifying_properties
        })

    def extract_single_quoted_strings(message: str) -> List[str]:
        return re.findall(r"'(.*?)'", message)

    schema_validator = jsonschema.Draft7Validator(schema)
    for schema_validation_error in schema_validator.iter_errors(data_item):
        if schema_validation_error.validator == "required":
            errors.append({
                "type": data_type,
                "item" if identifying_value else "unidentified": identifying_value if identifying_value else True,
                "index": data_item_index,
                "missing_properties": schema_validation_error.validator_value})
            continue
        if schema_validation_error.validator == "additionalProperties":
            properties = extract_single_quoted_strings(schema_validation_error.message)
            if properties:
                errors.append({
                    "type": data_type,
                    "item" if identifying_value else "unidentified": identifying_value if identifying_value else True,
                    "index": data_item_index,
                    "extraneous_properties": properties})
                continue
        errors.append({
            "type": data_type,
            "item" if identifying_value else "unidentified": identifying_value if identifying_value else True,
            "index": data_item_index,
            "unclassified_error": schema_validation_error.message})

    return errors


def summary_of_data_validation_errors(data_validation_errors: Dict,  # submission: SmahtSubmissionFolio,
                                      data_file_name: str,
                                      s3_data_file_location: str,
                                      s3_details_location: str) -> List[str]:
    """
    Summarize the given data validation errors into a simple short list of English phrases;
    this will end up going into the additional_properties of the IngestionSubmission object
    in the Portal database (see SubmissionFolio.record_results); this is what will get
    displayed, if any errors, by the submitr tool when it detects processing has completed.
    """
    errors = data_validation_errors.get("errors")
    if not errors:
        return []

    unidentified_count = 0
    missing_properties_count = 0
    extraneous_properties_count = 0
    unclassified_error_count = 0
    exception_count = 0

    for error in errors:
        if error.get("unidentified"):
            unidentified_count += 1
        if error.get("missing_properties"):
            missing_properties_count += 1
        if error.get("extraneous_properties"):
            extraneous_properties_count += 1
        if error.get("unclassified_error_count"):
            unclassified_error_count += 1
        if error.get("exception"):
            exception_count += 1

    return [
        f"Ingestion data validation error summary:",
        # f"Data file: {submission.data_file_name}",
        f"Data file: {data_file_name}",
        # f"Data file in S3: {submission.s3_data_file_location}",
        f"Data file in S3: {s3_data_file_location}",
        f"Items unidentified: {unidentified_count}",
        f"Items missing properties: {missing_properties_count}",
        f"Items with extraneous properties: {extraneous_properties_count}",
        f"Other errors: {unclassified_error_count}",
        f"Exceptions: {exception_count}",
        # f"Details: {submission.s3_details_location}"
        f"Details: {s3_details_location}"
    ]
