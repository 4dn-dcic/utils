from copy import deepcopy
from functools import lru_cache
import re
from typing import Any, List, Optional, Tuple, Type, Union
from dcicutils.data_readers import RowReader
from dcicutils.misc_utils import create_readonly_object
from dcicutils.portal_utils import Portal
from dcicutils.schema_utils import Schema

PortalObject = Type["PortalObject"]  # Forward type reference for type hints.


class PortalObject:

    _PROPERTY_DELETION_SENTINEL = RowReader.CELL_DELETION_SENTINEL

    def __init__(self, data: dict, portal: Portal = None,
                 schema: Optional[Union[dict, Schema]] = None, type: Optional[str] = None) -> None:
        self._data = data if isinstance(data, dict) else {}
        self._portal = portal if isinstance(portal, Portal) else None
        self._schema = schema if isinstance(schema, dict) else (schema.data if isinstance(schema, Schema) else None)
        self._type = type if isinstance(type, str) and type else None

    @property
    def data(self) -> dict:
        return self._data

    @property
    def portal(self) -> Optional[Portal]:
        return self._portal

    @property
    @lru_cache(maxsize=1)
    def type(self) -> Optional[str]:
        return self._type or Portal.get_schema_type(self._data) or (Schema(self._schema).type if self._schema else None)

    @property
    @lru_cache(maxsize=1)
    def types(self) -> Optional[List[str]]:
        return [self._type] if self._type else Portal.get_schema_types(self._data)

    @property
    @lru_cache(maxsize=1)
    def uuid(self) -> Optional[str]:
        return self._data.get("uuid") if isinstance(self._data, dict) else None

    @property
    @lru_cache(maxsize=1)
    def schema(self) -> Optional[dict]:
        return self._schema if self._schema else (self._portal.get_schema(self.type) if self._portal else None)

    def copy(self) -> PortalObject:
        return PortalObject(deepcopy(self.data), portal=self.portal, type=self.type)

    @property
    @lru_cache(maxsize=1)
    def identifying_properties(self) -> Optional[List[str]]:
        """
        Returns the list of all identifying property names of this Portal object which actually have values.
        Implicitly include "uuid" and "identifier" properties as identifying properties if they are actually
        properties in the object schema, and favor these (first); defavor "aliases"; no other ordering defined.
        """
        if not (schema := self.schema) or not (schema_identifying_properties := schema.get("identifyingProperties")):
            return None
        identifying_properties = []
        for identifying_property in schema_identifying_properties:
            if identifying_property not in ["uuid", "identifier", "aliases"]:
                if self._data.get(identifying_property):
                    identifying_properties.append(identifying_property)
        if self._data.get("identifier"):
            identifying_properties.insert(0, "identifier")
        if self._data.get("uuid"):
            identifying_properties.insert(0, "uuid")
        if "aliases" in schema_identifying_properties and self._data.get("aliases"):
            identifying_properties.append("aliases")
        return identifying_properties or None

    @property
    @lru_cache(maxsize=1)
    def identifying_paths(self) -> Optional[List[str]]:
        """
        Returns a list of the possible Portal URL paths identifying this Portal object.
        """
        identifying_paths = []
        if not (identifying_properties := self.identifying_properties):
            if self.uuid:
                if self.type:
                    identifying_paths.append(f"/{self.type}/{self.uuid}")
                identifying_paths.append(f"/{self.uuid}")
            return identifying_paths
        for identifying_property in identifying_properties:
            if (identifying_value := self._data.get(identifying_property)):
                if identifying_property == "uuid":
                    identifying_paths.append(f"/{self.type}/{identifying_value}")
                    identifying_paths.append(f"/{identifying_value}")
                # For now at least we include the path both with and without the schema type component,
                # as for some identifying values, it works (only) with, and some, it works (only) without.
                # For example: If we have FileSet with "accession", an identifying property, with value
                # SMAFSFXF1RO4 then /SMAFSFXF1RO4 works but /FileSet/SMAFSFXF1RO4 does not; and
                # conversely using "submitted_id", also an identifying property, with value
                # UW_FILE-SET_COLO-829BL_HI-C_1 then /UW_FILE-SET_COLO-829BL_HI-C_1 does
                # not work but /FileSet/UW_FILE-SET_COLO-829BL_HI-C_1 does work.
                elif isinstance(identifying_value, list):
                    for identifying_value_item in identifying_value:
                        if self.type:
                            identifying_paths.append(f"/{self.type}/{identifying_value_item}")
                        identifying_paths.append(f"/{identifying_value_item}")
                else:
                    if (schema := self.schema):
                        if pattern := schema.get("properties", {}).get(identifying_property, {}).get("pattern"):
                            if not re.match(pattern, identifying_value):
                                # If this identifying value is for a (identifying) property which has a
                                # pattern, and the value does NOT match the pattern, then do NOT include
                                # this value as an identifying path, since it cannot possibly be found.
                                continue
                    if self.type:
                        identifying_paths.append(f"/{self.type}/{identifying_value}")
                    identifying_paths.append(f"/{identifying_value}")
        return identifying_paths or None

    @property
    @lru_cache(maxsize=1)
    def identifying_path(self) -> Optional[str]:
        if identifying_paths := self.identifying_paths:
            return identifying_paths[0]

    def lookup(self, include_identifying_path: bool = False,
               raw: bool = False) -> Optional[Union[Tuple[PortalObject, str], PortalObject]]:
        return self._lookup(raw=raw) if include_identifying_path else self._lookup(raw=raw)[0]

    def lookup_identifying_path(self) -> Optional[str]:
        return self._lookup()[1]

    def _lookup(self, raw: bool = False) -> Tuple[Optional[PortalObject], Optional[str]]:
        try:
            if identifying_paths := self.identifying_paths:
                for identifying_path in identifying_paths:
                    if (value := self._portal.get(identifying_path, raw=raw)) and (value.status_code == 200):
                        return PortalObject(value.json(),
                                            portal=self._portal, type=self.type if raw else None), identifying_path
        except Exception:
            pass
        return None, self.identifying_path

    def compare(self, value: Union[dict, PortalObject],
                consider_refs: bool = False, resolved_refs: List[dict] = None) -> dict:
        if consider_refs and isinstance(resolved_refs, list):
            this_data = self.normalized_refs(refs=resolved_refs).data
        else:
            this_data = self.data
        if isinstance(value, PortalObject):
            comparing_data = value.data
        elif isinstance(value, dict):
            comparing_data = value
        else:
            return {}
        return PortalObject._compare(this_data, comparing_data)

    @staticmethod
    def _compare(a: Any, b: Any, _path: Optional[str] = None) -> dict:
        def diff_creating(value: Any) -> object:  # noqa
            return create_readonly_object(value=value,
                                          creating_value=True, updating_value=None, deleting_value=False)
        def diff_updating(value: Any, updating_value: Any) -> object:  # noqa
            return create_readonly_object(value=value,
                                          creating_value=False, updating_value=updating_value, deleting_value=False)
        def diff_deleting(value: Any) -> object:  # noqa
            return create_readonly_object(value=value,
                                          creating_value=False, updating_value=None, deleting_value=True)
        diffs = {}
        if isinstance(a, dict) and isinstance(b, dict):
            for key in a:
                path = f"{_path}.{key}" if _path else key
                if key not in b:
                    if a[key] != PortalObject._PROPERTY_DELETION_SENTINEL:
                        diffs[path] = diff_creating(a[key])
                else:
                    diffs.update(PortalObject._compare(a[key], b[key], _path=path))
        elif isinstance(a, list) and isinstance(b, list):
            # Ignore order of array elements; not absolutely technically correct but suits our purpose.
            for index in range(len(a)):
                path = f"{_path or ''}#{index}"
                if not isinstance(a[index], dict) and not isinstance(a[index], list):
                    if a[index] not in b:
                        if a[index] != PortalObject._PROPERTY_DELETION_SENTINEL:
                            if index < len(b):
                                diffs[path] = diff_updating(a[index], b[index])
                            else:
                                diffs[path] = diff_creating(a[index])
                        else:
                            if index < len(b):
                                diffs[path] = diff_deleting(b[index])
                elif index < len(b):
                    diffs.update(PortalObject._compare(a[index], b[index], _path=path))
                else:
                    diffs[path] = diff_creating(a[index])
        elif a != b:
            if a == PortalObject._PROPERTY_DELETION_SENTINEL:
                diffs[_path] = diff_deleting(b)
            else:
                diffs[_path] = diff_updating(a, b)
        return diffs

    def normalize_refs(self, refs: List[dict]) -> None:
        """
        Turns any (linkTo) references which are paths (e.g. /SubmissionCenter/uwsc_gcc) within
        this Portal object into the uuid style reference (e.g. d1b67068-300f-483f-bfe8-63d23c93801f),
        based on the given "refs" list which is assumed to be a list of dictionaries, where each
        contains a "path" and a "uuid" property; this list is typically (for our first usage of
        this function) the value of structured_data.StructuredDataSet.resolved_refs_with_uuid.
        Changes are made to this Portal object in place; use normalized_refs function to make a copy.
        If there are no "refs" (None or empty) or if the speicified reference is not found in this
        list then the references will be looked up via Portal calls (via Portal.get_metadata).
        """
        PortalObject._normalize_refs(self.data, refs=refs, schema=self.schema, portal=self.portal)

    def normalized_refs(self, refs: List[dict]) -> PortalObject:
        """
        Same as normalize_ref but does not make this change to this Portal object in place,
        rather it returns a new instance of this Portal object wrapped in a new PortalObject.
        """
        portal_object = self.copy()
        portal_object.normalize_refs(refs)
        return portal_object

    @staticmethod
    def _normalize_refs(value: Any, refs: List[dict], schema: dict, portal: Portal, _path: Optional[str] = None) -> Any:
        if not value or not isinstance(schema, dict):
            return value
        if isinstance(value, dict):
            for key in value:
                path = f"{_path}.{key}" if _path else key
                value[key] = PortalObject._normalize_refs(value[key], refs=refs,
                                                          schema=schema, portal=portal, _path=path)
        elif isinstance(value, list):
            for index in range(len(value)):
                path = f"{_path or ''}#{index}"
                value[index] = PortalObject._normalize_refs(value[index], refs=refs,
                                                            schema=schema, portal=portal, _path=path)
        elif value_type := Schema.get_property_by_path(schema, _path):
            if link_to := value_type.get("linkTo"):
                ref_path = f"/{link_to}/{value}"
                if not isinstance(refs, list):
                    refs = []
                if ref_uuids := [ref.get("uuid") for ref in refs if ref.get("path") == ref_path]:
                    ref_uuid = ref_uuids[0]
                else:
                    ref_uuid = None
                if ref_uuid:
                    return ref_uuid
                # Here our (linkTo) reference appears not to be in the given refs; if these refs came
                # from structured_data.StructuredDataSet.resolved_refs_with_uuid (in the context of
                # smaht-submitr, which is the typical/first use case for this function) then this could
                # be because the reference was to an internal object, i.e. another object existing within
                # the data/spreadsheet being submitted. In any case, we don't have the associated uuid
                # so let us look it up here.
                if isinstance(portal, Portal):
                    if (ref_object := portal.get_metadata(ref_path)) and (ref_uuid := ref_object.get("uuid")):
                        return ref_uuid
        return value
