from copy import deepcopy
from functools import lru_cache
from typing import Any, Callable, List, Optional, Tuple, Type, Union
from dcicutils.data_readers import RowReader
from dcicutils.misc_utils import create_readonly_object
from dcicutils.portal_utils import Portal
from dcicutils.schema_utils import Schema

PortalObject = Type["PortalObject"]  # Forward type reference for type hints.


class PortalObject:

    _PROPERTY_DELETION_SENTINEL = RowReader.CELL_DELETION_SENTINEL

    def __init__(self, data: dict, portal: Optional[Portal] = None, type: Optional[str] = None) -> None:
        self._data = data if isinstance(data, dict) else {}
        self._portal = portal if isinstance(portal, Portal) else None
        self._type = type if isinstance(type, str) else ""

    @property
    def data(self) -> dict:
        return self._data

    @property
    def portal(self) -> Optional[Portal]:
        return self._portal

    @property
    @lru_cache(maxsize=1)
    def type(self) -> str:
        return self._type or Portal.get_schema_type(self._data) or ""

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
        return self._portal.get_schema(self.type) if self._portal else None

    def copy(self) -> PortalObject:
        return PortalObject(deepcopy(self.data), portal=self.portal, type=self.type)

    @property
    @lru_cache(maxsize=1)
    def identifying_properties(self) -> Optional[List[str]]:
        """
        Returns the list of all identifying property names of this Portal object which actually have values.
        Implicitly include "uuid" and "identifier" properties as identifying properties if they are actually
        properties in the object schema, and favor these (first); defavor "aliases"; no other ordering defined.
        Changed (2024-05-26) to use portal_utils.get_identifying_property_names; migrating some intricate stuff there.
        """
        # Migrating to and unifying this in portal_utils.Portal.get_identifying_paths (2024-05-26).
        return self._portal.get_identifying_property_names(self.type, portal_object=self._data) if self._portal else []

    @lru_cache(maxsize=8192)
    def lookup(self, raw: bool = False,
               ref_lookup_strategy: Optional[Callable] = None) -> Tuple[Optional[PortalObject], Optional[str], int]:
        if not (identifying_paths := self._get_identifying_paths(ref_lookup_strategy=ref_lookup_strategy)):
            return None, None, 0
        nlookups = 0
        first_identifying_path = None
        try:
            for identifying_path in identifying_paths:
                if not first_identifying_path:
                    first_identifying_path = identifying_path
                nlookups += 1
                if self._portal and (item := self._portal.get(identifying_path, raw=raw)) and (item.status_code == 200):
                    return (
                        PortalObject(item.json(), portal=self._portal, type=self.type if raw else None),
                        identifying_path,
                        nlookups
                    )
        except Exception:
            pass
        return None, first_identifying_path, nlookups

    def compare(self, value: Union[dict, PortalObject],
                consider_refs: bool = False, resolved_refs: List[dict] = None) -> Tuple[dict, int]:
        if consider_refs and isinstance(resolved_refs, list):
            normlized_portal_object, nlookups = self._normalized_refs(refs=resolved_refs)
            this_data = normlized_portal_object.data
        else:
            this_data = self.data
            nlookups = 0
        if isinstance(value, PortalObject):
            comparing_data = value.data
        elif isinstance(value, dict):
            comparing_data = value
        else:
            return {}, nlookups
        return PortalObject._compare(this_data, comparing_data), nlookups

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
                            diffs[path] = diff_creating(a[index])
                        else:
                            diffs[path] = diff_deleting(b[index])
                elif index < len(b):
                    diffs.update(PortalObject._compare(a[index], b[index], _path=path))
                else:
                    diffs[path] = diff_creating(a[index])
            for index in range(len(b)):
                path = f"{_path or ''}#{index}.deleting"
                if b[index] not in a:
                    diffs[path] = diff_deleting(b[index])
        elif a != b:
            if a == PortalObject._PROPERTY_DELETION_SENTINEL:
                diffs[_path] = diff_deleting(b)
            else:
                diffs[_path] = diff_updating(a, b)
        return diffs

    @lru_cache(maxsize=1)
    def _get_identifying_paths(self, ref_lookup_strategy: Optional[Callable] = None) -> Optional[List[str]]:
        if not self._portal and (uuid := self.uuid):
            return [f"/{uuid}"]
        # Migrating to and unifying this in portal_utils.Portal.get_identifying_paths (2024-05-26).
        return self._portal.get_identifying_paths(self._data,
                                                  portal_type=self.schema,
                                                  lookup_strategy=ref_lookup_strategy) if self._portal else None

    def _normalized_refs(self, refs: List[dict]) -> Tuple[PortalObject, int]:
        """
        Same as _normalize_ref but does NOT make this change to this Portal object IN PLACE,
        rather it returns a new instance of this Portal object wrapped in a new PortalObject.
        """
        portal_object = self.copy()
        nlookups = portal_object._normalize_refs(refs)
        return portal_object, nlookups

    def _normalize_refs(self, refs: List[dict]) -> int:
        """
        Turns any (linkTo) references which are paths (e.g. /SubmissionCenter/uwsc_gcc) within this
        object IN PLACE into the uuid style reference (e.g. d1b67068-300f-483f-bfe8-63d23c93801f),
        based on the given "refs" list which is assumed to be a list of dictionaries, where each
        contains a "path" and a "uuid" property; this list is typically (for our first usage of
        this function) the value of structured_data.StructuredDataSet.resolved_refs_with_uuid.
        Changes are made to this Portal object IN PLACE; use _normalized_refs function to make a copy.
        If there are no "refs" (None or empty) or if the speicified reference is not found in this
        list then the references will be looked up via Portal calls (via Portal.get_metadata).
        """
        _, nlookups = PortalObject._normalize_data_refs(self.data, refs=refs, schema=self.schema, portal=self.portal)
        return nlookups

    @staticmethod
    def _normalize_data_refs(value: Any, refs: List[dict], schema: dict,
                             portal: Portal, _path: Optional[str] = None) -> Tuple[Any, int]:
        nlookups = 0
        if not value or not isinstance(schema, dict):
            return value, nlookups
        if isinstance(value, dict):
            for key in value:
                path = f"{_path}.{key}" if _path else key
                value[key], nlookups = PortalObject._normalize_data_refs(value[key], refs=refs,
                                                                         schema=schema, portal=portal, _path=path)
        elif isinstance(value, list):
            for index in range(len(value)):
                path = f"{_path or ''}#{index}"
                value[index], nlookups = PortalObject._normalize_data_refs(value[index], refs=refs,
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
                    return ref_uuid, nlookups
                # Here our (linkTo) reference appears not to be in the given refs; if these refs came
                # from structured_data.StructuredDataSet.resolved_refs_with_uuid (in the context of
                # smaht-submitr, which is the typical/first use case for this function) then this could
                # be because the reference was to an internal object, i.e. another object existing within
                # the data/spreadsheet being submitted. In any case, we don't have the associated uuid
                # so let us look it up here.
                if isinstance(portal, Portal):
                    nlookups += 1
                    if ((ref_object := portal.get_metadata(ref_path, raise_exception=False)) and
                        (ref_uuid := ref_object.get("uuid"))):  # noqa
                        return ref_uuid, nlookups
        return value, nlookups
