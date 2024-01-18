from dcicutils.schema_utils import get_identifying_properties
from dcicutils.portal_utils import Portal
from functools import lru_cache
from typing import List, Optional, Tuple, Union


class PortalObject:

    def __init__(self, portal: Portal, portal_object: dict, portal_object_type: Optional[str] = None) -> None:
        self._portal = portal
        self._object = portal_object
        self._type = portal_object_type if isinstance(portal_object_type, str) and portal_object_type else None

    @property
    @lru_cache(maxsize=1)
    def schema(self):
        return self._portal.get_schema(self.schema_type)

    @property
    @lru_cache(maxsize=1)
    def schema_type(self):
        return self._type or Portal.get_schema_type(self._object)

    @property
    @lru_cache(maxsize=1)
    def schema_types(self):
        return self._type or Portal.get_schema_types(self._object)

    @property
    @lru_cache(maxsize=1)
    def schema_identifying_properties(self) -> list:
        if not (schema := self.schema):
            return []
        return get_identifying_properties(schema)

    @property
    @lru_cache(maxsize=1)
    def identifying_properties(self) -> List[str]:
        """
        Returns the list of all identifying property names of this Portal object which actually have values.
        Implicitly include "uuid" and "identifier" properties as identifying properties if they are actually
        properties in the object schema, and favor these (first); defavor "aliases"; no other ordering defined.
        """
        identifying_properties = []
        for identifying_property in self.schema_identifying_properties:
            if identifying_property not in ["uuid", "identifier", "aliases"]:
                if self._object.get(identifying_property):
                    identifying_properties.append(identifying_property)
        if self._object.get("identifier"):
            identifying_properties.insert(0, "identifier")
        if self._object.get("uuid"):
            identifying_properties.insert(0, "uuid")
        if "aliases" in self.schema_identifying_properties and self._object.get("aliases"):
            identifying_properties.append("aliases")
        return identifying_properties

    @property
    @lru_cache(maxsize=1)
    def identifying_paths(self) -> List[str]:
        """
        Returns a list of the possible Portal URL paths identifying this Portal object.
        """
        if not (identifying_properties := self.identifying_properties):
            return []
        identifying_paths = []
        for identifying_property in identifying_properties:
            if (identifying_value := self._object.get(identifying_property)):
                if identifying_property == "uuid":
                    identifying_paths.append(f"/{identifying_value}")
                # For now at least we include the path both with and without the schema type component
                # as for some identifying values it works (only) with and some it works (only) without.
                # For example: If we have FileSet with "accession", an identifying property, with value
                # SMAFSFXF1RO4 then /SMAFSFXF1RO4 works but /FileSet/SMAFSFXF1RO4 does not; and
                # conversely using "submitted_id", also an identifying property, with value
                # UW_FILE-SET_COLO-829BL_HI-C_1 then /UW_FILE-SET_COLO-829BL_HI-C_1 does
                # not work but /FileSet/UW_FILE-SET_COLO-829BL_HI-C_1 does work.
                elif isinstance(identifying_value, list):
                    for identifying_value_item in identifying_value:
                        identifying_paths.append(f"/{self.schema_type}/{identifying_value_item}")
                        identifying_paths.append(f"/{identifying_value_item}")
                else:
                    identifying_paths.append(f"/{self.schema_type}/{identifying_value}")
                    identifying_paths.append(f"/{identifying_value}")
        return identifying_paths

    @property
    @lru_cache(maxsize=1)
    def identifying_path(self) -> Optional[str]:
        if identifying_paths := self.identifying_paths:
            return identifying_paths[0]

    def lookup(self, include_identifying_path: bool = False) -> Optional[Union[Tuple[dict, str], dict]]:
        return self._lookup() if include_identifying_path else self._lookup()[0]

    def lookup_identifying_path(self) -> Optional[str]:
        return self._lookup()[1]

    def _lookup(self) -> Tuple[Optional[dict], Optional[str]]:
        try:
            for identifying_path in self.identifying_paths:
                if (value := self._portal.get(identifying_path)) and (value.status_code == 200):
                    return value.json(), identifying_path
        except Exception:
            pass
        return None, self.identifying_path
