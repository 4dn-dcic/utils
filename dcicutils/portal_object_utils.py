from functools import lru_cache
import re
from typing import Any, Callable, List, Optional, Tuple, Type, Union
from dcicutils.data_readers import RowReader
from dcicutils.portal_utils import Portal
from dcicutils.schema_utils import Schema

PortalObject = Type["PortalObject"]  # Forward type reference for type hints.


class PortalObject:

    def __init__(self, portal: Portal, portal_object: dict, portal_object_type: Optional[str] = None) -> None:
        self._portal = portal
        self._data = portal_object
        self._type = portal_object_type if isinstance(portal_object_type, str) and portal_object_type else None

    @property
    def data(self):
        return self._data

    @property
    @lru_cache(maxsize=1)
    def type(self):
        return self._type or Portal.get_schema_type(self._data)

    @property
    @lru_cache(maxsize=1)
    def types(self):
        return self._type or Portal.get_schema_types(self._data)

    @property
    @lru_cache(maxsize=1)
    def uuid(self) -> Optional[str]:
        return self._data.get("uuid") if isinstance(self._data, dict) else None

    @property
    @lru_cache(maxsize=1)
    def schema(self):
        return self._portal.get_schema(self.type)

    @property
    @lru_cache(maxsize=1)
    def identifying_properties(self) -> List[str]:
        """
        Returns the list of all identifying property names of this Portal object which actually have values.
        Implicitly include "uuid" and "identifier" properties as identifying properties if they are actually
        properties in the object schema, and favor these (first); defavor "aliases"; no other ordering defined.
        """
        if not (schema := self.schema) or not (schema_identifying_properties := schema.get("identifyingProperties")):
            return []
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
                        identifying_paths.append(f"/{self.type}/{identifying_value_item}")
                        identifying_paths.append(f"/{identifying_value_item}")
                else:
                    identifying_paths.append(f"/{self.type}/{identifying_value}")
                    identifying_paths.append(f"/{identifying_value}")
        return identifying_paths

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
            for identifying_path in self.identifying_paths:
                if (value := self._portal.get(identifying_path, raw=raw)) and (value.status_code == 200):
                    return PortalObject(self._portal, value.json(), self.type if raw else None), identifying_path
        except Exception:
            pass
        return None, self.identifying_path

    def compare(self, value: Union[dict, PortalObject], consider_link_to: bool = False) -> dict:
        """
        Compares this Portal object against the given Portal object value; noting differences values of properites
        which they have in common; and properties which are in this Portal object and not in the given Portal object;
        we do NOT check the converse, i.e. properties in the given Portal object which are not in this Portal object.
        Returns a dictionary with a description of the differences. If the given consider_link_to flag is True then
        for differences detected linkTo reference values, we will actually check that the object which is being
        referenced is different or the same, e.g. the file_format reference (linkTo) property value "fastq" looks
        different from "eb417c0a-70dd-42e3-9841-ac7f1ee22962" but they (may) refer to the same object.
        """
        def are_properties_equal(property_path: str, property_value_a: Any, property_value_b: Any) -> bool:
            if property_value_a == property_value_b:
                return True
            nonlocal self
            if (schema := self.schema) and (property_type := Schema.get_property_by_path(schema, property_path)):
                if link_to := property_type.get("linkTo"):
                    """
                    This works basically except not WRT sub/super-types (e.g. CellCultureSample vs Sample);
                    this is only preferable as it only requires one Portal GET rather than two, as below.
                    if (a := self._portal.get(f"/{link_to}/{property_value_a}")) and (a.status_code == 200):
                        if a_identifying_paths := PortalObject(self._portal, a.json()).identifying_paths:
                            if f"/{link_to}/{property_value_b}" in a_identifying_paths:
                                return True
                    """
                    if a := self._portal.get(f"/{link_to}/{property_value_a}", raw=True):
                        if (a.status_code == 200) and (a := a.json()):
                            if b := self._portal.get(f"/{link_to}/{property_value_b}", raw=True):
                                if (b.status_code == 200) and (b := b.json()):
                                    return a == b
            return False
        return PortalObject._compare(self._data, value.data if isinstance(value, PortalObject) else value,
                                     compare=are_properties_equal if consider_link_to else None)

    _ARRAY_KEY_REGULAR_EXPRESSION = re.compile(rf"^({Schema._ARRAY_NAME_SUFFIX_CHAR}\d+)$")

    @staticmethod
    def _compare(a: dict, b: dict, compare: Optional[Callable] = None, _path: Optional[str] = None) -> dict:
        def key_to_path(key: str) -> Optional[str]:  # noqa
            nonlocal _path
            if match := PortalObject._ARRAY_KEY_REGULAR_EXPRESSION.search(key):
                return f"{_path}{match.group(1)}" if _path else match.group(1)
            return f"{_path}.{key}" if _path else key
        def list_to_dictionary(value: list) -> dict:  # noqa
            result = {}
            for index, item in enumerate(value):
                result[f"#{index}"] = item
            return result
        diffs = {}
        for key in a:
            path = key_to_path(key)
            if key not in b:
                if a[key] != RowReader.CELL_DELETION_SENTINEL:
                    diffs[path] = {"value": a[key], "creating_value": True}
            else:
                if isinstance(a[key], dict) and isinstance(b[key], dict):
                    diffs.update(PortalObject._compare(a[key], b[key], compare=compare, _path=path))
                elif isinstance(a[key], list) and isinstance(b[key], list):
                    # Note that lists will be compared in order, which means the when dealing with
                    # insertions/deletions to/from the list, we my easily mistakenly regard elements
                    # of the list to be different when they are really the same, since they occupy
                    # different indices within the array. This is just a known restriction of this
                    # comparison functionality; and perhaps actually technically correct, but probably
                    # in practice, at the application/semantic level, we likely regard the order of
                    # lists as unimportant, and with a little more work here we could try to detect
                    # and exclude from the diffs for a list, those elements in the list which are
                    # equal to each other but which reside at different indices with then two lists.
                    diffs.update(PortalObject._compare(list_to_dictionary(a[key]),
                                                       list_to_dictionary(b[key]), compare=compare, _path=path))
                elif a[key] != b[key]:
                    if a[key] == RowReader.CELL_DELETION_SENTINEL:
                        diffs[path] = {"value": b[key], "deleting_value": True}
                    elif not callable(compare) or not compare(path, a[key], b[key]):
                        diffs[path] = {"value": a[key], "updating_value": b[key]}
        return diffs
