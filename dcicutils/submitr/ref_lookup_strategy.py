import re
from typing import Optional
from dcicutils.structured_data import Portal


def ref_lookup_strategy(portal: Portal, type_name: str, schema: dict, value: str) -> (int, Optional[str]):
    #
    # FYI: Note this situation WRT object lookups ...
    #
    # /{submitted_id}                # NOT FOUND
    # /UnalignedReads/{submitted_id} # OK
    # /SubmittedFile/{submitted_id}  # OK
    # /File/{submitted_id}           # NOT FOUND
    #
    # /{accession}                   # OK
    # /UnalignedReads/{accession}    # NOT FOUND
    # /SubmittedFile/{accession}     # NOT FOUND
    # /File/{accession}              # OK
    #
    def ref_validator(schema: Optional[dict],
                      property_name: Optional[str], property_value: Optional[str]) -> Optional[bool]:
        """
        Returns False iff the type represented by the given schema, can NOT be referenced by
        the given property name with the given property value, otherwise returns None.

        For example, if the schema is for the UnalignedReads type and the property name
        is accession, then we will return False iff the given property value is NOT a properly
        formatted accession ID. Otherwise, we will return None, which indicates that the
        caller (in dcicutils.structured_data.Portal.ref_exists) will continue executing
        its default behavior, which is to check other ways in which the given type can NOT
        be referenced by the given value, i.e. it checks other identifying properties for
        the type and makes sure any patterns (e.g. for submitted_id or uuid) are ahered to.

        The goal (in structured_data) being to detect if a type is being referenced in such
        a way that cannot possibly be allowed, i.e. because none of its identifying types
        are in the required form (if indeed there any requirements). Note that it is guaranteed
        that the given property name is indeed an identifying property for the given type.
        """
        if property_format := schema.get("properties", {}).get(property_name, {}).get("format"):
            if (property_format == "accession") and (property_name == "accession"):
                if not _is_accession_id(property_value):
                    return False
        return None

    DEFAULT_RESPONSE = (Portal.LOOKUP_DEFAULT, ref_validator)

    if not value:
        return DEFAULT_RESPONSE
    if not schema:
        if not isinstance(portal, Portal) or not (schema := portal.get_schema(type_name)):
            return DEFAULT_RESPONSE
    if schema_properties := schema.get("properties"):
        if schema_properties.get("accession") and _is_accession_id(value):
            # Case: lookup by accession (only by root).
            return Portal.LOOKUP_ROOT, ref_validator
        elif schema_property_info_submitted_id := schema_properties.get("submitted_id"):
            if schema_property_pattern_submitted_id := schema_property_info_submitted_id.get("pattern"):
                if re.match(schema_property_pattern_submitted_id, value):
                    # Case: lookup by submitted_id (only by specified type).
                    return Portal.LOOKUP_SPECIFIED_TYPE, ref_validator
    return DEFAULT_RESPONSE


# This is here for now because of problems with circular dependencies.
# See: smaht-portal/.../schema_formats.py
def _is_accession_id(value: str) -> bool:
    return isinstance(value, str) and re.match(r"^SMA[1-9A-Z]{9}$", value) is not None
