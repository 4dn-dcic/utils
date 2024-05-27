import re
from typing import Optional
from dcicutils.structured_data import Portal

# This function is exposed (to smaht-portal/ingester and smaht-submitr) only because previously,
# before it was fully developed, we had differing behaviors; but this has been unified; so this
# could now be internalized to structured_data, and portal_object_utils (TODO).


def ref_lookup_strategy(portal: Portal, type_name: str, schema: dict, value: str) -> (int, Optional[str]):
    #
    # Note this slight odd situation WRT object lookups by submitted_id and accession:
    # -----------------------------+-----------------------------------------------+---------------+
    # PATH                         | EXAMPLE                                       | LOOKUP RESULT |
    # -----------------------------+-----------------------------------------------+---------------+
    # /submitted_id                | //UW_FILE-SET_COLO-829BL_HI-C_1               | NOT FOUND     |
    # /UnalignedReads/submitted_id | /UnalignedReads/UW_FILE-SET_COLO-829BL_HI-C_1 | FOUND         |
    # /SubmittedFile/submitted_id  | /SubmittedFile/UW_FILE-SET_COLO-829BL_HI-C_1  | FOUND         |
    # /File/submitted_id           | /File/UW_FILE-SET_COLO-829BL_HI-C_1           | NOT FOUND     |
    # -----------------------------+-----------------------------------------------+---------------+
    # /accession                   | /SMAFSFXF1RO4                                 | FOUND         |
    # /UnalignedReads/accession    | /UnalignedReads/SMAFSFXF1RO4                  | NOT FOUND     |
    # /SubmittedFile/accession     | /SubmittedFile/SMAFSFXF1RO4                   | NOT FOUND     |
    # /File/accession              | /File/SMAFSFXF1RO4                            | FOUND         |
    # -----------------------------+-----------------------------------------------+---------------+
    #
    def ref_validator(schema: Optional[dict],
                      property_name: Optional[str], property_value: Optional[str]) -> Optional[bool]:
        """
        Returns False iff objects of type represented by the given schema, CANNOT be referenced with
        a Portal path using the given property name and its given property value, otherwise returns None.

        For example, if the schema is for UnalignedReads and the property name is accession, then we will
        return False iff the given property value is NOT a properly formatted accession ID; otherwise, we
        will return None, which indicates that the caller (e.g. dcicutils.structured_data.Portal.ref_exists)
        will continue executing its default behavior, which is to check other ways in which the given type
        CANNOT be referenced by the given value, i.e. it checks other identifying properties for the type
        and makes sure any patterns (e.g. for submitted_id or uuid) are ahered to.

        The goal (in structured_data) being to detect if a type is being referenced in such a way that
        CANNOT possibly be allowed, i.e. because none of its identifying types are in the required form,
        if indeed there any requirements. It is assumed/guaranteed the given property name is indeed an
        identifying property for the given type.
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
# See: smaht-portal/.../schema_formats.py/is_accession(instance) ...
def _is_accession_id(value: str) -> bool:
    return isinstance(value, str) and re.match(r"^SMA[1-9A-Z]{9}$", value) is not None
