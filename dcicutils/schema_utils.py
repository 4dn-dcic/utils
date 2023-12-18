from typing import Any, Dict, List


class JsonSchemaConstants:
    ANY_OF = "anyOf"
    ARRAY = "array"
    BOOLEAN = "boolean"
    ENUM = "enum"
    INTEGER = "integer"
    ITEMS = "items"
    NUMBER = "number"
    OBJECT = "object"
    ONE_OF = "oneOf"
    PATTERN = "pattern"
    PROPERTIES = "properties"
    REQUIRED = "required"
    STRING = "string"
    TYPE = "type"


class EncodedSchemaConstants:
    DEFAULT = "default"
    FORMAT = "format"
    IDENTIFYING_PROPERTIES = "identifyingProperties"
    LINK_TO = "linkTo"
    MERGE_REF = "$merge"
    MIXIN_PROPERTIES = "mixinProperties"
    REF = "$ref"
    UNIQUE_KEY = "uniqueKey"


class SchemaConstants(JsonSchemaConstants, EncodedSchemaConstants):
    pass


def get_properties(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Return the properties of a schema."""
    return schema.get(SchemaConstants.PROPERTIES, {})


def get_required(schema: Dict[str, Any]) -> List[str]:
    """Return the required properties of a schema."""
    return schema.get(SchemaConstants.REQUIRED, [])


def get_any_of(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the anyOf properties of a schema."""
    return schema.get(SchemaConstants.ANY_OF, [])


def get_one_of(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the oneOf properties of a schema."""
    return schema.get(SchemaConstants.ONE_OF, [])


def get_conditionally_required_properties(schema: Dict[str, Any]) -> List[str]:
    """Get required + possibly required properties.

    Using heuristics here; update as needed.
    """
    return sorted(
        list(
            set(
                get_required(schema)
                + get_any_of_required_properties(schema)
                + get_one_of_required_properties(schema)
            )
        )
    )


def get_any_of_required_properties(schema: Dict[str, Any]) -> List[str]:
    """Get required properties from anyOf."""
    return [
        property_name
        for any_of_schema in get_any_of(schema)
        for property_name in get_required(any_of_schema)
    ]


def get_one_of_required_properties(schema: Dict[str, Any]) -> List[str]:
    """Get required properties from oneOf."""
    return [
        property_name
        for one_of_schema in get_one_of(schema)
        for property_name in get_required(one_of_schema)
    ]


def get_mixin_properties(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the mixin properties of a schema."""
    return schema.get(EncodedSchemaConstants.MIXIN_PROPERTIES, [])


def get_identifying_properties(schema: Dict[str, Any]) -> List[str]:
    """Return the identifying properties of a schema."""
    return schema.get(EncodedSchemaConstants.IDENTIFYING_PROPERTIES, [])


def get_schema_type(schema: Dict[str, Any]) -> str:
    """Return the type of a schema."""
    return schema.get(SchemaConstants.TYPE, "")


def is_array_schema(schema: Dict[str, Any]) -> bool:
    """Return True if the schema is an array."""
    return get_schema_type(schema) == SchemaConstants.ARRAY


def is_object_schema(schema: Dict[str, Any]) -> bool:
    """Return True if the schema is an object."""
    return get_schema_type(schema) == SchemaConstants.OBJECT


def is_string_schema(schema: Dict[str, Any]) -> bool:
    """Return True if the schema is a string."""
    return get_schema_type(schema) == SchemaConstants.STRING


def is_number_schema(schema: Dict[str, Any]) -> bool:
    """Return True if the schema is a number."""
    return get_schema_type(schema) == SchemaConstants.NUMBER


def is_integer_schema(schema: Dict[str, Any]) -> bool:
    """Return True if the schema is an integer."""
    return get_schema_type(schema) == SchemaConstants.INTEGER


def is_boolean_schema(schema: Dict[str, Any]) -> bool:
    """Return True if the schema is a boolean."""
    return get_schema_type(schema) == SchemaConstants.BOOLEAN


def get_items(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Return the items of a schema."""
    return schema.get(SchemaConstants.ITEMS, {})


def has_property(schema: Dict[str, Any], property_name: str) -> bool:
    """Return True if the schema has the given property."""
    return property_name in get_properties(schema)
