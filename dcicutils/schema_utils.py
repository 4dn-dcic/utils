from typing import Any, Dict, List


class JsonSchemaConstants:
    ANY_OF = "anyOf"
    ARRAY = "array"
    BOOLEAN = "boolean"
    DEFAULT = "default"
    ENUM = "enum"
    FORMAT = "format"
    INTEGER = "integer"
    ITEMS = "items"
    NUMBER = "number"
    OBJECT = "object"
    ONE_OF = "oneOf"
    PATTERN = "pattern"
    PROPERTIES = "properties"
    REF = "$ref"
    REQUIRED = "required"
    STRING = "string"
    TYPE = "type"


class EncodedSchemaConstants:
    IDENTIFYING_PROPERTIES = "identifyingProperties"
    LINK_TO = "linkTo"
    MERGE_REF = "$merge"
    MIXIN_PROPERTIES = "mixinProperties"
    UNIQUE_KEY = "uniqueKey"


class SchemaConstants(JsonSchemaConstants, EncodedSchemaConstants):
    pass


def get_properties(schema: Dict[str, Any]) -> Dict[str, Any]:
    """Return the properties of a schema."""
    return schema.get(SchemaConstants.PROPERTIES, {})


def get_property(schema: Dict[str, Any], property_name: str) -> Dict[str, Any]:
    """Return property in properties, if found."""
    return get_properties(schema).get(property_name, {})


def has_property(schema: Dict[str, Any], property_name: str) -> bool:
    """Return True if the schema has the given property."""
    return property_name in get_properties(schema)


def get_required(schema: Dict[str, Any]) -> List[str]:
    """Return the required properties of a schema."""
    return schema.get(SchemaConstants.REQUIRED, [])


def get_pattern(schema: Dict[str, Any]) -> str:
    """Return the pattern property of a schema."""
    return schema.get(SchemaConstants.PATTERN, "")


def get_any_of(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the anyOf properties of a schema."""
    return schema.get(SchemaConstants.ANY_OF, [])


def get_one_of(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the oneOf properties of a schema."""
    return schema.get(SchemaConstants.ONE_OF, [])


def get_conditional_required(schema: Dict[str, Any]) -> List[str]:
    """Get required + possibly required properties.

    Using heuristics here; update as needed.
    """
    return sorted(
        list(
            set(
                get_required(schema)
                + get_any_of_required(schema)
                + get_one_of_required(schema)
            )
        )
    )


def get_any_of_required(schema: Dict[str, Any]) -> List[str]:
    """Get required properties from anyOf."""
    return [
        property_name
        for any_of_schema in get_any_of(schema)
        for property_name in get_required(any_of_schema)
    ]


def get_one_of_required(schema: Dict[str, Any]) -> List[str]:
    """Get required properties from oneOf."""
    return [
        property_name
        for one_of_schema in get_one_of(schema)
        for property_name in get_required(one_of_schema)
    ]


def get_mixin_properties(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return the mixin properties of a schema."""
    return schema.get(SchemaConstants.MIXIN_PROPERTIES, [])


def get_identifying_properties(schema: Dict[str, Any]) -> List[str]:
    """Return the identifying properties of a schema."""
    return schema.get(SchemaConstants.IDENTIFYING_PROPERTIES, [])


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


def get_format(schema: Dict[str, Any]) -> str:
    """Return the format of a schema."""
    return schema.get(SchemaConstants.FORMAT, "")


def get_conditional_formats(schema: Dict[str, Any]) -> List[str]:
    """Return the format of a schema, as directly given or as listed
    as an option via oneOf or anyOf.
    """
    formats = set(
        get_any_of_formats(schema) + get_one_of_formats(schema) + [get_format(schema)]
    )
    return sorted(list([format_ for format_ in formats if format_]))


def get_any_of_formats(schema: Dict[str, Any]) -> List[str]:
    """Return the formats of a schema's anyOf properties."""
    return [
        get_format(any_of_schema)
        for any_of_schema in get_any_of(schema)
        if get_format(any_of_schema)
    ]


def get_one_of_formats(schema: Dict[str, Any]) -> List[str]:
    """Return the formats of a schema's oneOf properties."""
    return [
        get_format(one_of_schema)
        for one_of_schema in get_one_of(schema)
        if get_format(one_of_schema)
    ]
