import os
from typing import Any, Dict, List, Optional, Tuple
from dcicutils.misc_utils import to_camel_case


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


class Schema:

    def __init__(self, schema: dict, type: Optional[str] = None) -> None:
        self._data = schema if isinstance(schema, dict) else (schema.data if isinstance(schema, Schema) else {})
        self._type = ((type if isinstance(type, str) else "") or
                      Schema.type_name(self._data.get("title", "")) or
                      Schema.type_name(self._data.get("$id", "")))

    @property
    def data(self) -> dict:
        return self._data

    @property
    def type(self) -> str:
        return self._type

    @staticmethod
    def type_name(value: str) -> Optional[str]:  # File or other name.
        if isinstance(value, str) and (value := os.path.basename(value.replace(" ", ""))):
            return to_camel_case(value[0:dot] if (dot := value.rfind(".")) >= 0 else value)

    def property_by_path(self, property_path: str) -> Optional[dict]:
        """
        Looks for the given property path within this Portal schema and returns its dictionary value.
        This given property path can be either a simple property name, or a series of dot-separated
        property names representing nested (object) properties; and/or the property names may also
        be suffixed with a pound (#) characteter, optionally followed by an integer, representing
        an array type property and its optional array index (this integer, if specified, is ignored
        for the purposes of this function, but it may have been created by another process/function,
        for example by PortalObject.compare). If the property is not found then None is returned.
        """
        return Schema.get_property_by_path(self._data, property_path)

    _ARRAY_NAME_SUFFIX_CHAR = "#"
    _DOTTED_NAME_DELIMITER_CHAR = "."

    @staticmethod
    def get_property_by_path(schema: dict, property_path: str) -> Optional[dict]:
        if not isinstance(schema, dict) or not isinstance(property_path, str):
            return None
        elif not (schema_properties := schema.get(JsonSchemaConstants.PROPERTIES)):
            return None
        property_paths = property_path.split(Schema._DOTTED_NAME_DELIMITER_CHAR)
        for property_index, property_name in enumerate(property_paths):
            property_name, array_specifiers = Schema._unarrayize_property_name(property_name)
            if not (property_value := schema_properties.get(property_name)):
                return None
            elif (property_type := property_value.get(JsonSchemaConstants.TYPE)) == JsonSchemaConstants.OBJECT:
                property_paths_tail = Schema._DOTTED_NAME_DELIMITER_CHAR.join(property_paths[property_index + 1:])
                return Schema.get_property_by_path(property_value, property_paths_tail)
            elif (property_type := property_value.get(JsonSchemaConstants.TYPE)) == JsonSchemaConstants.ARRAY:
                if not array_specifiers:
                    if property_index == len(property_paths) - 1:
                        return property_value
                    return None
                for array_index in range(len(array_specifiers)):
                    if property_type != JsonSchemaConstants.ARRAY:
                        return None
                    elif not (array_items := property_value.get(JsonSchemaConstants.ITEMS)):
                        return None
                    property_type = (property_value := array_items).get(JsonSchemaConstants.TYPE)
                if property_type == JsonSchemaConstants.OBJECT:
                    if property_index == len(property_paths) - 1:
                        return property_value
                    property_paths_tail = Schema._DOTTED_NAME_DELIMITER_CHAR.join(property_paths[property_index + 1:])
                    return Schema.get_property_by_path(property_value, property_paths_tail)
        return property_value

    @staticmethod
    def _unarrayize_property_name(property_name: str) -> Tuple[str, Optional[List[int]]]:
        if len(components := (property_name := property_name.strip()).split(Schema._ARRAY_NAME_SUFFIX_CHAR)) < 2:
            return property_name, None
        unarrayized_property_name = components[0].strip()
        array_specifiers = []
        for component in components[1:]:
            if component.isdigit():
                array_specifiers.append(int(component))
            elif component == "":
                array_specifiers.append(0)
            else:
                return property_name, None
        return unarrayized_property_name, array_specifiers
