from typing import Any, Dict

import pytest
from dcicutils import schema_utils


REQUIRED = ["bar", "foo"]
MIXIN_PROPERTIES = [{"$ref": "mixins.json#/link"}]
ANY_OF_REQUIRED = ["baz", "fu"]
ANY_OF = [
        {
            "type": "string"
        },
        {
            "required": ANY_OF_REQUIRED,
        }
]
ONE_OF_REQUIRED = ["baz", "fa"]
ONE_OF = [
        {
            "foo": "bar"
        },
        {
            "required": ONE_OF_REQUIRED,
        }
    ]
CONDITIONAL_REQUIRED = ["bar", "baz", "fa", "foo", "fu"]
IDENTIFYING_PROPERTIES = ["bar", "foo"]
PROPERTIES = {
    "foo": {
        "type": "string",
    },
    "bar": {
        "type": "object",
        "properties": {
            "baz": {
                "type": "string",
            },
        },
    },
    "fun": {
        "type": "array",
        "items": {
            "type": "string",
        },
    },
}
SCHEMA = {
    "required": REQUIRED,
    "anyOf": ANY_OF,
    "oneOf": ONE_OF,
    "identifyingProperties": IDENTIFYING_PROPERTIES,
    "mixinProperties": MIXIN_PROPERTIES,
    "properties": PROPERTIES,
}
FORMAT = "email"
STRING_SCHEMA = {"type": "string", "format": FORMAT}
ARRAY_SCHEMA = {
    "type": "array",
    "items": [STRING_SCHEMA],
}
OBJECT_SCHEMA = {
    "type": "object",
    "properties": {
        "foo": STRING_SCHEMA,
    }
}
NUMBER_SCHEMA = {"type": "number"}
BOOLEAN_SCHEMA = {"type": "boolean"}
INTEGER_SCHEMA = {"type": "integer"}
FORMAT_SCHEMA = {
    "type": "string",
    "format": "date-time",
    "oneOf": [
        {
            "format": "date",
        },
    ],
    "anyOf": [
        {
            "format": "time",
        },
        {
            "format": "date-time",
        },
    ],
}


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, {}),
        (SCHEMA, PROPERTIES),
    ]
)
def test_get_properties(schema: Dict[str, Any], expected: Dict[str, Any]) -> None:
    assert schema_utils.get_properties(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, []),
        (SCHEMA, REQUIRED),
    ]
)
def test_get_required(schema: Dict[str, Any], expected: Dict[str, Any]) -> None:
    assert schema_utils.get_required(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, []),
        (SCHEMA, ANY_OF),
    ]
)
def test_get_any_of(schema: Dict[str, Any], expected: Dict[str, Any]) -> None:
    assert schema_utils.get_any_of(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, []),
        (SCHEMA, ONE_OF),
    ]
)
def test_get_one_of(schema: Dict[str, Any], expected: Dict[str, Any]) -> None:
    assert schema_utils.get_one_of(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, []),
        ({"anyOf": ANY_OF}, ANY_OF_REQUIRED),
        ({"oneOf": ONE_OF}, ONE_OF_REQUIRED),
        ({"required": REQUIRED}, REQUIRED),
        (SCHEMA, CONDITIONAL_REQUIRED),
    ]
)
def test_get_conditional_required(
    schema: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    assert schema_utils.get_conditional_required(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, []),
        (SCHEMA, ANY_OF_REQUIRED),
    ]
)
def test_get_any_of_required(
    schema: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    assert schema_utils.get_any_of_required(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, []),
        (SCHEMA, ONE_OF_REQUIRED),
    ]
)
def test_get_one_of_required(
    schema: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    assert schema_utils.get_one_of_required(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, []),
        (SCHEMA, MIXIN_PROPERTIES),
    ]
)
def test_get_mixin_properties(
    schema: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    assert schema_utils.get_mixin_properties(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, []),
        (SCHEMA, IDENTIFYING_PROPERTIES),
    ]
)
def test_get_identifying_properties(
    schema: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    assert schema_utils.get_identifying_properties(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, ""),
        (STRING_SCHEMA, "string"),
        (ARRAY_SCHEMA, "array"),
        (OBJECT_SCHEMA, "object"),
        (NUMBER_SCHEMA, "number"),
        (BOOLEAN_SCHEMA, "boolean"),
        (INTEGER_SCHEMA, "integer"),
    ]
)
def test_get_schema_type(
    schema: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    assert schema_utils.get_schema_type(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, False),
        (ARRAY_SCHEMA, False),
        (STRING_SCHEMA, True),
    ]
)
def test_is_string_schema(
    schema: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    assert schema_utils.is_string_schema(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, False),
        (STRING_SCHEMA, False),
        (ARRAY_SCHEMA, True),
    ]
)
def test_is_array_schema(
    schema: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    assert schema_utils.is_array_schema(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, False),
        (STRING_SCHEMA, False),
        (OBJECT_SCHEMA, True),
    ]
)
def test_is_object_schema(
    schema: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    assert schema_utils.is_object_schema(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, False),
        (STRING_SCHEMA, False),
        (NUMBER_SCHEMA, True),
    ]
)
def test_is_number_schema(
    schema: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    assert schema_utils.is_number_schema(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, False),
        (STRING_SCHEMA, False),
        (BOOLEAN_SCHEMA, True),
    ]
)
def test_is_boolean_schema(
    schema: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    assert schema_utils.is_boolean_schema(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, False),
        (STRING_SCHEMA, False),
        (INTEGER_SCHEMA, True),
    ]
)
def test_is_integer_schema(
    schema: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    assert schema_utils.is_integer_schema(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, {}),
        (ARRAY_SCHEMA, [STRING_SCHEMA]),
    ]
)
def test_get_items(
    schema: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    assert schema_utils.get_items(schema) == expected


@pytest.mark.parametrize(
    "schema,property_name,expected",
    [
        ({}, "foo", False),
        (ARRAY_SCHEMA, "foo", False),
        (SCHEMA, "foo", True),
    ]
)
def test_has_property(
    schema: Dict[str, Any], property_name: str, expected: Dict[str, Any]
) -> None:
    assert schema_utils.has_property(schema, property_name) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, ""),
        (STRING_SCHEMA, FORMAT),
    ]
)
def test_get_format(
    schema: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    assert schema_utils.get_format(schema) == expected


@pytest.mark.parametrize(
    "schema,expected",
    [
        ({}, []),
        (STRING_SCHEMA, [FORMAT]),
        (FORMAT_SCHEMA, ["date", "date-time", "time"]),
    ]
)
def test_get_conditional_formats(
    schema: Dict[str, Any], expected: Dict[str, Any]
) -> None:
    assert schema_utils.get_conditional_formats(schema) == expected
