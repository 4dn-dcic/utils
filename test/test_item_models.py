from contextlib import contextmanager
from random import random
from typing import Any, Iterator, Optional, Tuple, Union
from unittest import mock

import pytest

from dcicutils import item_models as item_models_module
from dcicutils.item_models import (
    JsonObject,
    _make_embeddable_property,
    _get_item_identifier,
    PortalItem,
    SubembeddedProperty,
)
from dcicutils.testing_utils import patch_context


SOME_UUID = "uuid1234"
SOME_AT_ID = "/foo/bar/"
SOME_ITEM_PROPERTIES = {"uuid": SOME_UUID, "@id": SOME_AT_ID}
OTHER_ITEM_PROPERTIES = {"uuid": "foo"}
SOME_AUTH = {"key": "some_key", "secret": "some_secret"}
HASHABLE_SOME_AUTH = (("key", "some_key"), ("secret", "some_secret"))


@contextmanager
def patch_make_embeddable_property(**kwargs: Any) -> Iterator[mock.MagicMock]:
    with patch_context(
        item_models_module, "_make_embeddable_property", **kwargs
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_get_and_cache_item_via_auth(**kwargs: Any) -> Iterator[mock.MagicMock]:
    with patch_context(
        item_models_module.PortalItem,
        "_get_and_cache_item_via_auth",
        **kwargs,
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_get_metadata(**kwargs: Any) -> Iterator[mock.MagicMock]:
    with patch_context(
        item_models_module,
        "get_metadata",
        **kwargs,
    ) as mock_item:
        yield mock_item


def mock_portal_item(embed_items: bool = False):
    mock_item = mock.create_autospec(PortalItem, instance=True)
    mock_item.embed_items.return_value = embed_items
    return mock_item


@pytest.mark.parametrize(
    "embed_items,property_value,expected_from_identifier,expected_from_properties",
    [
        (False, "foo", False, False),
        (True, "foo", True, False),
        (False, {}, False, True),
        (False, SOME_ITEM_PROPERTIES, False, True),
        (True, SOME_ITEM_PROPERTIES, True, False),
    ]
)
def test_make_embeddable_property(
    embed_items: bool, property_value: Union[str, JsonObject],
    expected_from_identifier: bool,
    expected_from_properties: bool,
) -> None:
    existing_item = mock_portal_item(embed_items)
    item_to_create = mock_portal_item()
    result = _make_embeddable_property(existing_item, property_value, item_to_create)
    if expected_from_identifier:
        identifier = _get_item_identifier(property_value)
        assert result == item_to_create.from_identifier_and_existing_item.return_value
        item_to_create.from_identifier_and_existing_item.assert_called_once_with(
            identifier, existing_item
        )
    elif expected_from_properties:
        assert result == item_to_create.from_properties_and_existing_item.return_value
        item_to_create.from_properties_and_existing_item.assert_called_once_with(
            property_value, existing_item
        )
    else:
        assert result == property_value


@pytest.mark.parametrize(
    "item,expected",
    [
        ("", ""),
        ({}, ""),
        ("foo", "foo"),
        ({"uuid": "foo"}, "foo"),
    ]
)
def test_get_item_identifier(item: Union[str, JsonObject], expected: str) -> None:
    result = _get_item_identifier(item)
    assert result == expected


def get_portal_item(
    properties: Optional[JsonObject] = None,
    embed_items: Optional[bool] = False,
    auth: Optional[bool] = None,
) -> PortalItem:
    if properties is None:
        properties = SOME_ITEM_PROPERTIES
    if auth is None:
        auth = SOME_AUTH
    return PortalItem(auth=auth, embed_items=embed_items, properties=properties)


class TestPortalItem:

    @pytest.mark.parametrize(
        "item_1_properties,item_2_properties,expected",
        [
            ({}, {}, False),
            ({}, SOME_ITEM_PROPERTIES, False),
            (SOME_ITEM_PROPERTIES, OTHER_ITEM_PROPERTIES, False),
            (SOME_ITEM_PROPERTIES, SOME_ITEM_PROPERTIES, True),
        ]
    )
    def test_equality(
        self,
        item_1_properties: JsonObject, item_2_properties: JsonObject,
        expected: bool,
    ) -> None:
        item_1 = get_portal_item(properties=item_1_properties)
        item_2 = get_portal_item(properties=item_2_properties)
        assert (item_1 == item_2) == expected

    def test_properties(self) -> None:
        assert PortalItem()._properties == {}
        assert get_portal_item()._properties == SOME_ITEM_PROPERTIES

    @pytest.mark.parametrize(
        "properties,expected", [({}, ""), (SOME_ITEM_PROPERTIES, SOME_UUID)]
    )
    def test_uuid(self, properties: JsonObject, expected: str) -> None:
        portal_item = get_portal_item(properties=properties)
        assert portal_item.uuid == expected

    @pytest.mark.parametrize(
        "properties,expected", [({}, ""), (SOME_ITEM_PROPERTIES, SOME_AT_ID)]
    )
    def test_at_id(self, properties: JsonObject, expected: str) -> None:
        portal_item = get_portal_item(properties=properties)
        assert portal_item.at_id == expected

    @pytest.mark.parametrize(
        "comparison_item,expected",
        [
            (get_portal_item(), True),
            (get_portal_item(properties=OTHER_ITEM_PROPERTIES), False),
            (SOME_UUID, True),
            (SOME_AT_ID, True),
            ("some_string", False),
        ]
    )
    def test_is_same_item(self, comparison_item: Any, expected: bool) -> None:
        portal_item = get_portal_item()
        result = portal_item.is_same_item(comparison_item)
        assert result == expected

    def test_get_embeddable_property(self) -> None:
        property_value = "foo"
        item_to_create = mock_portal_item()
        portal_item = get_portal_item()
        with patch_make_embeddable_property() as mock_make_embed:
            portal_item._get_embeddable_property(property_value, item_to_create)
            assert mock_make_embed.called_once_with(
                portal_item, property_value, item_to_create
            )

    def test_get_item_via_auth(self) -> None:
        with patch_get_and_cache_item_via_auth(
            return_value=SOME_ITEM_PROPERTIES
        ) as mock_get_and_cache_item:
            portal_item = get_portal_item()
            result = portal_item._get_item_via_auth(SOME_UUID, SOME_AUTH)
            assert result == SOME_ITEM_PROPERTIES
            mock_get_and_cache_item.assert_called_once_with(
                SOME_UUID, HASHABLE_SOME_AUTH, "frame=object"
            )

    @pytest.mark.parametrize(
        "auth,expected", [({}, tuple()), (SOME_AUTH, HASHABLE_SOME_AUTH)]
    )
    def test_make_hashable_auth(self, auth: JsonObject, expected: Tuple) -> None:
        portal_item = get_portal_item()
        result = portal_item._make_hashable_auth(auth)
        assert result == expected

    @pytest.mark.parametrize(
        "hashable_auth,expected", [(tuple(), {}), (HASHABLE_SOME_AUTH, SOME_AUTH)]
    )
    def test_undo_make_hashable_auth(self, hashable_auth: Tuple, expected: JsonObject) -> None:
        portal_item = get_portal_item()
        result = portal_item._undo_make_hashable_auth(hashable_auth)
        assert result == expected

    @pytest.mark.parametrize(
        "raise_exception,expected", [(False, SOME_ITEM_PROPERTIES), (True, {})]
    )
    def test_get_ad_cache_item_via_auth(self, raise_exception: bool, expected: JsonObject) -> None:
        side_effect = Exception if raise_exception else None
        random_add_on = str(random())  # To differentiate parametrized calls
        with patch_get_metadata(side_effect=side_effect, return_value=SOME_ITEM_PROPERTIES) as mock_get_metadata:
            portal_item = get_portal_item()
            result = portal_item._get_and_cache_item_via_auth(
                SOME_UUID, HASHABLE_SOME_AUTH, add_on=random_add_on
            )
            assert result == expected
            assert len(mock_get_metadata.call_args_list) == 1
            mock_get_metadata.assert_called_once_with(SOME_UUID, key=SOME_AUTH, add_on=random_add_on)

            # Ensure cached
            portal_item._get_and_cache_item_via_auth(SOME_UUID, HASHABLE_SOME_AUTH, add_on=random_add_on)
            assert len(mock_get_metadata.call_args_list) == 1


def get_subembedded_property(properties: Optional[JsonObject] = None, parent_item: Optional[PortalItem] = None) -> SubembeddedProperty:
    properties = properties or SOME_ITEM_PROPERTIES
    return SubembeddedProperty(properties=properties, parent_item=parent_item)


class TestSubembeddedProperty:

    @pytest.mark.parametrize(
        (
            "property_value,parent_item_exists,expected_make_embeddable_property_call,"
            "expected"
        ),
        [
        ]
    )
    def test_get_embeddable_property(self, property_value: Union[JsonObject, str], parent_item_exists: bool, expected_make_embeddable_property_call: bool, expected: Union[PortalItem, str]) -> None:
        parent_item = mock_portal_item() if parent_item_exists else None
        item_to_create = mock_portal_item()
        with patch_make_embeddable_property() as mock_make_embeddable_property:
            subembedded_property = get_subembedded_property(parent_item=parent_item)
            result = subembedded_property._get_embeddable_property(property_value, item_to_create)
            if expected_make_embeddable_property_call:
                mock_make_embeddable_property.assert_called_once_with(parent_item, property_value, item_to_create)
                assert result == mock_make_embeddable_property.return_value
            else:
                assert result == expected
                mock_make_embeddable_property.assert_not_called()
