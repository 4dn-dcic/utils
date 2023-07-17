from contextlib import contextmanager
from random import random
from typing import Any, Iterator, Optional, Tuple, Union
from unittest import mock

import pytest

from dcicutils import item_model_utils as item_models_module
from dcicutils.item_model_utils import (
    JsonObject,
    LinkTo,
    get_item_identifier,
    get_link_to,
    PortalItem,
    NestedProperty,
)
from dcicutils.testing_utils import patch_context


SOME_UUID = "uuid1234"
SOME_AT_ID = "/foo/bar/"
SOME_ACCESSION = "GAPXY12345"
SOME_TYPES = ["SomeItemType", "Item"]
SOME_ITEM_PROPERTIES = {
    "uuid": SOME_UUID,
    "@id": SOME_AT_ID,
    "accession": SOME_ACCESSION,
    "@type": SOME_TYPES,
}
OTHER_ITEM_PROPERTIES = {"uuid": "foo"}
SOME_AUTH = {"key": "some_key", "secret": "some_secret"}
HASHABLE_SOME_AUTH = (("key", "some_key"), ("secret", "some_secret"))


@contextmanager
def patch_get_link_to(**kwargs: Any) -> Iterator[mock.MagicMock]:
    with patch_context(item_models_module.get_link_to, **kwargs) as mock_item:
        yield mock_item


@contextmanager
def patch_item_get_link_to(**kwargs: Any) -> Iterator[mock.MagicMock]:
    with patch_context(
        item_models_module.PortalItem._get_link_to,
        **kwargs,
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_nested_get_link_to(**kwargs: Any) -> Iterator[mock.MagicMock]:
    with patch_context(
        item_models_module.NestedProperty._get_link_to,
        **kwargs,
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_get_item_via_auth(**kwargs: Any) -> Iterator[mock.MagicMock]:
    with patch_context(
        item_models_module.PortalItem._get_item_via_auth,
        **kwargs,
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_get_and_cache_item_via_auth(**kwargs: Any) -> Iterator[mock.MagicMock]:
    with patch_context(
        item_models_module.PortalItem._get_and_cache_item_via_auth,
        **kwargs,
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_from_identifier_and_auth(**kwargs: Any) -> Iterator[mock.MagicMock]:
    with patch_context(
        item_models_module.PortalItem.from_identifier_and_auth,
        **kwargs,
    ) as mock_item:
        yield mock_item


@contextmanager
def patch_get_metadata(**kwargs: Any) -> Iterator[mock.MagicMock]:
    with patch_context(
        item_models_module.ff_utils.get_metadata,
        **kwargs,
    ) as mock_item:
        yield mock_item


def mock_portal_item():
    return mock.create_autospec(PortalItem, instance=True)


def get_portal_item(
    properties: Optional[JsonObject] = None,
    fetch_links: Optional[bool] = False,
    auth: Optional[bool] = None,
    use_defaults: Optional[bool] = True,
) -> PortalItem:
    if properties is None and use_defaults:
        properties = SOME_ITEM_PROPERTIES
    if auth is None and use_defaults:
        auth = SOME_AUTH
    return PortalItem(auth=auth, fetch_links=fetch_links, properties=properties)


@pytest.mark.parametrize(
    "fetch_links,link_to,expected_from_identifier,expected_from_properties",
    [
        (False, SOME_UUID, False, False),
        (True, SOME_UUID, True, False),
        (True, SOME_ITEM_PROPERTIES, True, False),
        (False, SOME_ITEM_PROPERTIES, False, True),
    ],
)
def test_get_link_to(
    fetch_links: bool,
    link_to: LinkTo,
    expected_from_identifier: bool,
    expected_from_properties: bool,
) -> None:
    existing_item = get_portal_item(fetch_links=fetch_links)
    item_to_create = mock_portal_item()
    result = get_link_to(existing_item, link_to, item_to_create)
    if expected_from_identifier:
        identifier = get_item_identifier(link_to)
        assert result == item_to_create.from_identifier_and_existing_item.return_value
        item_to_create.from_identifier_and_existing_item.assert_called_once_with(
            identifier, existing_item
        )
    elif expected_from_properties:
        assert result == item_to_create.from_properties_and_existing_item.return_value
        item_to_create.from_properties_and_existing_item.assert_called_once_with(
            link_to, existing_item
        )
    else:
        assert result == link_to


@pytest.mark.parametrize(
    "item,expected",
    [
        ("", ""),
        ({}, ""),
        ("foo", "foo"),
        ({"uuid": "foo"}, "foo"),
    ],
)
def test_get_item_identifier(item: Union[str, JsonObject], expected: str) -> None:
    result = get_item_identifier(item)
    assert result == expected


class TestPortalItem:
    @pytest.mark.parametrize(
        "properties,expected", [({}, ""), (SOME_ITEM_PROPERTIES, SOME_UUID)]
    )
    def test_get_uuid(self, properties: JsonObject, expected: str) -> None:
        portal_item = get_portal_item(properties=properties)
        assert portal_item.get_uuid() == expected

    @pytest.mark.parametrize(
        "properties,expected", [({}, ""), (SOME_ITEM_PROPERTIES, SOME_AT_ID)]
    )
    def test_get_at_id(self, properties: JsonObject, expected: str) -> None:
        portal_item = get_portal_item(properties=properties)
        assert portal_item.get_at_id() == expected

    @pytest.mark.parametrize(
        "properties,expected", [({}, ""), (SOME_ITEM_PROPERTIES, SOME_ACCESSION)]
    )
    def test_get_accession(self, properties: JsonObject, expected: str) -> None:
        portal_item = get_portal_item(properties=properties)
        assert portal_item.get_accession() == expected

    @pytest.mark.parametrize(
        "properties,expected", [({}, []), (SOME_ITEM_PROPERTIES, SOME_TYPES)]
    )
    def test_get_types(self, properties: JsonObject, expected: str) -> None:
        portal_item = get_portal_item(properties=properties)
        assert portal_item.get_types() == expected

    def test_get_link_tos(self) -> None:
        link_tos = [SOME_UUID, SOME_ACCESSION]
        item_to_create = mock_portal_item()
        portal_item = get_portal_item()
        with patch_item_get_link_to() as mock_get_link_to:
            portal_item._get_link_tos(link_tos, item_to_create)
            assert len(mock_get_link_to.call_args_list) == len(link_tos)
            for link_to in link_tos:
                mock_get_link_to.assert_any_call(link_to, item_to_create)

    def test_get_link_to(self) -> None:
        link_to = "foo"
        item_to_create = mock_portal_item()
        portal_item = get_portal_item()
        with patch_get_link_to() as mock_get_link_to:
            portal_item._get_link_to(link_to, item_to_create)
            assert mock_get_link_to.called_once_with(
                portal_item, link_to, item_to_create
            )

    def test_from_properties(self) -> None:
        properties = OTHER_ITEM_PROPERTIES
        fetch_links = True
        auth = SOME_AUTH
        portal_item = get_portal_item()
        result = portal_item.from_properties(
            properties, fetch_links=fetch_links, auth=auth
        )
        assert isinstance(result, PortalItem)
        assert result.get_properties() == properties
        assert result.should_fetch_links() == fetch_links
        assert result.get_auth() == auth

    def test_from_identifier_and_auth(self) -> None:
        with patch_get_item_via_auth(
            return_value=OTHER_ITEM_PROPERTIES
        ) as mock_get_item_via_auth:
            identifier = SOME_UUID
            fetch_links = True
            portal_item = get_portal_item()
            result = portal_item.from_identifier_and_auth(
                identifier, SOME_AUTH, fetch_links=fetch_links
            )
            assert isinstance(result, PortalItem)
            assert result.should_fetch_links() == fetch_links
            assert result.get_auth() == SOME_AUTH
            mock_get_item_via_auth.assert_called_once_with(identifier, SOME_AUTH)

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
    def test_undo_make_hashable_auth(
        self, hashable_auth: Tuple, expected: JsonObject
    ) -> None:
        portal_item = get_portal_item()
        result = portal_item._undo_make_hashable_auth(hashable_auth)
        assert result == expected

    @pytest.mark.parametrize(
        "raise_exception,expected", [(False, SOME_ITEM_PROPERTIES), (True, {})]
    )
    def test_get_and_cache_item_via_auth(
        self, raise_exception: bool, expected: JsonObject
    ) -> None:
        side_effect = Exception if raise_exception else None
        random_add_on = str(random())  # To differentiate parametrized calls
        with patch_get_metadata(
            side_effect=side_effect, return_value=SOME_ITEM_PROPERTIES
        ) as mock_get_metadata:
            portal_item = get_portal_item()
            result = portal_item._get_and_cache_item_via_auth(
                SOME_UUID, HASHABLE_SOME_AUTH, add_on=random_add_on
            )
            assert result == expected
            assert len(mock_get_metadata.call_args_list) == 1
            mock_get_metadata.assert_called_once_with(
                SOME_UUID, key=SOME_AUTH, add_on=random_add_on
            )

            # Ensure cached
            portal_item._get_and_cache_item_via_auth(
                SOME_UUID, HASHABLE_SOME_AUTH, add_on=random_add_on
            )
            assert len(mock_get_metadata.call_args_list) == 1

    @pytest.mark.parametrize(
        "auth,fetch_links,exception_expected",
        [
            (None, False, True),
            (None, True, True),
            (SOME_AUTH, False, False),
            (SOME_AUTH, True, False),
        ],
    )
    def test_from_identifier_and_existing_item(
        self, auth: JsonObject, fetch_links: bool, exception_expected: bool
    ) -> None:
        identifier = SOME_UUID
        portal_item = get_portal_item(
            auth=auth, fetch_links=fetch_links, use_defaults=False
        )
        with patch_from_identifier_and_auth() as mock_from_identifier_and_auth:
            if exception_expected:
                with pytest.raises(RuntimeError):
                    PortalItem.from_identifier_and_existing_item(
                        identifier, portal_item
                    )
            else:
                result = PortalItem.from_identifier_and_existing_item(
                    identifier, portal_item
                )
                assert result == mock_from_identifier_and_auth.return_value
                mock_from_identifier_and_auth.assert_called_once_with(
                    identifier, auth, fetch_links=fetch_links
                )

    def test_from_properties_and_existing_item(self) -> None:
        properties = OTHER_ITEM_PROPERTIES
        portal_item = get_portal_item()
        result = PortalItem.from_properties_and_existing_item(properties, portal_item)
        assert isinstance(result, PortalItem)
        assert result.get_auth() == portal_item.get_auth()
        assert result.should_fetch_links() == portal_item.should_fetch_links()


def get_nested_property(
    properties: Optional[JsonObject] = None, parent_item: Optional[PortalItem] = None
) -> NestedProperty:
    properties = properties or SOME_ITEM_PROPERTIES
    return NestedProperty(properties=properties, parent_item=parent_item)


class TestNestedProperty:
    def test_get_link_tos(self) -> None:
        link_tos = [SOME_UUID, SOME_ACCESSION]
        item_to_create = mock_portal_item()
        nested_property = get_nested_property()
        with patch_nested_get_link_to() as mock_get_link_to:
            nested_property._get_link_tos(link_tos, item_to_create)
            assert len(mock_get_link_to.call_args_list) == len(link_tos)
            for link_to in link_tos:
                mock_get_link_to.assert_any_call(link_to, item_to_create)

    @pytest.mark.parametrize(
        "link_to,parent_item,expected_get_link_to_call,expected_item_from_properties",
        [
            (SOME_UUID, None, False, False),
            (SOME_ITEM_PROPERTIES, None, False, True),
            (SOME_UUID, get_portal_item(), True, False),
            (SOME_ITEM_PROPERTIES, get_portal_item(), True, False),
        ],
    )
    def test_get_link_to(
        self,
        link_to: LinkTo,
        parent_item: Union[PortalItem, None],
        expected_get_link_to_call: bool,
        expected_item_from_properties: bool,
    ) -> None:
        item_to_create = mock_portal_item()
        nested_property = get_nested_property(parent_item=parent_item)
        with patch_get_link_to() as mock_get_link_to:
            result = nested_property._get_link_to(link_to, item_to_create)
            if expected_get_link_to_call:
                mock_get_link_to.assert_called_once_with(
                    parent_item, link_to, item_to_create
                )
                assert result == mock_get_link_to.return_value
            elif expected_item_from_properties:
                item_to_create.from_properties.assert_called_once_with(link_to)
                assert result == item_to_create.from_properties.return_value
            else:
                assert result == link_to

    def test_from_properties(self) -> None:
        properties = OTHER_ITEM_PROPERTIES
        parent_item = mock_portal_item()
        nested_property = get_nested_property()
        result = nested_property.from_properties(properties, parent_item=parent_item)
        assert isinstance(result, NestedProperty)
        assert result.get_properties() == properties
        assert result.get_parent_item() == parent_item
