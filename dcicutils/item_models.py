from __future__ import annotations
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Mapping, Optional, Tuple, Union

import structlog

from .ff_utils import get_metadata


logger = structlog.getLogger(__name__)

JsonObject = Mapping[str, Any]


def _make_embeddable_property(
    existing_item: PortalItem,
    property_value: [str, JsonObject],
    item_to_create: PortalItem,
) -> Union[str, JsonObject, PortalItem]:
    embed_items = existing_item.embed_items()
    identifier = _get_item_identifier(property_value)
    if embed_items and identifier:
        return item_to_create.from_identifier_and_existing_item(
            identifier, existing_item
        )
    if isinstance(property_value, Mapping):
        return item_to_create.from_properties_and_existing_item(
            property_value, existing_item
        )
    return property_value


def _get_item_identifier(item: Union[str, JsonObject]) -> str:
    if isinstance(item, str):
        return item
    if isinstance(item, Mapping):
        return item.get(PortalItem.UUID, "")
    raise ValueError()


@dataclass(frozen=True)
class PortalItem:
    AT_ID = "@id"
    UUID = "uuid"

    IDENTIFYING_PROPERTIES = [AT_ID, UUID]

    properties: Optional[JsonObject] = field(default=None, hash=False)
    auth: Optional[JsonObject] = field(default=None, hash=False)
    do_embeds: Optional[bool] = field(default=False, hash=False)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.uuid})"

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, PortalItem) and self._uuid:
            return self._uuid == other._uuid
        return False

    @property
    def _properties(self) -> JsonObject:
        return self.properties or {}

    @property
    def _uuid(self) -> str:
        return self._properties.get(self.UUID, "")

    @property
    def _at_id(self) -> str:
        return self._properties.get(self.AT_ID, "")

    def is_same_item(self, item: Any) -> bool:
        if isinstance(item, PortalItem):
            return self == item
        if isinstance(item, dict):
            return self._has_same_identifier(item)
        if isinstance(item, str):
            return self._is_same_identifier(item)
        return False

    def _has_same_identifier(self, item: Mapping) -> bool:
        return any(
            [
                self._properties.get(identifying_property) == item.get(identifying_property)
                for identifying_property in self.IDENTIFYING_PROPERTIES
            ]
        )

    def _is_same_identifier(self, identifier: str) -> bool:
        return any(
            [
                self._properties.get(identifying_property) == identifier
                for identifying_property in self.IDENTIFYING_PROPERTIES
            ]
        )

    def do_embeds(self) -> bool:
        return self.do_embeds

    def get_auth(self) -> Union[JsonObject, None]:
        return self.auth

    def get_properties(self) -> JsonObject:
        return self._properties

    def _get_embeddable_property(
        self,
        property_value: [str, JsonObject],
        item_to_create: PortalItem,
    ) -> Union[str, JsonObject, PortalItem]:
        return _make_embeddable_property(self, property_value, item_to_create)

    @classmethod
    def from_properties(
        cls, properties: JsonObject, embed_items=False, auth=None, **kwargs: Any
    ) -> PortalItem:
        return cls(properties=properties, embed_items=embed_items, auth=auth)

    @classmethod
    def from_identifier_and_auth(
        cls, identifier: str, auth: JsonObject, embed_items=False, **kwargs: Any
    ) -> PortalItem:
        properties = cls._get_item_via_auth(identifier, auth)
        return cls.from_properties(properties=properties, auth=auth, embed_items=embed_items)

    @classmethod
    def _get_item_via_auth(
        cls, identifier: str, auth: JsonObject, add_on: Optional[str] = "frame=object"
    ) -> JsonObject:
        hashable_auth = cls._make_hashable_auth(auth)
        return cls._get_and_cache_item_via_auth(identifier, hashable_auth, add_on)

    @classmethod
    def _make_hashable_auth(cls, auth: Mapping[str, str]) -> Tuple[Tuple[str, str]]:
        """Assuming nothing nested here..."""
        return tuple(auth.items())

    @classmethod
    def _undo_make_hashable_auth(
        cls, hashable_auth: Tuple[Tuple[str, str]]
    ) -> JsonObject:
        return dict(hashable_auth)

    @classmethod
    @lru_cache(maxsize=256)
    def _get_and_cache_item_via_auth(
        cls, identifier: str, hashable_auth: Tuple[Tuple[str, Any]], add_on: Optional[str] = None
    ) -> JsonObject:
        """Save on requests by caching items."""
        auth = cls._undo_make_hashable_auth(hashable_auth)
        try:
            result = get_metadata(identifier, key=auth, add_on=add_on)
        except Exception as e:
            result = {}
            logger.error(f"Error getting metadata for {identifier}: {e}")
        return result

    @classmethod
    def from_identifier_and_existing_item(
        cls, identifier: str, existing_item: PortalItem, **kwargs: Any
    ) -> PortalItem:
        embed_items = existing_item.embed_items()
        auth = existing_item.get_auth()
        if auth:
            return cls.from_identifier_and_auth(
                identifier, auth, embed_items=embed_items
            )
        raise ValueError("Unable to create item from existing item")

    @classmethod
    def from_properties_and_existing_item(
        cls, properties: JsonObject, existing_item: PortalItem, **kwargs: Any
    ) -> PortalItem:
        embed_items = existing_item.embed_items()
        auth = existing_item.get_auth()
        return cls.from_properties(
            properties, embed_items=embed_items, auth=auth
        )


@dataclass(frozen=True)
class SubembeddedProperty:
    properties: Optional[JsonObject] = field(default=None, hash=False)
    parent_item: Optional[PortalItem] = field(default=None, hash=False)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(parent={self.parent_item.__repr__()})"

    @property
    def _properties(self) -> JsonObject:
        return self.properties or {}

    def _get_embeddable_property(
        self,
        property_value: [str, JsonObject],
        item_to_create: PortalItem,
    ) -> Union[str, JsonObject, PortalItem]:
        if self.parent_item:
            return _make_embeddable_property(
                self.parent_item, property_value, item_to_create
            )
        if isinstance(property_value, Mapping):
            return item_to_create.from_properties(property_value)
        return property_value

    @classmethod
    def from_properties(
        cls, properties: JsonObject, parent_item: Optional[PortalItem], **kwargs: Any
    ) -> SubembeddedProperty:
        return cls(properties=properties, parent_item=parent_item)
