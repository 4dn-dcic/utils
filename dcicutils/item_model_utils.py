from __future__ import annotations

from dataclasses import dataclass, field
from functools import lru_cache
from typing import Any, Iterable, List, Mapping, Optional, Tuple, Union

import structlog

from . import ff_utils


logger = structlog.getLogger(__name__)

JsonObject = Mapping[str, Any]
LinkTo = Union[str, JsonObject]


def get_link_to(
    existing_item: PortalItem,
    link_to: LinkTo,
    item_to_create: PortalItem,
) -> Union[str, PortalItem]:
    """Create new item model from existing one for given linkTo.

    LinkTos be identifiers (UUIDs) or (partially) embedded objects.

    Follow rules of existing item model for fetching linkTo via
    request. If not fetching via request, then make item model from
    existing properties if possible.
    """
    fetch_links = existing_item.should_fetch_links()
    identifier = get_item_identifier(link_to)
    if fetch_links and identifier:
        return item_to_create.from_identifier_and_existing_item(
            identifier, existing_item
        )
    if isinstance(link_to, Mapping):
        return item_to_create.from_properties_and_existing_item(link_to, existing_item)
    return link_to


def get_item_identifier(item: LinkTo) -> str:
    if isinstance(item, str):
        return item
    if isinstance(item, Mapping):
        return item.get(PortalItem.UUID, "")


@dataclass(frozen=True)
class PortalItem:
    ACCESSION = "accession"
    AT_ID = "@id"
    TYPE = "@type"
    UUID = "uuid"

    properties: JsonObject
    auth: Optional[JsonObject] = field(default=None, hash=False)
    fetch_links: Optional[bool] = field(default=False, hash=False)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self._uuid})"

    @property
    def _uuid(self) -> str:
        return self.properties.get(self.UUID, "")

    @property
    def _at_id(self) -> str:
        return self.properties.get(self.AT_ID, "")

    @property
    def _accession(self) -> str:
        return self.properties.get(self.ACCESSION, "")

    @property
    def _types(self) -> List[str]:
        return self.properties.get(self.TYPE, [])

    def should_fetch_links(self) -> bool:
        return self.fetch_links

    def get_auth(self) -> Union[JsonObject, None]:
        return self.auth

    def get_properties(self) -> JsonObject:
        return self.properties

    def get_accession(self) -> str:
        return self._accession

    def get_uuid(self) -> str:
        return self._uuid

    def get_at_id(self) -> str:
        return self._at_id

    def get_types(self) -> List[str]:
        return self._types

    def _get_link_tos(
        self, link_tos: Iterable[LinkTo], item_to_create: PortalItem
    ) -> List[Union[str, PortalItem]]:
        return [self._get_link_to(link_to, item_to_create) for link_to in link_tos]

    def _get_link_to(
        self,
        link_to: LinkTo,
        item_to_create: PortalItem,
    ) -> Union[str, PortalItem]:
        return get_link_to(self, link_to, item_to_create)

    @classmethod
    def from_properties(
        cls,
        properties: JsonObject,
        fetch_links: bool = False,
        auth: JsonObject = None,
        **kwargs: Any,
    ) -> PortalItem:
        return cls(properties, fetch_links=fetch_links, auth=auth)

    @classmethod
    def from_identifier_and_auth(
        cls, identifier: str, auth: JsonObject, fetch_links=False, **kwargs: Any
    ) -> PortalItem:
        properties = cls._get_item_via_auth(identifier, auth)
        return cls.from_properties(properties, auth=auth, fetch_links=fetch_links)

    @classmethod
    def _get_item_via_auth(
        cls, identifier: str, auth: JsonObject, add_on: Optional[str] = "frame=object"
    ) -> JsonObject:
        hashable_auth = cls._make_hashable_auth(auth)
        return cls._get_and_cache_item_via_auth(identifier, hashable_auth, add_on)

    @classmethod
    def _make_hashable_auth(cls, auth: Mapping[str, str]) -> Tuple[Tuple[str, str]]:
        """Assuming nothing nested here."""
        return tuple(auth.items())

    @classmethod
    def _undo_make_hashable_auth(
        cls, hashable_auth: Tuple[Tuple[str, str]]
    ) -> JsonObject:
        return dict(hashable_auth)

    @classmethod
    @lru_cache(maxsize=256)
    def _get_and_cache_item_via_auth(
        cls,
        identifier: str,
        hashable_auth: Tuple[Tuple[str, Any]],
        add_on: Optional[str] = None,
    ) -> JsonObject:
        """Save on requests by caching items."""
        auth = cls._undo_make_hashable_auth(hashable_auth)
        try:
            result = ff_utils.get_metadata(identifier, key=auth, add_on=add_on)
        except Exception as e:
            result = {}
            logger.error(f"Error getting metadata for {identifier}: {e}")
        return result

    @classmethod
    def from_identifier_and_existing_item(
        cls, identifier: str, existing_item: PortalItem, **kwargs: Any
    ) -> PortalItem:
        fetch_links = existing_item.should_fetch_links()
        auth = existing_item.get_auth()
        if auth:
            return cls.from_identifier_and_auth(
                identifier, auth, fetch_links=fetch_links
            )
        raise RuntimeError("Unable to fetch given identifier without auth key")

    @classmethod
    def from_properties_and_existing_item(
        cls, properties: JsonObject, existing_item: PortalItem, **kwargs: Any
    ) -> PortalItem:
        fetch_links = existing_item.should_fetch_links()
        auth = existing_item.get_auth()
        return cls.from_properties(properties, fetch_links=fetch_links, auth=auth)


@dataclass(frozen=True)
class NestedProperty:
    properties: JsonObject
    parent_item: Optional[PortalItem] = field(default=None, hash=False)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(parent={self.parent_item.__repr__()})"

    def get_properties(self) -> JsonObject:
        return self.properties

    def get_parent_item(self) -> Union[PortalItem, None]:
        return self.parent_item

    def _get_link_tos(
        self, link_tos: LinkTo, item_to_create: PortalItem
    ) -> List[Union[str, PortalItem]]:
        return [self._get_link_to(link_to, item_to_create) for link_to in link_tos]

    def _get_link_to(
        self,
        link_to: LinkTo,
        item_to_create: PortalItem,
    ) -> Union[str, PortalItem]:
        if self.parent_item:
            return get_link_to(self.parent_item, link_to, item_to_create)
        if isinstance(link_to, Mapping):
            return item_to_create.from_properties(link_to)
        return link_to

    @classmethod
    def from_properties(
        cls,
        properties: JsonObject,
        parent_item: Optional[PortalItem] = None,
        **kwargs: Any,
    ) -> NestedProperty:
        return cls(properties, parent_item=parent_item)
