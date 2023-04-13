# Some utilities related to obfuscating sensitive data (dmichaels/2022-07-20).

import copy
import re

from .common import AnyJsonData
from .misc_utils import check_true, StorageCell
from typing import Optional, Any, Union


# The _SENSITIVE_KEY_NAMES_REGEX regex defines key names representing sensitive values, case-insensitive.
# Note the below 'crypt(?!_key_id$)' regex matches any thing with 'crypt' except for 'crypt_key_id'.
_SENSITIVE_KEY_NAMES_REGEX = re.compile(
    r"""
    .*(
        password       |
        passwd         |
        secret         |
        secrt          |
        scret          |
        session.*token |
        session.*id    |
        crypt(?!_key_id$)
    ).*
    """, re.VERBOSE | re.IGNORECASE)


def should_obfuscate(key: str, value: Any = None) -> bool:
    """
    Returns True if the given key looks as if it represents a sensitive value.
    Just sees if it contains "secret" or "password" or "crypt" some obvious variants,
    case-insensitive; i.e. whatever is in the _SENSITIVE_KEY_NAMES_REGEX list
    containing regular expressions; add more to if/when needed.

    :param key: Key name of some property which may or may not need to be obfuscated.
    :param value: (optional) value of item to be obfuscated, just in case it already is obfuscated
    :return: True if the given key name looks as if it represents a sensitive value.
    """
    obfuscate_because_of_key = isinstance(key, str) and bool(_SENSITIVE_KEY_NAMES_REGEX.match(key))
    not_obfuscated_already = not isinstance(value, str) or not is_obfuscated(value)
    return obfuscate_because_of_key and not_obfuscated_already


def obfuscate(value: str, show: bool = False, obfuscated: Optional[str] = None) -> str:
    """
    Obfuscates and returns the given string value.
    If the given value is not a string, is None, or is empty then just returns the given value.
    If the show argument is True then does not actually obfuscate and simply returns the given string.

    :param value: Value to obfuscate.
    :param show: If True then do not actually obfuscate, rather simply returns the given value.
    :param obfuscated:
    :return: Obfuscated (or not if show) value or empty string if not a string or empty.
    """
    if not isinstance(value, str) or not value:
        return value
    return value if show else obfuscated or len(value) * "*"


# The rationale here is theoretically passwords can have "<" and ">" in them, so we want a pretty
# restricted set of characters in order that real passwords that are just coincidentally arranged
# this way are unlikely. -kmp 18-Aug-2022

OBFUSCATED_VALUE_DESCRIPTION = ("a series of asterisks or a meta-identifier like <some-name>,"
                                " made up only of alphanumerics, hyphens and underscores")
OBFUSCATED_VALUE = re.compile(r'^([*]+|[<][a-z0-9_-]+[>])$', re.IGNORECASE)


def is_obfuscated(value: str) -> bool:
    """
    Returns True if a given string is in the format we use as an obfuscated value.
    Returns False if the argument is not a string or is not in the obfuscated value format.

    NOTE: This is heuristic. Your password MIGHT be *** or <my-password>, but we're hoping not.
    """

    return isinstance(value, str) and bool(OBFUSCATED_VALUE.match(value))


def obfuscate_json(item: Union[AnyJsonData, tuple], inplace: bool = False, show: bool = False,
                   obfuscated: Optional[str] = None) -> Union[AnyJsonData, tuple]:
    """
    Obfuscates all STRING values within the given dictionary, RECURSIVELY, for all key names which look
    as if they represent sensitive values (based on the should_obfuscate function). By default, if the
    inplace argument is False, a COPY of the dictionary is returned, but ONLY if it actually needs to
    be modified (i.e. has values to obfuscate, based on key name, and which are not already obfuscated);
    i.e. the given dictionary is NOT modified if there are no values to obfuscate or if such values are
    already abfuscated. If the inplace argument is True, then any changes (value obfuscations) are made to
    the given dictionary itself in place (NOT a copy). In either case the resultant dictionary is returned.
    If the show argument is True then does not actually obfuscate and simply returns the given dictionary.

    :param item: Any JSON object that might be or contain a dictionary whose senstive values are to be obfuscated.
    :param inplace: If True obfuscate the given dictionary in place; else a COPY is returned, if modified.
    :param show: If True does not actually obfuscate and simply returns the given dictionary.
    :param obfuscated: The obfuscation string to use if one is not to be generated dynamically.
    :return: Resultant dictionary.
    """

    check_true(not obfuscated or is_obfuscated(obfuscated),
               message=f"If obfuscated= is supplied, it must be {OBFUSCATED_VALUE_DESCRIPTION}.")

    if show:
        return item

    orig_item = item

    changed = StorageCell(False)

    if not inplace:
        item = copy.deepcopy(item)

    def process_recursively(item: Union[AnyJsonData, tuple]):
        # We only need to process non-atomic items recursively, since they are the only things
        # that might conceivably be or contain a dictionary in need of obfuscation.
        if isinstance(item, dict):
            for key, value in item.items():
                if should_obfuscate(key, value):
                    changed.value = True
                    item[key] = obfuscate(value, obfuscated=obfuscated)
                else:
                    process_recursively(value)
        elif isinstance(item, list) or isinstance(item, tuple):
            for element in item:
                process_recursively(element)

    process_recursively(item)

    return item if changed.value else orig_item


# The function obfuscate_dict is deprecated and will go away in a future major release.
obfuscate_dict = obfuscate_json
