# Some utilities related to obfuscating sensitive data (dmichaels/2022-07-20).

import copy
import re

from dcicutils.misc_utils import check_true
from typing import Any, Optional


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


def should_obfuscate(key: str) -> bool:
    """
    Returns True if the given key looks as if it represents a sensitive value.
    Just sees if it contains "secret" or "password" or "crypt" some obvious variants,
    case-insensitive; i.e. whatever is in the _SENSITIVE_KEY_NAMES_REGEX list
    containing regular expressions; add more to if/when needed.

    :param key: Key name of some property which may or may not need to be obfuscated.
    :return: True if the given key name looks as if it represents a sensitive value.
    """
    if not key or not isinstance(key, str):
        return False
    return _SENSITIVE_KEY_NAMES_REGEX.match(key) is not None


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


def is_obfuscated(value: str, obfuscated: str = None) -> bool:
    """
    Returns True if a given string is in the format we use as an obfuscated value.
    Returns False if the argument is not a string or is not in the obfuscated value format.

    NOTE: This is heuristic. Your password MIGHT be *** or <my-password>, but we're hoping not.
    """
    if isinstance(value, str):
        if isinstance(obfuscated, str):
            return value == obfuscated
        else:
            return bool(OBFUSCATED_VALUE.match(value))
    return False


def obfuscate_dict(target: Any, inplace: bool = False, show: bool = False, obfuscated: Optional[str] = None) -> Any:
    """
    Obfuscates all STRING values within the given dictionary, RECURSIVELY, for all key names which look
    as if they represent sensitive values (based on the should_obfuscate function). By default, if the
    inplace argument is False, a COPY of the dictionary is returned, but ONLY if it actually needs to
    be modified (i.e. has values to obfuscate, based on key name, and which are not already obfuscated);
    i.e. the given dictionary is NOT modified if there are no values to obfuscate or if such values are
    already abfuscated. If the inplace argument is True, then any changes (value obfuscations) are made to
    the given dictionary itself in place (NOT a copy). In either case the resultant dictionary is returned.
    If the show argument is True then does not actually obfuscate and simply returns the given dictionary.

    N.B. ACTUALLY, this ALSO works if the given target value is a LIST (in which case we look, recursively,
    for dictionary elements within to obfuscate); and actually, ANY value may be passed, which, if not
    a dictionary or list, we just return the given value.

    :param dictionary: Given dictionary whose senstive values obfuscate.
    :param inplace: If True obfuscate the given dictionary in place; else a COPY is returned, if modified.
    :param show: If True does not actually obfuscate and simply returns the given dictionary.
    :return: Resultant dictionary.
    """

    check_true(not obfuscated or is_obfuscated(obfuscated),
               message=f"If obfuscated= is supplied, it must be {OBFUSCATED_VALUE_DESCRIPTION}.")

    def has_values_to_obfuscate(target: Any) -> bool:
        if isinstance(target, dict):
            for key, value in target.items():
                if isinstance(value, dict):
                    if has_values_to_obfuscate(value):
                        return True
                elif isinstance(value, list):
                    for item in value:
                        if has_values_to_obfuscate(item):
                            return True
                elif isinstance(value, str):
                    if should_obfuscate(key):
                        if not is_obfuscated(value, obfuscated=obfuscated):
                            return True
        elif isinstance(target, list):
            for item in target:
                if has_values_to_obfuscate(item):
                    return True
        return False

    if isinstance(show, bool) and show:
        return target
    if not isinstance(inplace, bool) or not inplace:
        if has_values_to_obfuscate(target):
            target = copy.deepcopy(target)
    if isinstance(target, dict):
        for key, value in target.items():
            if isinstance(value, dict):
                target[key] = obfuscate_dict(value, inplace=True, show=False, obfuscated=obfuscated)
            elif isinstance(value, list):
                obfuscated_value = []
                for item in value:
                    obfuscated_value.append(obfuscate_dict(item, inplace=True, show=False, obfuscated=obfuscated))
                target[key] = obfuscated_value
            elif isinstance(value, str):
                if should_obfuscate(key):
                    if not is_obfuscated(value, obfuscated=obfuscated):
                        target[key] = obfuscate(value, show=False, obfuscated=obfuscated)
    elif isinstance(target, list):
        return [obfuscate_dict(item, inplace=True, show=False, obfuscated=obfuscated) for item in target]
    return target
