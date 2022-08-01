# Some utilities related to obfuscating sensitive data (dmichaels/2022-07-20).

import copy
import re


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


def obfuscate(value: str, show: bool = False) -> str:
    """
    Obfuscates and returns the given string value.
    If the given value is not a string, is None, or is empty then just returns the given value.
    If the show argument is True then does not actually obfuscate and simply returns the given string.

    :param value: Value to obfuscate.
    :param show: If True then do not actually obfuscate, rather simply returns the given value.
    :return: Obfuscated (or not if show) value or empty string if not a string or empty.
    """
    if not isinstance(value, str) or not value:
        return value
    return value if show else len(value) * "*"


def obfuscate_dict(dictionary: dict, inplace: bool = False, show: bool = False) -> dict:
    """
    Obfuscates all STRING values within the given dictionary, RECURSIVELY, for all key names which look
    as if they represent sensitive values (based on the should_obfuscate function). By default, if the
    inplace argument is False, a COPY of the dictionary is returned, but ONLY if it actually needs to
    be modified (i.e. has values to obfuscate, based on key name, and which are not already obfuscated);
    i.e. the given dictionary is NOT modified if there are no values to obfuscate or if such values are
    already abfuscated. If the inplace argument is True, then any changes (value obfuscations) are made to
    the given dictionary itself in place (NOT a copy). In either case the resultant dictionary is returned.
    If the show argument is True then does not actually obfuscate and simply returns the given dictionary.

    :param dictionary: Given dictionary whose senstive values obfuscate.
    :param inplace: If True obfuscate the given dictionary in place; else a COPY is returned, if modified.
    :param show: If True does not actually obfuscate and simply returns the given dictionary.
    :return: Resultant dictionary.
    """
    def already_obfuscated(value: str) -> bool:
        return set(list(value)) == {"*"}

    def has_values_to_obfuscate(dictionary: dict) -> bool:
        for key, value in dictionary.items():
            if isinstance(value, dict):
                if has_values_to_obfuscate(value):
                    return True
            elif isinstance(value, str) and should_obfuscate(key) and not already_obfuscated(value):
                return True
        return False

    if dictionary is None or not isinstance(dictionary, dict):
        return {}
    if isinstance(show, bool) and show:
        return dictionary
    if not isinstance(inplace, bool) or not inplace:
        if has_values_to_obfuscate(dictionary):
            dictionary = copy.deepcopy(dictionary)
    for key, value in dictionary.items():
        if isinstance(value, dict):
            dictionary[key] = obfuscate_dict(value, show=False, inplace=False)
        elif isinstance(value, str) and should_obfuscate(key) and not already_obfuscated(value):
            dictionary[key] = obfuscate(value, show=False)
    return dictionary
