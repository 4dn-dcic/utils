import copy

from dcicutils.obfuscation_utils import (
    is_obfuscated, should_obfuscate, obfuscate, obfuscate_dict,
)


def assert_should_obfuscate_with_different_cases(value: str, expected: bool) -> None:
    assert should_obfuscate(value) == expected
    assert should_obfuscate(value.upper()) == expected
    assert should_obfuscate(value.lower()) == expected


def test_should_obfuscate() -> None:
    assert_should_obfuscate_with_different_cases("password", True)
    assert_should_obfuscate_with_different_cases("secret", True)
    assert_should_obfuscate_with_different_cases("scret", True)
    assert_should_obfuscate_with_different_cases("crypt", True)
    assert_should_obfuscate_with_different_cases("foo_password_bar", True)
    assert_should_obfuscate_with_different_cases("foo_passwd_bar", True)
    assert_should_obfuscate_with_different_cases("foo_secret_bar", True)
    assert_should_obfuscate_with_different_cases("foo_scret_bar", True)
    assert_should_obfuscate_with_different_cases("foo_crypt_bar", True)
    assert_should_obfuscate_with_different_cases("foo_crypt_key_id_bar", True)
    assert_should_obfuscate_with_different_cases("crypt_key_id", False)
    assert_should_obfuscate_with_different_cases("not_a_s3cret_foo", False)


def assert_string_contains_only_asterisks(value: str) -> None:
    assert set(list(value)) == {"*"}


def assert_obfuscate(value: str) -> None:
    assert_string_contains_only_asterisks(obfuscate(value))


def test_is_obfuscated():

    assert is_obfuscated("") is False

    assert is_obfuscated("*") is True
    assert is_obfuscated("**") is True
    assert is_obfuscated("***") is True

    assert is_obfuscated("a") is False
    assert is_obfuscated("ab") is False
    assert is_obfuscated("abc") is False

    assert is_obfuscated("<>") is False

    assert is_obfuscated("<foo>") is True
    assert is_obfuscated("<foo-bar>") is True
    assert is_obfuscated("<foo-bar-1>") is True
    assert is_obfuscated("<foo_bar>") is True
    assert is_obfuscated("<foo_bar-1>") is True

    assert is_obfuscated(" <foo-bar>") is False
    assert is_obfuscated("<foo bar>") is False  # A space is not allowed in an obfuscation pattern
    assert is_obfuscated("<foo*bar-1>") is False  # An asterisk is not allowed in an obfuscation pattern


def test_obfuscate():

    assert_string_contains_only_asterisks(obfuscate("a"))
    assert_string_contains_only_asterisks(obfuscate("abc"))
    assert_string_contains_only_asterisks(obfuscate("prufrock"))

    sample_obfuscation = "<sample-obfuscation>"

    assert not is_obfuscated("abc")
    assert is_obfuscated(obfuscate("abc"))
    assert len(obfuscate("abc")) == len("abc") != len(sample_obfuscation)

    assert obfuscate("abc") == "***"
    assert obfuscate("abc", obfuscated=sample_obfuscation) == sample_obfuscation  # "<sample-obfuscation>"

    for x in ["", None, 123, 3.14]:
        assert not is_obfuscated(x)
        assert obfuscate(x) == x  # although not obfuscated, they are nonetheless not obfuscated by special case
        assert obfuscate(x, obfuscated=sample_obfuscation) == x
        assert obfuscate(x, obfuscated=sample_obfuscation) != sample_obfuscation
        assert not is_obfuscated(obfuscate(x))


def test_obfuscate_dict():
    d = {"abc": "123", "def_password_ghi": "456", "secret": 789, "foo":
         {"jkl": "678", "secret": "789", "encrypt_id": "9012", "encrypt_key_id": "foo"}}
    o = {"abc": "123", "def_password_ghi": "***", "secret": 789, "foo":
         {"jkl": "678", "secret": "***", "encrypt_id": "****", "encrypt_key_id": "foo"}}

    x = obfuscate_dict(d)
    assert x == o
    assert id(x) != id(d)

    x = obfuscate_dict(d, inplace=True)
    assert x == o
    assert id(x) == id(d)

    d = {"abc": "123", "def": "456"}
    o = {"abc": "123", "def": "456"}
    x = obfuscate_dict(d)
    assert x == o
    assert id(x) == id(d)

    d = {"secret": "********"}
    x = obfuscate_dict(d)
    assert x == d
    assert id(x) == id(d)

    d = {"abc": "123", "def": {"ghi": "456"}, "jkl": {"secret": "789"}}
    o = {"abc": "123", "def": {"ghi": "456"}, "jkl": {"secret": "***"}}
    x = obfuscate_dict(d)
    assert x == o
    assert id(x) != id(d)

    d = {"abc": "123", "def": {"ghi": "456"}, "jkl": {"secret": "789"}}
    o = {"abc": "123", "def": {"ghi": "456"}, "jkl": {"secret": "<REDACTED>"}}
    x = obfuscate_dict(d, obfuscated="<REDACTED>")
    assert x == o

def test_obfuscate_dict_with_nested_list():

    d = {"abc": "123", "def": [{"ghi": "456", "jklsecret": "obfuscatethisvalue"}, 789], "jkl": "hello"}
    o = {"abc": "123", "def": [{"ghi": "456", "jklsecret": "<REDACTED>"        }, 789], "jkl": "hello"}
    d_copy = copy.deepcopy(d)  # to make sure the original is not inadvertantly modified
    x = obfuscate_dict(d, obfuscated="<REDACTED>")
    assert x == o
    assert d == d_copy
