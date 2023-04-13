import copy

from dcicutils.obfuscation_utils import (
    is_obfuscated, should_obfuscate, obfuscate, obfuscate_json, obfuscate_dict,
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

    # Edge cases that are really soft errors...
    assert should_obfuscate(None) is False  # NoQA - Argument is not intended, but function returns False
    assert should_obfuscate(17) is False    # NoQA - ditto
    assert should_obfuscate({}) is False    # NoQA - ditto

    assert should_obfuscate("my_secret") is True
    assert should_obfuscate("my_secret", "<ALREADY-OBFUSCATED>") is False


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


def check_obfuscate_json(obfuscator):

    d = {"abc": "123", "def_password_ghi": "456", "secret": 789, "my_secret": "<ALREADY-HIDDEN>",
         "foo": {"jkl": "678", "secret": "789", "encrypt_id": "9012", "encrypt_key_id": "foo"},
         "bar": ({"jkl": "678", "secret": "789", "encrypt_id": "9012", "encrypt_key_id": "foo"},)}
    o = {"abc": "123", "def_password_ghi": "***", "secret": 789, "my_secret": "<ALREADY-HIDDEN>",
         "foo": {"jkl": "678", "secret": "***", "encrypt_id": "****", "encrypt_key_id": "foo"},
         "bar": ({"jkl": "678", "secret": "***", "encrypt_id": "****", "encrypt_key_id": "foo"},)}

    x = obfuscator(d)
    assert x == o
    assert x is not d

    d0 = copy.deepcopy(d)
    x = obfuscator(d0, inplace=True)
    assert x == o
    assert d0 == o
    assert x is d0

    d = {"abc": "123", "def": "456"}
    o = {"abc": "123", "def": "456"}
    x = obfuscator(d)
    assert x == o
    assert x is d

    d = {"secret": "********"}
    x = obfuscator(d)
    assert x == d
    assert x is d

    d = {"abc": "123", "def": {"ghi": "456"}, "jkl": {"secret": "789"}}
    o = {"abc": "123", "def": {"ghi": "456"}, "jkl": {"secret": "***"}}
    x = obfuscator(d)
    assert x == o
    assert x is not d

    d = {"abc": "123", "def": {"ghi": "456"}, "jkl": {"secret": "789"}}
    o = {"abc": "123", "def": {"ghi": "456"}, "jkl": {"secret": "<REDACTED>"}}
    x = obfuscator(d, obfuscated="<REDACTED>")
    assert x == o

    xlist = obfuscator([d], obfuscated="<REDACTED>")
    assert xlist == [o]

    x = obfuscator(d, show=True)  # When show=True, this is just an identity operation
    assert x != o
    assert x == d


def test_obfuscate_dict():
    check_obfuscate_json(obfuscate_dict)


def test_obfuscate_json():
    check_obfuscate_json(obfuscate_json)


def test_obfuscate_json_already_obfuscated():
    d = {"secret": "<my-redacted_value>"}
    x = obfuscate_json(d, obfuscated="<my-redacted_value>")
    assert d == x
    assert d is x  # needs should_obfuscate to check if is_obfuscated


def test_obfuscate_json_with_nested_tuple():

    d = {"abc": "123", "def": ({"ghi": "456", "jklsecret": "obfuscatethisvalue"}, 789), "jkl": "hello"}
    o = {"abc": "123", "def": ({"ghi": "456", "jklsecret": "<REDACTED>"}, 789), "jkl": "hello"}
    d_copy = copy.deepcopy(d)  # to make sure the original is not inadvertantly modified
    x = obfuscate_json(d, obfuscated="<REDACTED>")
    assert x == o
    assert d == d_copy


def test_obfuscate_json_with_tuple():

    d = ({"abc": "123", "def": ({"ghi": "456", "jklsecret": "obfuscatethisvalue"}, 789), "passwd": "hello"}, (1, 2, 3))
    o = ({"abc": "123", "def": ({"ghi": "456", "jklsecret": "<REDACTED>"}, 789), "passwd": "<REDACTED>"}, (1, 2, 3))
    d_copy = copy.deepcopy(d)  # to make sure the original is not inadvertantly modified
    x = obfuscate_json(d, obfuscated="<REDACTED>")
    assert x == o
    assert d == d_copy
