import pytest

from dcicutils.bucket_utils import parse_s3_object_name


def test_parse_s3_object_name():

    GOOD = [
        ("foo/bar", {"Bucket": "foo", "Key": "bar"}),
        ("foo/bar?versionId=abc", {"Bucket": "foo", "Key": "bar", "VersionId": "abc"}),
        ("foo/bar/baz", {"Bucket": "foo", "Key": "bar/baz"}),
        ("foo/bar/baz?versionId=", {"Bucket": "foo", "Key": "bar/baz"}),
        ("foo/bar/baz?versionId=abc/def?ghi", {"Bucket": "foo", "Key": "bar/baz", "VersionId": "abc/def?ghi"}),
    ]

    for input, expected in GOOD:
        actual = parse_s3_object_name(input)
        assert actual == expected

    BAD = [
        # We don't allow empty bucket or key
        "", "foo", "/bar", "foo/",
        # We don't accept junk, after or instead of the query param because we don't know what that would mean
        # If a query parameter is present, we want it to be the one we care about
        "foo/bar?junk=1",
        "foo/bar?junkbefore=1&versionId=xyz",
        "foo/bar?junkbefore=1&versionId=xyz&junkafter=2",
        "foo/bar?versionId=xyz&junkafter=2",
        # We think this is supposed to be case-sensitive
        "foo/bar?versionid=xyz",
        "foo/bar?versionID=xyz"
    ]

    for input in BAD:
        assert parse_s3_object_name(input, ignore_errors=True) is None
        with pytest.raises(ValueError):
            assert parse_s3_object_name(input)
