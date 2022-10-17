import contextlib
import pytest
import hashlib

from botocore.exceptions import ClientError
from unittest import mock
from dcicutils.bucket_utils import (
    s3_bucket_head, s3_bucket_exists,
    s3_bucket_object_count, s3_object_head,
    s3_object_exists, s3_put_object,
    s3_object_delete_mark, s3_object_delete_version,
    s3_object_delete_completely,
)
from dcicutils.misc_utils import ignored
from dcicutils.ff_mocks import (
    mocked_boto3_object,
    # For now, no need to explicitly use these now because the
    # mocked_boto3_object hides them. -kmp 15-Sep-2021
    # MockBoto3, MockBotoS3Client
)


@contextlib.contextmanager
def typical_boto3_mocking(s3_files=None, **override_mappings):
    # Create a mocked boto3 and install it for libraries we typically call for aws_utils
    # Might have to add more utils library mocks if we end up callig those, but this will do for now.
    with mocked_boto3_object(s3_files=s3_files, **override_mappings) as mocked_boto3:
        with mock.patch("dcicutils.bucket_utils.boto3", mocked_boto3):
            # with mock.patch("dcicutils.s3_utils.boto3", mocked_boto3):
            yield mocked_boto3


def test_s3_bucket_head():

    with typical_boto3_mocking(s3_files={'foo/bar': 'something'}) as mocked_boto3:

        res = s3_bucket_head(bucket_name='foo')
        print(f"res={res}")
        assert isinstance(res, dict)
        assert res['ResponseMetadata']['HTTPStatusCode'] == 200

        res = s3_bucket_head(bucket_name='no-such-bucket')
        print(f"res={res}")
        assert res is None

        # We don't have to pass an s3 argument, but it will have to create a client on each call,
        # which might have some associated expense.
        s3 = mocked_boto3.client('s3')

        res = s3_bucket_head(bucket_name='foo', s3_client=s3)
        print(f"res={res}")
        assert isinstance(res, dict)
        assert res['ResponseMetadata']['HTTPStatusCode'] == 200

        res = s3_bucket_head(bucket_name='no-such-bucket', s3_client=s3)
        print(f"res={res}")
        assert res is None


def test_s3_bucket_exists():

    with typical_boto3_mocking(s3_files={'foo/bar': 'something'}) as mocked_boto3:

        assert s3_bucket_exists(bucket_name='foo')
        assert not s3_bucket_exists(bucket_name='no-such-bucket')

        # We don't have to pass an s3 argument, but it will have to create a client on each call,
        # which might have some associated expense.
        s3 = mocked_boto3.client('s3')

        assert s3_bucket_exists(bucket_name='foo', s3_client=s3)
        assert not s3_bucket_exists(bucket_name='no-such-bucket', s3_client=s3)


def test_s3_bucket_object_count():

    with typical_boto3_mocking():  # Note no files in initial s3 file system, so everything is missing
        with pytest.raises(ClientError):
            s3_bucket_object_count(bucket_name='no-such-bucket')

    one_file_in_each_of_two_buckets = {'foo/bar': 'something', 'bar/baz': 'something-else'}

    with typical_boto3_mocking(s3_files=one_file_in_each_of_two_buckets):
        assert s3_bucket_object_count(bucket_name='foo') == 1
        assert s3_bucket_object_count(bucket_name='bar') == 1

    two_files_in_foo_three_in_bar = {
        'foo/x': 'ex',
        'foo/y': 'why',
        'bar/q': 'cue',
        'bar/r': 'arrrr',
        'bar/s': 'ess'
    }

    with typical_boto3_mocking(s3_files=two_files_in_foo_three_in_bar):
        assert s3_bucket_object_count(bucket_name='foo') == 2
        assert s3_bucket_object_count(bucket_name='bar') == 3


def test_s3_object_head():
    expected_res = {'Bucket': 'foo', 'Key': 'bar',
                    'ETag': '437b930db84b8079c2dd804a71936b5f',
                    'ContentLength': 9}
    with typical_boto3_mocking(s3_files={'foo/bar': 'something'}) as mocked_boto3:
        res = s3_object_head(object_key='bar', bucket_name='foo')
        for k, v in expected_res.items():
            assert res.get(k) == v
        # the assertions below fail because of an Exception raised in the mocked s3_file
        # that may need to be updated to allow these type of assertions based on what is
        # expected in the function
        # assert not s3_object_head(object_key='no-such-object', bucket_name='foo')
        # assert not s3_object_head(object_key='bar', bucket_name='no-such-bucket')
        # assert not s3_object_head(object_key='no-such-bucket', bucket_name='no-such-bucket')

        # We don't have to pass an s3 argument, but it will have to create a client on each call,
        # which might have some associated expense.
        s3 = mocked_boto3.client('s3')
        res = s3_object_head(object_key='bar', bucket_name='foo', s3_client=s3)
        for k, v in expected_res.items():
            assert res.get(k) == v


def test_s3_object_exists():
    with typical_boto3_mocking(s3_files={'foo/bar': 'something'}) as mocked_boto3:
        assert s3_object_exists(object_key='bar', bucket_name='foo')
        # the assertions below fail because of an Exception raised in the mocked s3_file
        # that may need to be updated to allow these type of assertions based on what is
        # expected in the function
        # assert not s3_object_exists(object_key='no-such-object', bucket_name='foo')
        # assert not s3_object_exists(object_key='bar', bucket_name='no-such-bucket')
        # assert not s3_object_exists(object_key='no-such-bucket', bucket_name='no-such-bucket')
        # We don't have to pass an s3 argument, but it will have to create a client on each call,
        # which might have some associated expense.
        s3 = mocked_boto3.client('s3')
        assert s3_object_exists(object_key='bar', bucket_name='foo', s3_client=s3)


@pytest.mark.skip()  # So that running this empty test won't make it look like we did actual testing.
# do we want to parameterize to check all valid extensions?
# this is a work in progress so skipped for now
def test_s3_put_object():
    keys_to_test = ['tester.txt', 'test_uuid_1/test.txt']
    obj_content = 'teststring'.encode()
    bucket = 'foo'
    with typical_boto3_mocking(s3_files={'foo': None}):
        for obj_key in keys_to_test:
            res = s3_put_object(object_key=obj_key, obj=obj_content, bucket_name=bucket)
            assert res.get('ETag') == hashlib.md5(obj_content).hexdigest()
            assert s3_object_exists(object_key=obj_key, bucket_name=bucket)

        # put to non-existent bucket
        res = s3_put_object(object_key=keys_to_test[0], obj=obj_content, bucket_name='baz')

        assert False
    # TODO: Making a test of s3_object_exists means extending the mocks in qa_utils.py to handle ACL.
    #       If you want to just accept the argument and ignore it. That's easy enough.
    #       If you want to test that the argument was received, you can probably do something with
    #       the required arguments checkers, like what is done with testing SSE arguments.
    #       If you want to actually model acls, that will be more difficult, and I might suggest for
    #       now using other tech to do testing of this function (either integration tests or some
    #       kind of MagicMock where you can just test that appropriate calls to AWS functions have
    #       occurred due to the logic of the function you're building around the AWS functions),
    #       but I can talk to you about what is needed to do the modeling if you want,
    #       or I could extend it to do that and you could review the extension. -kmp 15-Sep-2021


@pytest.mark.skip()  # So that running this empty test won't make it look like we did actual testing.
def test_s3_object_delete_mark():
    ignored(s3_object_delete_mark)
    # TODO: Again the business with bucket versioning is not implemented. Whether you want to implement
    #       that depends on the depth of what you're wanting to test. -kmp 15-Sep-2021
    pass


@pytest.mark.skip()  # So that running this empty test won't make it look like we did actual testing.
def test_s3_object_delete_version():
    ignored(s3_object_delete_version)
    # TODO: Again the business with bucket versioning is not implemented. Whether you want to implement
    #       that depends on the depth of what you're wanting to test. -kmp 15-Sep-2021
    pass


@pytest.mark.skip()  # So that running this empty test won't make it look like we did actual testing.
def test_s3_object_delete_completely():
    ignored(s3_object_delete_completely)
    # TODO: Again the business with bucket versioning is not implemented. Whether you want to implement
    #       that depends on the depth of what you're wanting to test. -kmp 15-Sep-2021
    pass
