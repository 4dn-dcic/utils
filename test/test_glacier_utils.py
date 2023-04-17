import pytest
from unittest import mock
from dcicutils.glacier_utils import GlacierUtils


def mock_keydict() -> dict:
    """ Dummy keydict (unused) """
    return {
        'server': 'dummy-server',
        'key': 'key',
        'secret': 'secret'
    }


def mock_health_page() -> dict:
    """ Dummy health page """
    return {
        '@type': ['Health', 'Portal'],
        '@context': '/health',
        '@id': '/health',
        'display_title': 'CGAP Status and Foursight Monitoring',
        'file_upload_bucket': 'cgap-dummy-main-application-cgap-dummy-files',
        'namespace': 'cgap-dummy',
        'processed_file_bucket': 'cgap-dummy-main-application-cgap-dummy-wfoutput',
   }


@pytest.fixture(scope='module')
def glacier_utils():
    """ Module scoped fixture for mock execution, greatly speeds up tests """
    with mock.patch('dcicutils.ff_utils.get_health_page', return_value=mock_health_page()):
        with mock.patch('dcicutils.creds_utils.KeyManager.get_keydict_for_env', return_value=mock_keydict()):
            return GlacierUtils('cgap-dummy')


class TestGlacierUtils:

    @pytest.mark.parametrize('file_meta', [
        {
            '@id': 'dummy1',
            'upload_key': 'uuid/test.gz',
            '@type': ['File']
        },
        {
            '@id': 'dummy2',
            'upload_key': 'uuid/test2.gz',
            '@type': ['File', 'FileSubmitted']
        },
    ])
    def test_glacier_utils_bucket_key_upload(self, glacier_utils, file_meta):
        """ Tests bootstrapping a glacier utils object and resolving uploaded (files bucket) files """
        gu = glacier_utils
        with mock.patch('dcicutils.glacier_utils.get_metadata', return_value=file_meta):
            bucket, key = gu.resolve_bucket_key_from_portal('discarded')
            assert f'{bucket}/{key}' == f"{gu.health_page.get('file_upload_bucket')}/{file_meta['upload_key']}"

    @pytest.mark.parametrize('file_meta', [
        {
            '@id': 'dummy1',
            'upload_key': 'uuid/test.gz',
            '@type': ['File', 'FileProcessed']
        },
        {
            '@id': 'dummy2',
            'upload_key': 'uuid/test.gz',
            '@type': ['FileProcessed']
        }
    ])
    def test_glacier_utils_bucket_key_processed_file(self, glacier_utils, file_meta):
        """ Tests bootstrapping a glacier utils object and resolving processed files """
        gu = glacier_utils
        with mock.patch('dcicutils.glacier_utils.get_metadata', return_value=file_meta):
            bucket, key = gu.resolve_bucket_key_from_portal('discarded')
            assert f'{bucket}/{key}' == f"{gu.health_page.get('processed_file_bucket')}/{file_meta['upload_key']}"

    @pytest.mark.parametrize('mocked_response', [
        {
            'ResponseMetadata': {
                'RequestId': 'EXAMPLE_REQUEST_ID',
                'HostId': 'EXAMPLE_HOST_ID',
                'HTTPStatusCode': 200,  # either 200 or 202 can be returned
                'HTTPHeaders': {
                    'x-amz-id-2': 'EXAMPLE_AMZ_ID',
                    'x-amz-request-id': 'EXAMPLE_REQUEST_ID',
                    'date': 'Sun, 17 Apr 2023 07:35:29 GMT',
                    'content-length': '0',
                    'server': 'AmazonS3'
                },
                'RetryAttempts': 0
            }
        },
        {
            'ResponseMetadata': {
                'RequestId': 'EXAMPLE_REQUEST_ID',
                'HostId': 'EXAMPLE_HOST_ID',
                'HTTPStatusCode': 202,  # either 200 or 202 can be returned
                'HTTPHeaders': {
                    'x-amz-id-2': 'EXAMPLE_AMZ_ID',
                    'x-amz-request-id': 'EXAMPLE_REQUEST_ID',
                    'date': 'Sun, 17 Apr 2023 07:35:29 GMT',
                    'content-length': '0',
                    'server': 'AmazonS3'
                },
                'RetryAttempts': 0
            }
        }
    ])
    def test_glacier_utils_mocked_restore(self, glacier_utils, mocked_response):
        """ Mocks the result of the restore_object API call """
        gu = glacier_utils
        bucket, key = 'bucket', 'key'
        with mock.patch.object(gu.s3, 'restore_object', return_value=mocked_response):
            assert mocked_response == gu.restore_s3_from_glacier(bucket, key)

    def test_glacier_utils_mocked_restore_fail(self, glacier_utils):
        """ Mocks the case where the API throws exception, should catch and return None"""
        bucket, key = 'bucket', 'key'

        def mocked_restore_error():
            raise Exception  # Would normally raise ClientError but harder to mock

        gu = glacier_utils
        with mock.patch.object(gu.s3, 'restore_object', side_effect=mocked_restore_error):
            assert gu.restore_s3_from_glacier(bucket, key) is None

    @pytest.mark.parametrize('response, expected', [
        ({"Restore": "ongoing-request=\"false\"", "RestoreOutputPath": "s3://temp-bucket/temp-key"},
         "s3://temp-bucket/temp-key"),
        ({"Restore": None}, None),
        ({"Restore": "ongoing-request=\"true\""}, None),
        ({}, None),
    ])
    def test_glacier_utils_extract_temporary_s3_location_from_restore_response(self, glacier_utils, response, expected):
        """ Tests getting the restore object from some example head responses """
        gu = glacier_utils
        with mock.patch.object(gu.s3, 'head_object',
                               return_value={'ResponseMetadata': {'HTTPStatusCode': 200}, **response}):
            result = gu.extract_temporary_s3_location_from_restore_response('bucket', 'key')
            assert result == expected

    @pytest.mark.parametrize('response', [
        {
            'Versions': [
                {
                    'Key': 'example.txt',
                    'VersionId': 'rOthWbK8m0MFeX9Y_k4E4V69gJu8n0RU',
                    'IsLatest': True,
                    'ETag': '"abc123"',
                    'Size': 1024,
                    'StorageClass': 'STANDARD'
                },
                {
                    'Key': 'example.txt',
                    'VersionId': 'Y4D4UNy1m4vLyJ7fIQdSPS1LzZuT8TJG',
                    'IsLatest': False,
                    'ETag': '"def456"',
                    'Size': 2048,
                    'StorageClass': 'GLACIER'
                }
            ],
            'Name': 'dummy-bucket',
            'KeyCount': 2,
        },
        {
            'Versions': [
                {
                    'Key': 'example.txt',
                    'VersionId': 'Y4D4UNy1m4vLyJ7fIQdSPS1LzZuT8TJG',
                    'IsLatest': False,
                    'ETag': '"def456"',
                    'Size': 2048,
                    'StorageClass': 'GLACIER'
                }
            ],
            'Name': 'dummy-bucket',
            'KeyCount': 1,
        },
        {
            'Versions': [
                {
                    'Key': 'example.txt',
                    'VersionId': 'Y4D4UNy1m4vLyJ7fIQdSPS1LzZuT8TJG',
                    'IsLatest': False,
                    'ETag': '"def456"',
                    'Size': 2048,
                    'StorageClass': 'GLACIER_IR'
                }
            ],
            'Name': 'dummy-bucket',
            'KeyCount': 1,
        },
        {
            'Versions': [
                {
                    'Key': 'example.txt',
                    'VersionId': 'rOthWbK8m0MFeX9Y_k4E4V69gJu8n0RU',
                    'IsLatest': True,
                    'ETag': '"abc123"',
                    'Size': 1024,
                    'StorageClass': 'STANDARD'
                },
                {
                    'Key': 'example.txt',
                    'VersionId': 'Y4D4UNy1m4vLyJ7fIQdSPS1LzZuT8TJG',
                    'IsLatest': False,
                    'ETag': '"def456"',
                    'Size': 2048,
                    'StorageClass': 'DEEP_ARCHIVE'
                }
            ],
            'Name': 'dummy-bucket',
            'KeyCount': 2,
        }
    ])
    def test_glacier_utils_delete_glaciered_versions_exist(self, glacier_utils, response):
        """ Tests some mocked deletes of dummy list_objects calls for a particular object with a standard
            and glacier version, should return true """
        gu = glacier_utils
        with mock.patch.object(gu.s3, 'list_object_versions',
                               return_value=response):
            with mock.patch.object(gu.s3, 'delete_object'):
                assert gu.delete_glaciered_object_versions('bucket', 'key')
                assert gu.delete_glaciered_object_versions('bucket', 'key', delete_all_versions=True)

    @pytest.mark.parametrize('response', [
        {
            'Versions': [
                {
                    'Key': 'example.txt',
                    'VersionId': 'rOthWbK8m0MFeX9Y_k4E4V69gJu8n0RU',
                    'IsLatest': True,
                    'ETag': '"abc123"',
                    'Size': 1024,
                    'StorageClass': 'STANDARD'
                },
                {
                    'Key': 'example.txt',
                    'VersionId': 'Y4D4UNy1m4vLyJ7fIQdSPS1LzZuT8TJG',
                    'IsLatest': False,
                    'ETag': '"def456"',
                    'Size': 2048,
                    'StorageClass': 'STANDARD'
                }
            ],
            'Name': 'dummy-bucket',
            'KeyCount': 2,
        },
        {
            'Versions': [
                {
                    'Key': 'example.txt',
                    'VersionId': 'rOthWbK8m0MFeX9Y_k4E4V69gJu8n0RU',
                    'IsLatest': True,
                    'ETag': '"abc123"',
                    'Size': 1024,
                    'StorageClass': 'STANDARD'
                },
            ],
            'Name': 'dummy-bucket',
            'KeyCount': 1,
        },
    ])
    def test_glacier_utils_delete_glaciered_versions_do_not_exist(self, glacier_utils, response):
        """ Tests some mocked deletes of dummy list_objects calls for a particular object with a standard
            and glacier version, should return true """
        gu = glacier_utils
        with mock.patch.object(gu.s3, 'list_object_versions',
                               return_value=response):
            with mock.patch.object(gu.s3, 'delete_object'):
                assert not gu.delete_glaciered_object_versions('bucket', 'key')
                assert not gu.delete_glaciered_object_versions('bucket', 'key', delete_all_versions=True)
