import io
import pytest

from unittest import mock

from dcicutils.ff_mocks import mocked_s3utils
from dcicutils.glacier_utils import GlacierUtils, GlacierRestoreException
from dcicutils.qa_utils import MockFileSystem


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
            [(bucket, key)] = gu.resolve_bucket_key_from_portal('discarded')
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
            [(bucket, key)] = gu.resolve_bucket_key_from_portal('discarded')
            assert f'{bucket}/{key}' == f"{gu.health_page.get('processed_file_bucket')}/{file_meta['upload_key']}"

    @pytest.mark.parametrize('file_meta', [
        {
            '@id': 'dummy1',
            'upload_key': 'uuid/test.gz',
            '@type': ['File', 'FileProcessed'],
            'extra_files': [
                {
                    'upload_key': 'uuid/test.gz.tbi'
                }
            ]
        },
        {
            '@id': 'dummy2',
            'upload_key': 'uuid/test.gz',
            '@type': ['FileProcessed'],
            'extra_files': [
                {
                    'upload_key': 'uuid/test.gz.tbi'
                },
                {
                    'upload_key': 'uuid/test2.gz.tbi'
                }
            ]
        }
    ])
    def test_glacier_utils_bucket_key_processed_file_with_extra_files(self, glacier_utils, file_meta):
        """ Tests bootstrapping a glacier utils object and resolving processed files with extra files """
        gu = glacier_utils
        with mock.patch('dcicutils.glacier_utils.get_metadata', return_value=file_meta):
            files = gu.resolve_bucket_key_from_portal('discarded')
            found = 0
            total_expected = 1 + len(file_meta['extra_files'])
            for bucket, key in files:
                assert bucket == gu.health_page.get('processed_file_bucket')
                assert key in [
                    'uuid/test.gz',
                    'uuid/test.gz.tbi',
                    'uuid/test2.gz.tbi'
                ]
                found += 1
            assert found == total_expected

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
        ({"Restore": "ongoing-request=\"false\""}, True),
        ({"Restore": None}, False),
        ({"Restore": "ongoing-request=\"true\""}, False),
        ({}, False),
    ])
    def test_glacier_utils_is_restore_finished(self, glacier_utils, response, expected):
        """ Tests getting the restore object from some example head responses """
        gu = glacier_utils
        with mock.patch.object(gu.s3, 'head_object',
                               return_value={'ResponseMetadata': {'HTTPStatusCode': 200}, **response}):
            result = gu.is_restore_finished('bucket', 'key')
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
                    'StorageClass': 'STANDARD',
                    'LastModified': '2023'
                },
                {
                    'Key': 'example.txt',
                    'VersionId': 'Y4D4UNy1m4vLyJ7fIQdSPS1LzZuT8TJG',
                    'IsLatest': False,
                    'ETag': '"def456"',
                    'Size': 2048,
                    'StorageClass': 'GLACIER',
                    'LastModified': '2023'
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
                    'StorageClass': 'GLACIER',
                    'LastModified': '2023'
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
                    'StorageClass': 'GLACIER_IR',
                    'LastModified': '2023'
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
                    'StorageClass': 'STANDARD',
                    'LastModified': '2023'
                },
                {
                    'Key': 'example.txt',
                    'VersionId': 'Y4D4UNy1m4vLyJ7fIQdSPS1LzZuT8TJG',
                    'IsLatest': False,
                    'ETag': '"def456"',
                    'Size': 2048,
                    'StorageClass': 'DEEP_ARCHIVE',
                    'LastModified': '2023'
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
                    'StorageClass': 'STANDARD',
                    'LastModified': '2023'
                },
                {
                    'Key': 'example.txt',
                    'VersionId': 'Y4D4UNy1m4vLyJ7fIQdSPS1LzZuT8TJG',
                    'IsLatest': False,
                    'ETag': '"def456"',
                    'Size': 2048,
                    'StorageClass': 'GLACIER',
                    'LastModified': '2023'
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
                    'StorageClass': 'STANDARD',
                    'LastModified': '2023'
                },
                {
                    'Key': 'example.txt',
                    'VersionId': 'Y4D4UNy1m4vLyJ7fIQdSPS1LzZuT8TJG',
                    'IsLatest': False,
                    'ETag': '"def456"',
                    'Size': 2048,
                    'StorageClass': 'GLACIER_IR',
                    'LastModified': '2023'
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
                    'StorageClass': 'STANDARD_IA',
                    'LastModified': '2023'
                }
            ],
            'Name': 'dummy-bucket',
            'KeyCount': 1,
        }
    ])
    def test_glacier_utils_non_glacier_versions_exist(self, glacier_utils, response):
        """ Tests that we can detect when non-glacier versions exist """
        gu = glacier_utils
        with mock.patch.object(gu.s3, 'list_object_versions',
                               return_value=response):
            assert gu.non_glacier_versions_exist('bucket', 'key')

    @pytest.mark.parametrize('response', [
        {
            'Versions': [
                {
                    'Key': 'example.txt',
                    'VersionId': 'Y4D4UNy1m4vLyJ7fIQdSPS1LzZuT8TJG',
                    'IsLatest': False,
                    'ETag': '"def456"',
                    'Size': 2048,
                    'StorageClass': 'GLACIER',
                    'LastModified': '2023'
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
                    'StorageClass': 'DEEP_ARCHIVE',
                    'LastModified': '2023'
                }
            ],
            'Name': 'dummy-bucket',
            'KeyCount': 1,
        }]
    )
    def test_glacier_utils_non_glacier_versions_dont_exist(self, glacier_utils, response):
        """ Tests that we can detect when non-glacier versions exist """
        gu = glacier_utils
        with mock.patch.object(gu.s3, 'list_object_versions',
                               return_value=response):
            assert not gu.non_glacier_versions_exist('bucket', 'key')

    @pytest.mark.parametrize('response', [
        {
            'Versions': [
                {
                    'Key': 'example.txt',
                    'VersionId': 'rOthWbK8m0MFeX9Y_k4E4V69gJu8n0RU',
                    'IsLatest': True,
                    'ETag': '"abc123"',
                    'Size': 1024,
                    'StorageClass': 'STANDARD',
                    'LastModified': '2023'
                },
                {
                    'Key': 'example.txt',
                    'VersionId': 'Y4D4UNy1m4vLyJ7fIQdSPS1LzZuT8TJG',
                    'IsLatest': False,
                    'ETag': '"def456"',
                    'Size': 2048,
                    'StorageClass': 'STANDARD',
                    'LastModified': '2023'
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
                    'StorageClass': 'STANDARD',
                    'LastModified': '2023'
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

    @pytest.mark.parametrize('search_result, expected_success', [
        ([{
            '@id': 'uuid',
            '@type': ['File'],
            'upload_key': 'uuid/file.txt',
            'extra_files': [
                {
                    'upload_key': 'uuid/file2.txt'
                }
            ]
        }], ['uuid']),
        ([
             {
                '@id': 'uuid',
                '@type': ['File'],
                'upload_key': 'uuid/file.txt',
                'extra_files': [
                    {
                        'upload_key': 'uuid/file2.txt'
                    }
                ]
             },
             {
                 '@id': 'uuid2',
                 '@type': ['File'],
                 'upload_key': 'uuid/file2.txt',
             }
         ], ['uuid', 'uuid2'])
    ])
    def test_glacier_utils_restore_all_from_search(self, glacier_utils, search_result, expected_success):
        """ Tests a couple different argument combinations for this method """
        gu = glacier_utils
        with mock.patch('dcicutils.glacier_utils.search_metadata', return_value=search_result):
            # Test phase 1
            with mock.patch.object(gu, 'get_portal_file_and_restore_from_glacier', return_value=(
                ['uuid'], []
            )):
                assert gu.restore_all_from_search(search_query='/search', phase=1,
                                                  confirm=False) == (expected_success, [])
                assert gu.restore_all_from_search(search_query='/search', phase=1,
                                                  search_generator=True, confirm=False) == (expected_success, [])

            # Test phase 2
            with mock.patch.object(gu, 'copy_object_back_to_original_location', return_value={'success': True}):
                assert gu.restore_all_from_search(search_query='/search', phase=2,
                                                  search_generator=True, confirm=False) == (expected_success, [])
                assert gu.restore_all_from_search(search_query='/search', phase=2,
                                                  confirm=False) == (expected_success, [])
                with pytest.raises(GlacierRestoreException):
                    gu.restore_all_from_search(search_query='/search', phase=2,
                                               search_generator=True, parallel=True, confirm=False)

            # Test phase 3
            with mock.patch('dcicutils.glacier_utils.patch_metadata', return_value={'success': True}):
                assert gu.restore_all_from_search(search_query='/search', phase=3,
                                                  confirm=False) == (expected_success, [])
                assert gu.restore_all_from_search(search_query='/search', phase=3,
                                                  search_generator=True, confirm=False) == (expected_success, [])

            # Test phase 4
            with mock.patch.object(gu, 'delete_glaciered_object_versions', return_value={'success': True}):
                with mock.patch.object(gu, 'non_glacier_versions_exist', return_value=True):
                    assert gu.restore_all_from_search(search_query='/search', phase=4,
                                                      confirm=False) == (expected_success, [])
                    assert gu.restore_all_from_search(search_query='/search', phase=4, confirm=False,
                                                      search_generator=True) == (expected_success, [])

    def test_glacier_utils_multipart_upload(self, glacier_utils):
        """ Tests the basics of a multipart upload """
        gu = glacier_utils
        with mock.patch.object(gu.s3, 'create_multipart_upload', return_value={'UploadId': '123'}):
            with mock.patch.object(gu.s3, 'upload_part_copy', return_value={'CopyPartResult': {'ETag': 'abc'}}):
                with mock.patch.object(gu.s3, 'complete_multipart_upload', return_value={'success': True}):
                    with mock.patch.object(gu.s3, 'head_object', return_value={'ContentLength': 600000000000}):
                        assert gu.copy_object_back_to_original_location('bucket', 'key', preserve_lifecycle_tag=True)

    def test_glacier_utils_with_mock_s3(self, glacier_utils):
        """ Uses our mock_s3 system to test some operations with object versioning enabled """
        gu = glacier_utils
        mfs = MockFileSystem()
        with mocked_s3utils(environments=['fourfront-mastertest']) as mock_boto3:
            with mfs.mock_exists_open_remove():
                s3 = mock_boto3.client('s3')
                with mock.patch.object(gu, 's3', s3):
                    bucket_name = 'foo'
                    key_name = 'file.txt'
                    key2_name = 'file2.txt'
                    with io.open(key_name, 'w') as fp:
                        fp.write("first contents")
                    s3.upload_file(key_name, Bucket=bucket_name, Key=key_name)
                    with io.open(key2_name, 'w') as fp:
                        fp.write("second contents")
                    s3.upload_file(key2_name, Bucket=bucket_name, Key=key2_name)
                    with io.open(key2_name, 'w') as fp:  # add a second version
                        fp.write("second contents 2")
                    s3.upload_file(key2_name, Bucket=bucket_name, Key=key2_name)
                    versions = s3.list_object_versions(Bucket=bucket_name, Prefix=key2_name)
                    version_1 = versions['Versions'][0]['VersionId']
                    assert gu.restore_s3_from_glacier(bucket_name, key2_name, version_id=version_1)
                    assert gu.copy_object_back_to_original_location(bucket_name, key2_name, version_id=version_1,
                                                                    preserve_lifecycle_tag=True)
