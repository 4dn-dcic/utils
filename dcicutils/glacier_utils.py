import boto3
from typing import Union, List, Tuple
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from .common import (
    S3_GLACIER_CLASSES, S3StorageClass, MAX_MULTIPART_CHUNKS, MAX_STANDARD_COPY_SIZE,
    ENCODED_LIFECYCLE_TAG_KEY
)
from .command_utils import require_confirmation
from .misc_utils import PRINT
from .ff_utils import get_metadata, search_metadata, get_health_page, patch_metadata
from .creds_utils import CGAPKeyManager


class GlacierRestoreException(Exception):
    pass


class GlacierUtils:

    def __init__(self, env_name: str):
        """ Pass an env_name that exists in your ~/.cgap_keys.json or ~/.fourfront_keys.json file
            No support for admin keys!

            This class is intended to work in 4 phases:
                1. Given a list of object @ids, issue API calls to restore those @ids from Glacier or DA.
                2. Once restore is complete, issue a copy API call to create a new version of the object under
                   the same bucket/key.
                3. Once the copy is complete, patch the metadata @ids to reflect the new state.
                4. Once the copy is complete, delete all glacierized versions of the object (if desired). This is
                   optional as you can choose to keep the glacierized version if you plan to delete it from standard
                   shortly after analysis.

            There are multiple ways to use this library:
                1. Given a search, run the particular phase given with relevant arguments. The user
                   is responsible for checking restores were successful before moving on after phase 1.
                   This is the recommended way to use this library. Call the restore_all_from_search to
                   use this pathway. Note that this method does not handle object versioning.
                2. Given a list of @ids, you can use the phasing methods directly to resolve metadata
                   on-demand for each item. This is likely to be slower but may be more convenient/compatible
                   in certain cases with existing code looking for very specific files. Call the following
                   methods in this case:
                       * restore_glacier_phase_one_restore
                       * restore_glacier_phase_two_copy
                       * restore_glacier_phase_three_patch
                       * restore_glacier_phase_four_delete
                3. Given an individual @id, you can use the internal methods for working on just that
                   one item, or you can use the phase methods above with the single item list (recommended).
                   If you want to use the internal methods, see:
                       * get_portal_file_and_restore_from_glacier
                       * copy_object_back_to_original_location
                       * patch_file_lifecycle_status
                       * delete_glaciered_object_versions
        """
        self.s3 = boto3.client('s3')
        self.env_name = env_name
        self.key_manager = CGAPKeyManager()
        self.env_key = self.key_manager.get_keydict_for_env(env_name)
        self.health_page = get_health_page(key=self.env_key, ff_env=env_name)

    @property
    def kms_key_id(self) -> str:
        return self.health_page.get("s3_encrypt_key_id", "")

    @classmethod
    def is_glacier_storage_class(cls, storage_class: S3StorageClass):
        return storage_class in S3_GLACIER_CLASSES

    @classmethod
    def is_available_storage_class(cls, storage_class: S3StorageClass):
        return not cls.is_glacier_storage_class(storage_class)

    @classmethod
    def transition_involves_glacier_restoration(cls, from_storage_class: S3StorageClass,
                                                to_storage_class: S3StorageClass):
        return cls.is_glacier_storage_class(from_storage_class) and cls.is_available_storage_class(to_storage_class)

    def resolve_possible_file_status(self) -> list:
        """ Checks the File.json profile to see valid status values for files """
        profile = get_metadata('/profiles/file.json', key=self.env_key, ff_env=self.env_name)
        return profile['properties']['status']['enum']

    def resolve_possible_lifecycle_status(self) -> list:
        profile = get_metadata('/profiles/file.json', key=self.env_key, ff_env=self.env_name)
        return profile['properties']['s3_lifecycle_status']['enum']

    def resolve_possible_lifecycle_category(self) -> list:
        profile = get_metadata('/profiles/file.json', key=self.env_key, ff_env=self.env_name)
        return profile['properties']['s3_lifecycle_category']['suggested_enum']

    def resolve_bucket_key_from_portal(self, atid: str, atid_meta: dict = None) -> List[Tuple[str, str]]:
        """ Resolves the bucket, key combination for the given @id
            Raises GlacierRestoreException if not found

        :param atid: resource path to extract bucket, key information from
        :param atid_meta: metadata if already resolved (such as from search)
        :return: bucket, key tuple
        """
        if not atid_meta:
            atid_meta = get_metadata(atid, key=self.env_key, ff_env=self.env_name,
                                     add_on='frame=object&datastore=database')
        upload_key = atid_meta.get('upload_key', None)
        atid_types = atid_meta.get('@type', [])
        bucket = self.health_page.get('file_upload_bucket')
        if upload_key:
            if 'FileProcessed' in atid_types:
                bucket = self.health_page.get('processed_file_bucket')
                files = [(bucket, upload_key)]
            else:  # if not a processed file assume it is an uploaded file
                files = [(bucket, upload_key)]
        else:
            raise GlacierRestoreException(f'@id {atid} does not have an upload_key, thus cannot be queried for'
                                          f' Glacier restore.')
        # Add extra files
        if 'extra_files' in atid_meta:
            for extra_file in atid_meta['extra_files']:
                if 'upload_key' in extra_file:
                    files.append((bucket, extra_file['upload_key']))
        return files

    def get_portal_file_and_restore_from_glacier(self, atid: str, file_meta: Union[None, dict] = None,
                                                 versioning: bool = False, days: int = 7) -> (List[Tuple[str, str]],
                                                                                              List[Tuple[str, str]]):
        """ Resolves the given atid and restores it from glacier, returning the response if successful

        :param atid: resource path to extract bucket, key information from
        :param file_meta: object metadata if already resolved from file metadata upstream
        :param versioning: whether versioning should be considered, most recent is used
        :param days: number of days to store in the temporary location
        :return: arrays of success, failure tuples containing bucket, key
        """
        success, fail = [], []
        version_id = None
        file_meta = self.resolve_bucket_key_from_portal(atid, file_meta)
        for bucket, key in file_meta:
            if versioning:
                response = self.s3.list_object_versions(Bucket=bucket, Prefix=key)
                versions = sorted(response.get('Versions', []), key=lambda x: x['LastModified'], reverse=True)
                version_id = versions[0]['VersionId']
            resp = self.restore_s3_from_glacier(bucket, key, version_id=version_id, days=days)
            if resp:
                success.append((bucket, key))
            else:
                fail.append((bucket, key))
        return success, fail

    def restore_s3_from_glacier(self, bucket: str, key: str, days: int = 7,
                                version_id: str = None,) -> Union[dict, None]:
        """ Restores a file from glacier given the bucket, key and duration of restore

        :param bucket: bucket where the file is stored
        :param key: key under which the file is stored
        :param days: number of days to store in the temporary location
        :param version_id: version ID to restore if applicable
        :return: response, if successful, or else None
        """
        try:
            args = {
                'Bucket': bucket,
                'Key': key,
                'RestoreRequest': {'Days': days}
            }
            if version_id:
                args['VersionId'] = version_id
            response = self.s3.restore_object(**args)
            PRINT(f'Object {bucket}/{key} restored from Glacier storage class and will be available in S3'
                  f' for {days} days after restore has been processed (24 hours)')
            return response
        except Exception as e:
            PRINT(f'Error restoring object {key} from Glacier storage class: {str(e)}')
            return None

    def is_restore_finished(self, bucket: str, key: str) -> bool:
        """ Heads the object to see if it has been restored - note that from the POV of the API,
            the object is still in Glacier, but it has been restored to its original location and
            can be downloaded immediately

        :param bucket: bucket of original file location
        :param key: key of original file location
        :return: boolean whether the restore was successful yet
        """
        try:  # extract temporary location by heading object
            response = self.s3.head_object(Bucket=bucket, Key=key)
            restore = response.get('Restore')
            if restore is None:
                PRINT(f'Object {bucket}/{key} is not currently being restored from Glacier')
                return False
            if 'ongoing-request="false"' not in restore:
                PRINT(f'Object {bucket}/{key} is still being restored from Glacier')
                return False
            return True
        except Exception as e:
            PRINT(f'Error checking restore status of object {bucket}/{key} in S3: {str(e)}')
            return False

    def patch_file_lifecycle_status(self, atid: str, status: str = 'uploaded',
                                    s3_lifecycle_status: str = 'standard') -> dict:
        """ Patches the File @id object to update 3 things
                1. status denoted by the status argument (usually uploaded)
                2. s3_lifecycle_status to 'standard' by default
                3. s3_lifecycle_category deleted (so lifecycle transition doesn't occur again)

        :param atid: object resource path to update
        :param status: status to replace at top level
        :param s3_lifecycle_status: life cycle status to replace on file, typically standard as that is the default
        :return: response from patch_metadata
        """
        return patch_metadata(
            {
                'status': status,
                's3_lifecycle_status': s3_lifecycle_status
            },
            atid,
            key=self.env_key, ff_env=self.env_name,
            add_on='?delete_fields=s3_lifecycle_category'
        )

    def non_glacier_versions_exist(self, bucket: str, key: str) -> bool:
        """ Returns True if non-glacier tiered versions of an object exist,
            False otherwise.

        :param bucket: bucket to look in
        :param key: key to check
        :return: True if non-glacier versions exist, False otherwise
        """
        try:
            response = self.s3.list_object_versions(Bucket=bucket, Prefix=key)
            versions = sorted(response.get('Versions', []), key=lambda x: x['LastModified'], reverse=True)
            for v in versions:
                if v.get('StorageClass') not in S3_GLACIER_CLASSES:
                    return True
            return False
        except Exception as e:
            PRINT(f'Error checking versions for object {bucket}/key: {str(e)}')
            return False

    def delete_glaciered_object_versions(self, bucket: str, key: str, delete_all_versions: bool = False) -> bool:
        """ Deletes glaciered object versions of the given bucket/key, clearing all versions in glacier if the
            delete_all_versions flag is specified

        :param bucket: bucket location containing key
        :param key: file name in s3 to delete
        :param delete_all_versions: whether to delete all glacier versions or rather than just the most recent one
        :return: True if success or False if failed
        """
        try:
            response = self.s3.list_object_versions(Bucket=bucket, Prefix=key)
            versions = sorted(response.get('Versions', []), key=lambda x: x['LastModified'], reverse=True)
            deleted = False
            for v in versions:
                if v.get('StorageClass') in S3_GLACIER_CLASSES:
                    response = self.s3.delete_object(Bucket=bucket, Key=key, VersionId=v.get('VersionId'))
                    PRINT(f'Object {bucket}/{key} Glacier version {v.get("VersionId")} deleted:\n{response}')
                    deleted = True
                    if not delete_all_versions:
                        break
            if not deleted:
                PRINT(f'No Glacier version found for object {bucket}/{key}')
                return False
            return True
        except Exception as e:
            PRINT(f'Error deleting Glacier versions of object {bucket}/{key}: {str(e)}')
            return False

    @staticmethod
    def _format_tags(tags: List[dict]) -> str:
        """ Helper method that formats tags so that they match the format expected by the boto3 API

        :param tags: array of dictionaries containing Key, Value mappings to be reformatted
        :return: String formatted tag list ie:
            [{Key: key1, Value: value1}, Key: key2, Value: value2}] --> 'key1=value1&key2=value2'
        """
        return '&'.join([f'{tag["Key"]}={tag["Value"]}' for tag in tags])

    def _do_multipart_upload(self, bucket: str, key: str, total_size: int, part_size: int = 200,
                             storage_class: str = 'STANDARD', tags: str = '',
                             version_id: Union[str, None] = None) -> Union[dict, None]:
        """ Helper function for copy_object_back_to_original_location, not intended to
            be called directly, will arrange for a multipart copy of large updates
            to change storage class

        :param bucket: bucket to copy from
        :param key: key to copy within bucket
        :param total_size: total size of object
        :param part_size: what size to divide the object into when uploading the chunks
        :param storage_class: new storage class to use
        :param tags: string of tags to apply
        :param version_id: object version ID, if applicable
        :return: response, if successful, or else None
        """
        try:
            part_size = part_size * 1024 * 1024  # convert MB to B
            num_parts = int(total_size / part_size) + 1
            if num_parts > MAX_MULTIPART_CHUNKS:
                raise GlacierRestoreException(f'Must user a part_size larger than {part_size}'
                                              f' that will result in fewer than {MAX_MULTIPART_CHUNKS} chunks')
            cmu = {
                'Bucket': bucket, 'Key': key, 'StorageClass': storage_class
            }
            if tags:
                cmu['Tagging'] = tags
            if self.kms_key_id:
                cmu['ServerSideEncryption'] = 'aws:kms'
                cmu['SSEKMSKeyId'] = self.kms_key_id
            mpu = self.s3.create_multipart_upload(**cmu)
            mpu_upload_id = mpu['UploadId']
        except Exception as e:
            PRINT(f'Error creating multipart upload for {bucket}/{key} : {str(e)}')
            return None
        parts = []
        for i in range(num_parts):
            start = i * part_size
            end = min(start + part_size, total_size)
            part = {
                'PartNumber': i + 1
            }
            copy_source = {'Bucket': bucket, 'Key': key}
            copy_target = {
                'Bucket': bucket, 'Key': key,
            }
            if version_id:
                copy_source['VersionId'] = version_id
                copy_target['CopySourceVersionId'] = version_id

            # retry upload a few times
            for _ in range(3):
                try:
                    response = self.s3.upload_part_copy(
                        CopySource=copy_source, **copy_target,
                        PartNumber=i + 1,
                        CopySourceRange=f'bytes={start}-{end-1}',
                        UploadId=mpu_upload_id
                    )
                    break
                except Exception as e:
                    PRINT(f'Failed to upload part {i+1}, potentially retrying: {str(e)}')
            else:
                PRINT(f'Fatal error arranging multipart upload of {bucket}/{key},'
                      f' see previous output')
                return None
            part['ETag'] = response['CopyPartResult']['ETag']
            parts.append(part)

        # mark upload as completed
        # exception should be caught by caller
        return self.s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            MultipartUpload={
                'Parts': parts
            },
            UploadId=mpu_upload_id
        )

    def copy_object_back_to_original_location(self, bucket: str, key: str, storage_class: str = 'STANDARD',
                                              part_size: int = 200,  # MB
                                              preserve_lifecycle_tag: bool = False,
                                              version_id: Union[str, None] = None) -> Union[dict, None]:
        """ Reads the temporary location from the restored object and copies it back to the original location

        :param bucket: bucket where object is stored
        :param key: key within bucket where object is stored
        :param storage_class: new storage class for this object
        :param part_size: if doing a large copy, size of chunks to upload (in MB)
        :param preserve_lifecycle_tag: whether to keep existing lifecycle tag on the object
        :param version_id: version of object, if applicable
        :return: boolean whether the copy was successful
        """
        # Determine file size
        try:
            response = self.s3.head_object(Bucket=bucket, Key=key)
            size = response['ContentLength']
            multipart = (size >= MAX_STANDARD_COPY_SIZE)
            if not preserve_lifecycle_tag:  # default: preserve tags except 'Lifecycle'
                tags = self.s3.get_object_tagging(Bucket=bucket, Key=key).get('TagSet', [])
                tags = [tag for tag in tags if tag['Key'] != ENCODED_LIFECYCLE_TAG_KEY]
                tags = self._format_tags(tags)
                if not tags:
                    self.s3.delete_object_tagging(Bucket=bucket, Key=key)
            else:
                tags = ''
        except Exception as e:
            PRINT(f'Could not retrieve metadata on file {bucket}/{key} : {str(e)}')
            return None
        try:
            if multipart:
                return self._do_multipart_upload(bucket, key, size, part_size, storage_class, tags, version_id)
            else:
                # Force copy the object into standard in a single operation
                copy_source = {'Bucket': bucket, 'Key': key}
                copy_args = {
                    'Bucket': bucket, 'Key': key,
                    'StorageClass': storage_class,
                }
                if version_id:
                    copy_source['VersionId'] = version_id
                    copy_args['CopySourceVersionId'] = version_id
                if tags:
                    copy_args['Tagging'] = tags
                if self.kms_key_id:
                    copy_args['ServerSideEncryption'] = 'aws:kms'
                    copy_args['SSEKMSKeyId'] = self.kms_key_id
                response = self.s3.copy_object(
                    **copy_args, CopySource=copy_source
                )
                PRINT(f'Response from boto3 copy:\n{response}')
                PRINT(f'Object {bucket}/{key} copied back to its original location in S3')
                return response
        except Exception as e:
            PRINT(f'Error copying object {bucket}/{key} back to its original location in S3: {str(e)}')
            return None

    def restore_glacier_phase_one_restore(self, atid_list: List[Union[dict, str]], versioning: bool = False,
                                          days: int = 7) -> (List[str], List[str]):
        """ Triggers a restore operation for all @id in the @id list, returning a list of success and
            error objects.

        :param atid_list: list of @ids or actual file object metadata to restore from glacier
        :param versioning: whether to consider versioning, most recent version is used
        :param days: days to store the temporary copy
        :return: 2 tuple of success, error list of @ids
        """
        success, errors = [], []
        for atid in atid_list:
            if isinstance(atid, dict):
                _atid = atid['@id']
            else:
                _atid = atid
            _, current_error = self.get_portal_file_and_restore_from_glacier(_atid, file_meta=atid,
                                                                             versioning=versioning, days=days)
            if current_error:
                PRINT(f'Failed to restore bucket/keys: {current_error}')
                errors.append(_atid)
            else:  # no errors should occur
                success.append(_atid)
        if len(errors) != 0:
            PRINT(f'Errors encountered restoring @ids: {errors}')
        else:
            PRINT(f'Successfully triggered restore requests for all @ids passed {success}')
        return success, errors

    def restore_glacier_phase_two_copy(self, atid_list: List[Union[str, dict]], versioning: bool = False,
                                       storage_class: S3StorageClass = 'STANDARD',
                                       parallel: bool = False, num_threads: int = 4) -> (List[str], List[str]):
        """ Triggers a copy operation for all restored objects passed in @id list

        :param atid_list: list of @ids or actual file metadata objects to issue copy operations back to their
                          original bucket/key location
        :param versioning: whether to consider object versions
        :param storage_class: which storage class to copy into
        :param parallel: whether to parallelize the copy
        :param num_threads: number of threads to use when parallelizing, default to 4
        :return: 2 tuple of success, error list of @ids
        """
        success, errors = [], []
        if parallel:
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = []
                for atid in atid_list:
                    if isinstance(atid, dict):
                        _atid = atid['@id']
                    else:
                        _atid = atid
                    files_meta = self.resolve_bucket_key_from_portal(_atid, atid)
                    version_id = None
                    for bucket, key in files_meta:
                        if versioning:
                            response = self.s3.list_object_versions(Bucket=bucket, Prefix=key)
                            versions = sorted(response.get('Versions', []), key=lambda x: x['LastModified'],
                                              reverse=True)
                            version_id = versions[0]['VersionId']
                        future = executor.submit(  # noQA - TODO: PyCharm doesn't like this call for some reason
                            self.copy_object_back_to_original_location,
                            bucket=bucket, key=key, storage_class=storage_class, version_id=version_id)
                        futures.append(future)
                for future in tqdm(futures, total=len(atid_list)):
                    res = future.result()
                    if res:
                        success.append(res)
                    else:
                        errors.append(res)
        else:
            for atid in atid_list:
                if isinstance(atid, dict):
                    _atid = atid['@id']
                else:
                    _atid = atid
                files_meta = self.resolve_bucket_key_from_portal(_atid, atid)
                accumulated_results = []
                for bucket, key in files_meta:
                    version_id = None
                    if versioning:
                        response = self.s3.list_object_versions(Bucket=bucket, Prefix=key)
                        versions = sorted(response.get('Versions', []), key=lambda x: x['LastModified'], reverse=True)
                        version_id = versions[0]['VersionId']
                    resp = self.copy_object_back_to_original_location(bucket=bucket, key=key,
                                                                      storage_class=storage_class,
                                                                      version_id=version_id)
                    if resp:
                        accumulated_results.append(_atid)
                if len(accumulated_results) == len(files_meta):  # all files for this @id were successful
                    success.append(_atid)
                else:
                    errors.append(_atid)
        if len(errors) != 0:
            PRINT(f'Errors encountered copying @ids: {errors}')
        else:
            PRINT(f'Successfully triggered copy for all @ids passed {success}')
        return success, errors

    def restore_glacier_phase_three_patch(self, atid_list: List[Union[str, dict]],
                                          status: str = 'uploaded') -> (List[str], List[str]):
        """ Patches out lifecycle information for @ids we've transferred back to standard

        :param atid_list: list of @ids or actual file metadata objects to patch info on
        :param status: top level status to replace for files
        :return: 2 tuple of success, error list of @ids
        """
        success, errors = [], []
        for atid in atid_list:
            if isinstance(atid, dict):
                atid = atid['@id']
            try:
                self.patch_file_lifecycle_status(atid, status=status)
                success.append(atid)
            except Exception as e:
                PRINT(f'Error encountered patching @id {atid}, error: {str(e)}')
                errors.append(atid)
        return success, errors

    def restore_glacier_phase_four_cleanup(self, atid_list: List[str],
                                           delete_all_versions: bool = False) -> (List[str], List[str]):
        """ Triggers delete requests for all @ids for the glacierized objects, since they are in standard

        :param atid_list: list of @ids or actual file metadata objects to delete from glacier
        :param delete_all_versions: bool whether to clear all glacier versions
        :return: 2 tuple of success, error list of @ids
        """
        success, errors = [], []
        for atid in atid_list:
            if isinstance(atid, dict):
                _atid = atid['@id']
            else:
                _atid = atid
            bucket_key_pairs = self.resolve_bucket_key_from_portal(_atid, atid)
            accumulated_results = []
            for bucket, key in bucket_key_pairs:
                if self.non_glacier_versions_exist(bucket, key):
                    resp = self.delete_glaciered_object_versions(bucket, key, delete_all_versions=delete_all_versions)
                    if resp:
                        accumulated_results.append(_atid)
                else:
                    PRINT(f'Error cleaning up {bucket}/{key}, no non-glaciered versions'
                          f' exist, ignoring this file and erroring on @id {_atid}')
            if len(accumulated_results) == len(bucket_key_pairs):
                success.append(_atid)
            else:
                errors.append(_atid)
        if len(errors) != 0:
            PRINT(f'Errors encountered deleting glaciered @ids: {errors}')
        else:
            PRINT(f'Successfully triggered delete for all @ids passed {success}')
        return success, errors

    @require_confirmation
    def restore_all_from_search(self, *, search_query: str, page_limit: int = 50, search_generator: bool = False,
                                restore_length: int = 7, new_status: str = 'uploaded',
                                storage_class: S3StorageClass = 'STANDARD', versioning: bool = False,
                                parallel: bool = False, num_threads: int = 4, delete_all_versions: bool = False,
                                phase: int = 1) -> (List[str], List[str]):
        """ Overarching method that will take a search query and loop through all files in the
            search results, running the appropriate phase as passed

        :param search_query: search query used to resolve items desired to be restored
        :param page_limit: number of pages to resolve ie: 25 * page_limit items, 25 * 50 = 1250 items by default
        :param search_generator: whether to use a generator - can be useful in some steps, but not in the copy
        :param restore_length: length of time for restore to be active in days
        :param new_status: status to patch to file items
        :param storage_class: new storage class for copy
        :param versioning: whether versioning should be taken into consideration - most recent version is used
        :param parallel: whether to use the parallel copy
        :param num_threads: number of threads to use if parallel is active
        :param delete_all_versions: if deleting, whether to clear ALL glacier versions
        :param phase: which phase of the glacier restore to run, one of [1, 2, 3, 4]
        :return: 2-tuple of successful, failed @ids extracted from search
        """
        if phase not in [1, 2, 3, 4]:
            raise GlacierRestoreException(f'Invalid phase passed to restore_all_from_search: {phase},'
                                          f' valid phases: [1, 2, 3, 4]\n'
                                          f'Phase 1: Issue Restore\n'
                                          f'Phase 2: Issue Copy\n'
                                          f'Phase 3: Patch lifecycle status\n'
                                          f'Phase 4: Delete Glacier copies')
        success, failed = [], []
        if search_generator:  # operate on search results individually, fine for all phases except the copy (2)
            for item_meta in search_metadata(
                search_query,
                key=self.env_key, ff_env=self.env_name,
                page_limit=page_limit, is_generator=search_generator
            ):
                if phase == 1:
                    current_success, current_failed = self.restore_glacier_phase_one_restore(
                        [item_meta], days=restore_length, versioning=versioning
                    )
                    success += current_success
                    failed += current_failed
                elif phase == 2:
                    if parallel:
                        raise GlacierRestoreException(f'Invalid phase for search_generator=True!'
                                                      f' Do not use a generator when doing the copy phase in parallel'
                                                      f' mode.')
                    else:
                        current_success, current_failed = self.restore_glacier_phase_two_copy([item_meta], versioning,
                                                                                              storage_class)
                        success += current_success
                        failed += current_failed
                elif phase == 3:
                    current_success, current_failed = self.restore_glacier_phase_three_patch([item_meta],
                                                                                             status=new_status)
                    success += current_success
                    failed += current_failed
                else:  # phase == 4
                    current_success, current_failed = self.restore_glacier_phase_four_cleanup(
                        atid_list=[item_meta],
                        delete_all_versions=delete_all_versions
                    )
                    success += current_success
                    failed += current_failed
            return success, failed
        else:  # generate all results immediately - tune page_length if resolving results is too slow
            search_results = search_metadata(
                search_query,
                key=self.env_key, ff_env=self.env_name,
                page_limit=page_limit
            )
            if phase == 1:
                return self.restore_glacier_phase_one_restore(atid_list=search_results, versioning=versioning,
                                                              days=restore_length)
            elif phase == 2:
                return self.restore_glacier_phase_two_copy(atid_list=search_results, versioning=versioning,
                                                           parallel=parallel, storage_class=storage_class,
                                                           num_threads=num_threads)
            elif phase == 3:
                return self.restore_glacier_phase_three_patch(atid_list=search_results,
                                                              status=new_status)
            else:  # phase == 4
                return self.restore_glacier_phase_four_cleanup(atid_list=search_results,
                                                               delete_all_versions=delete_all_versions)
