import boto3
from typing import Union, List
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm
from .misc_utils import PRINT
from .ff_utils import get_metadata, get_health_page, patch_metadata
from .creds_utils import KeyManager


GLACIER_CLASSES = [
    'GLACIER',
    'DEEP_ARCHIVE'
]


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
        """
        self.s3 = boto3.client('s3')
        self.env_name = env_name
        self.key_manager = KeyManager()
        self.env_key = self.key_manager.get_keydict_for_env(env_name)
        self.health_page = get_health_page(key=self.env_key, ff_env=env_name)

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

    def resolve_bucket_key_from_portal(self, atid: str) -> (str, str):
        """ Resolves the bucket, key combination for the given @id
            Raises GlacierRestoreException if not found

        :param atid: resource path to extract bucket, key information from
        :return: bucket, key tuple
        """
        atid_meta = get_metadata(atid, key=self.env_key, ff_env=self.env_name,
                                 add_on='frame=object&datastore=database')
        upload_key = atid_meta.get('upload_key', None)
        atid_types = atid_meta.get('@type', [])
        if upload_key:
            if 'FileProcessed' in atid_types:
                return self.health_page.get('processed_file_bucket'), upload_key
            else:  # if not a processed file assume it is an uploaded file
                return self.health_page.get('file_upload_bucket'), upload_key
        else:
            raise GlacierRestoreException(f'@id {atid} does not have an upload_key, thus cannot be queried for'
                                          f' Glacier restore.')

    def restore_portal_from_glacier(self, atid: str, days: int = 7) -> Union[dict, None]:
        """ Resolves the given atid and restores it from glacier, returning the response if successful

        :param atid: resource path to extract bucket, key information from
        :param days: number of days to store in the temporary location
        :return: response if successful or None
        """
        bucket, key = self.resolve_bucket_key_from_portal(atid)
        return self.restore_s3_from_glacier(bucket, key, days=days)

    def restore_s3_from_glacier(self, bucket: str, key: str, days: int = 7) -> Union[dict, None]:
        """ Restores a file from glacier given the bucket, key and duration of restore

        :param bucket: bucket where the file is stored
        :param key: key under which the file is stored
        :param days: number of days to store in the temporary location
        :return: response if successful or None
        """
        try:
            response = self.s3.restore_object(
                Bucket=bucket,
                Key=key,
                RestoreRequest={'Days': days}
            )
            PRINT(f'Object {bucket}/{key} restored from Glacier storage class and will be available in S3'
                  f' for {days} days')
            return response
        except Exception as e:
            PRINT(f'Error restoring object {key} from Glacier storage class: {str(e)}')
            return None

    def extract_temporary_s3_location_from_restore_response(self, bucket: str, key: str) -> Union[dict, None]:
        """ Extracts the S3 location that the restored file will be sent to given the bucket and key

        :param bucket: bucket of original file location
        :param key: key of original file location
        :return: Restore object from s3
        """
        try:  # extract temporary location by heading object
            response = self.s3.head_object(Bucket=bucket, Key=key)
            restore = response.get('Restore')
            if restore is None:
                PRINT(f'Object {bucket}/{key} is not currently being restored from Glacier')
                return None
            if 'ongoing-request="false"' not in restore:
                PRINT(f'Object {bucket}/{key} is still being restored from Glacier')
                return None
            restore_path = response.get('RestoreOutputPath')
            if restore_path is None:
                PRINT(f'Error: Could not determine the temporary location of restored object {bucket}/{key}')
                return None
            return restore
        except Exception as e:
            PRINT(f'Error copying object {bucket}/{key} back to its original location in S3: {str(e)}')
            return None

    def patch_file_lifecycle_status_to_standard(self, atid: str, status: str,
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

    def delete_glaciered_object_versions(self, bucket: str, key: str, delete_all_versions: bool = False) -> bool:
        """ Deletes glaciered object versions of the given bucket/key, clearing all versions in glacier if the
            delete_all_versions flag is specified

        :param bucket: bucket location containing key
        :param key: file name in s3 to delete
        :param delete_all_versions: whether or not to delete all glacier versions or just the most recent one
        :return: True if success or False if failed
        """
        try:
            # Retrieve the object versions for the key
            response = self.s3.list_object_versions(Bucket=bucket, Prefix=key)
            versions = response.get('Versions', [])
            versions.sort(key=lambda x: x['VersionId'], reverse=True)  # most recent version will be first

            # Delete all glaciered versions if the flag is set
            if delete_all_versions:
                glacier_versions = [v for v in versions if v.get('StorageClass') in GLACIER_CLASSES]
                for v in glacier_versions:
                    response = self.s3.delete_object(
                        Bucket=bucket,
                        Key=key,
                        VersionId=v.get('VersionId')
                    )
                    PRINT(f"Object {key} Glacier version {v.get('VersionId')} deleted:\n{response}")
                # no Glacier versions were found
                if not glacier_versions:
                    PRINT(f"No Glacier versions found for object {key}")
                    return False
                else:
                    return True
            else:
                # Find the first glacierized version and delete it
                for v in versions:
                    if v.get('StorageClass') in GLACIER_CLASSES:
                        response = self.s3.delete_object(
                            Bucket=bucket,
                            Key=key,
                            VersionId=v.get('VersionId')
                        )
                        PRINT(f"Object {key} Glacier version {v.get('VersionId')} deleted:\n{response}")
                        break
                else:
                    PRINT(f"No Glacier version found for object {key}")

        except Exception as e:
            PRINT(f"Error deleting Glacier versions of object {key}: {str(e)}")
            return False

    def copy_object_back_to_original_location(self, bucket: str, key: str,
                                              version_id: Union[str, None] = None) -> Union[dict, None]:
        """ Reads the temporary location from the restored object and copies it back to the original location

        :param bucket: bucket where object is stored
        :param key: key within bucket where object is stored
        :param version_id: version of object, if applicable
        :return: boolean whether the copy was successful
        """
        try:
            # Get temporary location
            restore = self.extract_temporary_s3_location_from_restore_response(bucket, key)
            # Copy the object from the temporary location to its original location
            copy_source = {'Bucket': bucket, 'Key': key, 'Restore': restore}
            copy_args = {'Bucket': bucket, 'Key': key}
            if version_id:
                copy_source['VersionId'] = version_id
                copy_args['CopySourceVersionId'] = version_id
            response = self.s3.copy_object(CopySource=copy_source, **copy_args)
            PRINT(f'Response from boto3 copy:\n{response}')
            PRINT(f'Object {bucket}/{key} copied back to its original location in S3')
            return response
        except Exception as e:
            PRINT(f'Error copying object {bucket}/{key} back to its original location in S3: {str(e)}')
            return None

    def restore_glacier_phase_one_restore(self, atid_list: List[str], days: int = 7) -> (List[str], List[str]):
        """ Triggers a restore operation for all @id in the @id list, returning a list of success and
            error objects.

        :param atid_list: list of @ids to restore from glacier
        :param days: days to store the temporary copy
        :return: 2 tuple of success, error list of @ids
        """
        success, errors = [], []
        for atid in atid_list:
            resp = self.restore_portal_from_glacier(atid, days=days)
            if resp:
                success.append(atid)
            else:
                errors.append(atid)
        if len(errors) == 0:
            PRINT(f'Errors encountered restoring @ids: {errors}')
        else:
            PRINT(f'Successfully triggered restore requests for all @ids passed {success}')
        return success, errors

    def restore_glacier_phase_two_copy(self, atid_list: List[str],
                                       parallel: bool = False, num_threads: int = 4) -> (List[str], List[str]):
        """ Triggers a copy operation for all restored objects passed in @id list

        :param atid_list: list of @ids to issue copy operations back to their original bucket/key location
        :param parallel: whether or not to parallelize the copy
        :param num_threads: number of threads to use when parallelizing, default to 4
        :return: 2 tuple of success, error list of @ids
        """
        success, errors = [], []
        if parallel:
            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = []
                for atid in atid_list:
                    bucket, key = self.resolve_bucket_key_from_portal(atid)
                    future = executor.submit(self.copy_object_back_to_original_location, bucket, key)
                    futures.append(future)
                for future in tqdm(futures, total=len(atid_list)):
                    res = future.result()
                    if res:
                        success.append(res)
                    else:
                        errors.append(res)
        else:
            for atid in atid_list:
                bucket, key = self.resolve_bucket_key_from_portal(atid)
                resp = self.copy_object_back_to_original_location(bucket, key)
                if resp:
                    success.append(atid)
                else:
                    errors.append(atid)
        if len(errors) == 0:
            PRINT(f'Errors encountered copying @ids: {errors}')
        else:
            PRINT(f'Successfully triggered copy for all @ids passed {success}')
        return success, errors

    def restore_glacier_phase_three_patch(self, atid_list: List[str], status='uploaded') -> (List[str], List[str]):
        """ Patches out lifecycle information for @ids we've transferred back to standard
        :param atid_list: list of @ids to patch info on
        :param status: top level status to replace for files
        :return: 2 tuple of success, error list of @ids
        """
        success, errors = [], []
        for atid in atid_list:
            try:
                self.patch_file_lifecycle_status_to_standard(atid, status=status)
                success.append(atid)
            except Exception as e:
                PRINT(f'Error encountered patching @id {atid}, error: {str(e)}')
                errors.append(atid)
        return success, errors

    def restore_glacier_phase_four_cleanup(self, atid_list: List[str],
                                            delete_all_versions: bool = False) -> (List[str], List[str]):
        """ Triggers delete requests for all @ids for the glacierized objects, since they are in standard

        :param atid_list: list of @ids to delete from glacier
        :param delete_all_versions: bool whether to clear all glacier versions
        :return: 2 tuple of success, error list of @ids
        """
        success, errors = [], []
        for atid in atid_list:
            bucket, key = self.resolve_bucket_key_from_portal(atid)
            resp = self.delete_glaciered_object_versions(bucket, key, delete_all_versions=delete_all_versions)
            if resp:
                success.append(atid)
            else:
                errors.append(atid)
        if len(errors) == 0:
            PRINT(f'Errors encountered deleting glaciered @ids: {errors}')
        else:
            PRINT(f'Successfully triggered delete for all @ids passed {success}')
        return success, errors
