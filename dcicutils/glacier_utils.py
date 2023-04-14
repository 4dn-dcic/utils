import boto3
from typing import Union
from .misc_utils import PRINT
from .ff_utils import get_metadata, get_health_page
from .creds_utils import KeyManager


class GlacierRestoreException(Exception):
    pass


class GlacierUtils:

    def __init__(self, env_name):
        """ Pass an env_name that exists in your ~/.cgap_keys.json or ~/.fourfront_keys.json file
            No support for admin keys!
        """
        self.s3 = boto3.client('s3')
        self.env_name = env_name
        self.key_manager = KeyManager()
        self.env_key = self.key_manager.get_keydict_for_env(env_name)
        self.health_page = get_health_page(key=self.env_key, ff_env=env_name)

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

    def extract_temporary_s3_location_from_restore_response(self, bucket: str, key: str) -> Union[str, None]:
        """ Extracts the S3 location that the restored file will be sent to given the bucket and key

        :param bucket: bucket of original file location
        :param key: key of original file location
        :return: path or None
        """
        try:
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
            return restore_path
        except Exception as e:
            PRINT(f'Error copying object {bucket}/{key} back to its original location in S3: {str(e)}')
            return None

    def copy_object_back_to_original_location(self, bucket: str, key: str, restore_path: Union[str, None] = None,
                                              version_id: Union[str, None] = None) -> Union[bool, None]:
        """ Reads the temporary location from the restored object and copies it back to the original location

        :param bucket: bucket where object is stored
        :param key: key within bucket where object is stored
        :param restore_path: path to which the object was restored, can be empty
        :param version_id: version of object, if applicable
        :return: boolean whether the copy was successful
        """
        try:
            if not restore_path:
                # Get the restored object's temporary location by heading the existing object
                response = self.s3.head_object(Bucket=bucket, Key=key)
                restore = response.get('Restore')
                if restore is None:
                    PRINT(f'Object {bucket}/{key} is not currently being restored from Glacier')
                    return False
                if 'ongoing-request="false"' not in restore:
                    PRINT(f'Object {bucket}/{key} is still being restored from Glacier')
                    return False
                restore_path = response.get('RestoreOutputPath')
                if restore_path is None:
                    PRINT(f'Error: Could not determine the temporary location of restored object {bucket}/{key}')
                    return False

            # Copy the object from the temporary location to its original location
            copy_source = {'Bucket': bucket, 'Key': key, 'Restore': restore}
            copy_args = {'Bucket': bucket, 'Key': key}
            if version_id:
                copy_source['VersionId'] = version_id
                copy_args['CopySourceVersionId'] = version_id
            response = self.s3.copy_object(CopySource=copy_source, **copy_args)
            PRINT(f'Response from boto3 copy:\n{response}')
            PRINT(f'Object {bucket}/{key} copied back to its original location in S3')
            return True
        except Exception as e:
            PRINT(f'Error copying object {bucket}/{key} back to its original location in S3: {str(e)}')
            return None
