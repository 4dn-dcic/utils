""" Functions to support general aws operations

    Function names should generally follow a convention to include a short string
    to indicate the service that they are designed to operate with eg. 's3' for AWS s3.
"""
import boto3
import logging
import mimetypes
import json
import os
import requests

from botocore.exceptions import ClientError
from .exceptions import ()
from .misc_utils import PRINT
from .s3_utils import s3Utils

###########################
# Config
###########################
logging.basicConfig()
logger = logging.getLogger(__name__)


def s3_bucket_head(*, bucket_name, s3=None):
    """ Gets head info for a bucket if it exists

    :param bucket_name: name of the bucket - string
    :param s3: AWS s3 client
    :return: dict: head response or None
    """
    try:
        s3 = s3 or s3Utils().s3
        info = s3.head_bucket(Bucket=bucket_name)
        return info
    except ClientError:
        return None


def s3_bucket_exists(*, bucket_name, s3=None):
    """ Does a bucket exist?

    :param bucket_name: name of the bucket - string
    :param s3: AWS s3 client
    :return: boolean - True if exists, False if not
    """
    return bool(s3_bucket_head(bucket_name=bucket_name, s3=s3))


def s3_bucket_object_count(bucket_name):
    """ Number of objects in the given s3 bucket

    NB: this works with locally stored credentials - not sure
    if something needs to be added or if it is possible to create
    a resource from a client or provide credentials if not locally stored

    :param bucket_name: name of the bucket - string
    :return: int - number of objects in bucket
    """
    bucket = boto3.resource('s3').Bucket(bucket_name)
    # get only head of objects so we can count them
    return sum(1 for _ in bucket.objects.all())


def s3_object_head(*, object_key, bucket_name, s3=None):
    """ Gets head info for a object if it exists in provided bucket

    :param object_key: key for the object - string
    :param bucket_name: name of the bucket to check for the object - string
    :param s3: AWS s3 client
    :return: dict - head response or None
    """
    try:
        s3 = s3 or s3Utils().s3
        info = s3.head_object(Bucket=bucket_name, Key=object_key)
        return info
    except ClientError:
        return None


def s3_object_exists(*, object_key, bucket_name, s3=None):
    """ Does an object exist in the given bucket?

    :param object_key: key for the object - string
    :param bucket_name: name of the bucket - string
    :param s3: AWS s3 client
    :return: boolean - True if exists, False if not
    """
    return bool(s3_object_head(object_key=object_key, bucket_name=bucket_name, s3=s3))


def s3_put_object(*, object_key, obj, bucket_name, acl=None, s3=None):
    """ Add an object to the given bucket

        NB: add specfic upload functions that use this?

    :param object_key: key for the object - string
    :param obj: object data - bytes or seekable file-like object
    :param bucket_name: name of the bucket to check for the object - string
    :param acl: The (optional) canned ACL to apply to the object.
    :param s3: AWS s3 client
    :return: ETag of the put object
    """
    s3 = s3 or s3Utils().s3
    # try to guess content type
    content_type = mimetypes.guess_type(object_key)[0]
    if content_type is None:
        content_type = 'binary/octet-stream'
    # TODO: ? do we want to calc md5sum and check against that to ensure full upload ?
    # perhaps as optional parameter
    try:
        if acl:
            return s3.put_object(Bucket=bucket_name,
                                 Key=object_key,
                                 Body=obj,
                                 ContentType=content_type,
                                 ACL=acl)
        else:
            return s3.put_object(Bucket=bucket_name,
                                 Key=object_key,
                                 Body=obj,
                                 ContentType=content_type)
    except ClientError:
        # how to handle errors here?
        return None


def delete_mark_s3_object(*, object_key, bucket_name, s3=None):
    """ Delete Mark an object in the given bucket
        Versioning must be enabled on the bucket

    :param object_key: key for the object - string
    :param bucket_name: name of the bucket - string
    :param s3: AWS s3 client
    :return: string - versionId of the delete marker
    """
    s3 = s3 or s3Utils().s3
    try:
        if not s3.get_bucket_versioning(Bucket=bucket_name).get('Status') == 'Enabled':  # # check that versioning is enabled
            raise "versioning is disabled on {} - cannot delete mark {}".format(bucket_name, object_key)
        return s3.delete_object(Bucket=bucket, Key=key)
    except ClientError:
        return None


def delete_s3_object_version(*, object_key, bucket_name, version_id=None, s3=None):
    """ Delete the version of an object in the given bucket if the bucket is version enabled
        Or delete the object if is in an unversioned bucket.  If you do not provide a
        version_id and a version enabled bucket an Exception is raised.  'null' is returned
        as the version_id for an version disabled bucket delete
    NB: providing 'null' as version_id is allowed for version disable buckets
    NB: This is currently agnostic as to whether the object exists or not

    :param object_key: key for the object - string
    :param bucket_name: name of the bucket - string
    :param version_id: version id for version to delete - string
    :param s3: AWS s3 client
    :return: string - versionId of the deleted version
    """
    s3 = s3 or s3Utils().s3
    try:
        versioning = s3.get_bucket_versioning(Bucket=bucket_name).get('Status')
    except ClientError, AttributeError as e:
        logger.error(str(e))
        return None

    try:
        if versioning == 'Enabled' and version_id and version_id != 'null':
            logger.info("Deleting version {version_id} of object {object_id} from version enabled {bucket_name}".
                        format(version_id=version_id, object_id=object_id, bucket_name=bucket_name))
            res = s3.delete_object(Bucket=bucket, Key=key, VersionId=version_id)
        elif not version_id or version_id == 'null':
            logger.info("Deleting object {object_id} from version disabled {bucket_name}".
                        format(object_id=object_id, bucket_name=bucket_name))
            res = s3.delete_object(Bucket=bucket, Key=key)
    except ClientError as e:
        logger.error(str(e))
        return None
    if res.get('ResponseMetadata').get('HTTPStatusCode') == 204:
        # the object.version is no longer in the bucket (or maybe never was)
        if 'VersionId' in res:
            return res.get('VersionId')
        return 'null'
    else:
        # what's a good thing to do here?  logging, raise exception
        raise("Unexpected response status - {}".format(res))
        return None


def delete_s3_object_completely(*, object_key, bucket_name, s3):
    """ Delete all the versions of an object in the given bucket

    :param object_key: key for the object - string
    :param bucket_name: name of the bucket - string
    :param s3: AWS s3 client
    :return: boolean - True if all expected versions were deleted
    """
    s3 = s3 or s3Utils().s3
    expected_cnt = None
    deleted_cnt = 0
    if s3.get_bucket_versioning(Bucket=bucket_name).get('Status') == 'Disabled':
        expected_cnt = 1
        if delete_s3_object_version(object_key=key, bucket_name=bucket_name, s3):
            deleted_cnt += 1
    else:
        ver_res = s3.list_object_versions(Bucket=bucket_name, Prefix=object_key)
        if ver_res.get('ResponseMetadata').get('HTTPStatusCode') == 200:
            if ver_res.get('ResponseMetadata').get('IsTruncated'):
                logger.warning("Too many versions of {} in {} - incomplete delete".format(object_key, bucket_name))
            delete_markers = ver_res.get('DeleteMarkers', [])
            versions = ver_res.get('Versions', [])
            versions.extend(delete_markers)
            expected_cnt = len(versions)
            for version in versions:
                version_id = version.get('VersionId')
                res = delete_s3_object_version(object_key=key, bucket_name=bucket_name, version_id=version_id, s3)
                if not res:
                    logger.warning("Problem with delete of {} - version id {} from {}".format(object_key, version_id, bucket_name))
                else:
                    deleted_cnt += 1
    if expected_cnt:
        if expected_cnt == deleted_cnt:
            logger.info("Deleted {} versions of {} from {}".format(deleted_cnt, object_key, bucket_name))
            return True
        else:
            logger.warning("Expected to delete {expected} and DELETED {delcnt}".format(expected=expected_cnt, delcnt=deleted_cnt))
            return False
    return False

# def read_s3_object():
#     pass
#     response = self.s3.get_object(Bucket=self.outfile_bucket, Key=filename)
#     logger.info(str(response))
#     return response['Body'].read()
#
# def s3_read_dir(self, prefix):
#     return self.s3.list_objects(Bucket=self.outfile_bucket, Prefix=prefix)
#
# def s3_delete_dir(self, prefix):
#     # one query get list of all the files we want to delete
#     obj_list = self.s3.list_objects(Bucket=self.outfile_bucket, Prefix=prefix)
#     files = obj_list.get('Contents', [])
#
#     # morph file list into format that boto3 wants
#     delete_keys = {'Objects': [{'Key': k}
#                                for k in [obj['Key']
#                                          for obj in files]]}
#
#     # second query deletes all the files, NOTE: Max 1000 files
#     if delete_keys['Objects']:
#         self.s3.delete_objects(Bucket=self.outfile_bucket, Delete=delete_keys)
