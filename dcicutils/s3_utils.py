from __future__ import print_function
import json
import boto3
import os
import mimetypes
from zipfile import ZipFile
from io import BytesIO
import logging
import requests
from .env_utils import is_stg_or_prd_env, prod_bucket_env
from .misc_utils import PRINT


###########################
# Config
###########################
logging.basicConfig()
logger = logging.getLogger(__name__)


class s3Utils(object):  # NOQA - This class name violates style rules, but a lot of things might break if we change it.

    SYS_BUCKET_TEMPLATE = "elasticbeanstalk-%s-system"
    OUTFILE_BUCKET_TEMPLATE = "elasticbeanstalk-%s-wfoutput"
    RAW_BUCKET_TEMPLATE = "elasticbeanstalk-%s-files"
    BLOB_BUCKET_TEMPLATE = "elasticbeanstalk-%s-blobs"

    @staticmethod
    def verify_and_get_env_config(s3_client, global_bucket: str):
        """ Verifies the S3 environment from which the env config is coming from, and returns the S3-based env config
            Throws exceptions if the S3 bucket is unreachable, or an env based on the name of the global S3 bucket
            is not present.
        """
        head_response = s3_client.head_bucket(Bucket=global_bucket)
        status = head_response['ResponseMetadata']['HTTPStatusCode']  # should be 200; raise error for 404 or 403
        if status != 200:
            raise Exception('Could not access GLOBAL_BUCKET_ENV {global_bucket}: status: {status}'.format(
                global_bucket=global_bucket, status=status))
        # list contents of global bucket, look for a match with the global bucket name
        list_response = s3_client.list_objects_v2(Bucket=global_bucket)
        # no match, raise exception
        if list_response['KeyCount'] < 1:
            raise Exception('No config objects found in global bucket {global_bucket}'.format(
                global_bucket=global_bucket))
        keys = [list_response['Contents'][i]['Key'] for i in range(0, len(list_response['Contents']))]
        config_filename = None
        for filename in keys:
            # multiple matches, raise exception
            if filename in global_bucket and config_filename is not None:
                raise Exception('multiple matches for global env bucket: {global_bucket}; keys: {keys}'.format(
                    global_bucket=global_bucket,
                    keys=keys,
                ))
            elif filename in global_bucket:
                config_filename = filename
            else:
                pass
        if not config_filename:
            raise Exception('no matches for global env bucket: {global_bucket}; keys: {keys}'.format(
                global_bucket=global_bucket,
                keys=keys,
            ))
        else:
            # one match, fetch that file as config
            get_response = s3_client.get_object(Bucket=global_bucket, Key=config_filename)
            env_config = json.loads(get_response['Body'].read())
            return env_config

    def __init__(self, outfile_bucket=None, sys_bucket=None, raw_file_bucket=None,
                 blob_bucket=None, metadata_bucket=None, env=None):
        """ Initializes s3 utils in one of three ways:
        1) If 'GLOBAL_BUCKET_ENV' is set to an S3 env bucket, use that bucket to fetch the env for the buckets.
           We then use this env to build the bucket names.
        2) With no global env set, if we instead pass in the env kwarg, we use this kwarg to build the bucket names.
        3) With no global env or env kwarg, we expect bucket kwargs to be set, and use those as bucket names directly.
        """
        # avoid circular ref
        from .beanstalk_utils import get_beanstalk_real_url
        self.url = ''
        self.s3 = boto3.client('s3', region_name='us-east-1')
        global_bucket = os.environ.get('GLOBAL_BUCKET_ENV')
        if global_bucket:
            env_config = self.verify_and_get_env_config(s3_client=self.s3, global_bucket=global_bucket)
            ff_url = env_config['fourfront']
            health_json = requests.get('{ff_url}/health?format=json'.format(ff_url=ff_url)).json()
            sys_bucket = health_json['system_bucket']
            outfile_bucket = health_json['processed_file_bucket']
            raw_file_bucket = health_json['file_upload_bucket']
            blob_bucket = health_json['blob_bucket']
            metadata_bucket = health_json['metadata_bundles_bucket']
        elif sys_bucket is None:
            # staging and production share same buckets
            if env:
                if is_stg_or_prd_env(env):
                    self.url = get_beanstalk_real_url(env)
                    env = prod_bucket_env(env)
            # we use standardized naming schema, so s3 buckets always have same prefix
            sys_bucket = self.SYS_BUCKET_TEMPLATE % env
            outfile_bucket = self.OUTFILE_BUCKET_TEMPLATE % env
            raw_file_bucket = self.RAW_BUCKET_TEMPLATE % env
            blob_bucket = self.BLOB_BUCKET_TEMPLATE % env

        self.sys_bucket = sys_bucket
        self.outfile_bucket = outfile_bucket
        self.raw_file_bucket = raw_file_bucket
        self.blob_bucket = blob_bucket
        self.metadata_bucket = metadata_bucket

    ACCESS_KEYS_S3_KEY = 'access_key_admin'

    def get_access_keys(self, name=ACCESS_KEYS_S3_KEY):
        keys = self.get_key(keyfile_name=name)
        if not isinstance(keys, dict):
            raise ValueError("Remotely stored access keys are not in the expected form")

        if isinstance(keys.get('default'), dict):
            keys = keys['default']
        if self.url:
            keys['server'] = self.url
        return keys

    def get_ff_key(self):
        return self.get_access_keys()

    def get_higlass_key(self):
        # higlass key corresponds to Django server super user credentials
        return self.get_key(keyfile_name='api_key_higlass')

    def get_google_key(self):
        return self.get_key(keyfile_name='api_key_google')

    def get_jupyterhub_key(self):
        # jupyterhub key is a Jupyterhub API token
        return self.get_key(keyfile_name='api_key_jupyterhub')

    def get_key(self, keyfile_name='access_key_admin'):
        # Share secret encrypted S3 File
        response = self.s3.get_object(Bucket=self.sys_bucket,
                                      Key=keyfile_name,
                                      SSECustomerKey=os.environ['S3_ENCRYPT_KEY'],
                                      SSECustomerAlgorithm='AES256')
        akey = response['Body'].read()
        if type(akey) == bytes:
            akey = akey.decode()
        try:
            return json.loads(akey)
        except (ValueError, TypeError):
            # maybe its not json after all
            return akey

    def read_s3(self, filename):
        response = self.s3.get_object(Bucket=self.outfile_bucket, Key=filename)
        logger.info(str(response))
        return response['Body'].read()

    def does_key_exist(self, key, bucket=None, print_error=True):
        if not bucket:
            bucket = self.outfile_bucket
        try:
            file_metadata = self.s3.head_object(Bucket=bucket, Key=key)
        except Exception as e:
            if print_error:
                PRINT("object %s not found on bucket %s" % (str(key), str(bucket)))
                PRINT(str(e))
            return False
        return file_metadata

    def get_file_size(self, key, bucket=None, add_bytes=0, add_gb=0,
                      size_in_gb=False):
        """
        default returns file size in bytes,
        unless size_in_gb = True
        """
        meta = self.does_key_exist(key, bucket)
        if not meta:
            raise Exception("key not found")
        one_gb = 1073741824
        add = add_bytes + (add_gb * one_gb)
        size = meta['ContentLength'] + add
        if size_in_gb:
            size = size / one_gb
        return size

    def delete_key(self, key, bucket=None):
        if not bucket:
            bucket = self.outfile_bucket
        self.s3.delete_object(Bucket=bucket, Key=key)

    @classmethod
    def size(cls, bucket):
        sbuck = boto3.resource('s3').Bucket(bucket)
        # get only head of objects so we can count them
        return sum(1 for _ in sbuck.objects.all())

    def s3_put(self, obj, upload_key, acl=None):
        """
        try to guess content type
        """
        content_type = mimetypes.guess_type(upload_key)[0]
        if content_type is None:
            content_type = 'binary/octet-stream'
        if acl:
            # we use this to set some of the object as public
            return self.s3.put_object(Bucket=self.outfile_bucket,
                                      Key=upload_key,
                                      Body=obj,
                                      ContentType=content_type,
                                      ACL=acl)
        else:
            return self.s3.put_object(Bucket=self.outfile_bucket,
                                      Key=upload_key,
                                      Body=obj,
                                      ContentType=content_type)

    def s3_put_secret(self, data, keyname, bucket=None, secret=None):
        if not bucket:
            bucket = self.sys_bucket
        if not secret:
            secret = os.environ["S3_ENCRYPT_KEY"]
        return self.s3.put_object(Bucket=bucket,
                                  Key=keyname,
                                  Body=data,
                                  SSECustomerKey=secret,
                                  SSECustomerAlgorithm='AES256')

    def s3_read_dir(self, prefix):
        return self.s3.list_objects(Bucket=self.outfile_bucket, Prefix=prefix)

    def s3_delete_dir(self, prefix):
        # one query get list of all the files we want to delete
        obj_list = self.s3.list_objects(Bucket=self.outfile_bucket, Prefix=prefix)
        files = obj_list.get('Contents', [])

        # morph file list into format that boto3 wants
        delete_keys = {'Objects': [{'Key': k}
                                   for k in [obj['Key']
                                             for obj in files]]}

        # second query deletes all the files, NOTE: Max 1000 files
        if delete_keys['Objects']:
            self.s3.delete_objects(Bucket=self.outfile_bucket, Delete=delete_keys)

    def read_s3_zipfile(self, s3key, files_to_extract):
        s3_stream = self.read_s3(s3key)
        bytestream = BytesIO(s3_stream)
        zipstream = ZipFile(bytestream, 'r')
        ret_files = {}

        for name in files_to_extract:
            # search subdirectories for file with name
            # so I don't have to worry about figuring out the subdirs
            zipped_filename = find_file(name, zipstream)
            if zipped_filename:
                ret_files[name] = zipstream.open(zipped_filename).read()
        return ret_files

    def unzip_s3_to_s3(self, zipped_s3key, dest_dir, acl=None, store_results=True):
        """stream the content of a zipped key on S3 to another location on S3.
        if store_results=True, it saves the content and returns it in the dictionary format
        (default)
        """

        if not dest_dir.endswith('/'):
            dest_dir += '/'

        s3_stream = self.read_s3(zipped_s3key)
        # read this badboy to memory, don't go to disk
        bytestream = BytesIO(s3_stream)
        zipstream = ZipFile(bytestream, 'r')

        # The contents of zip can sometimes be like
        # ["foo/", "file1", "file2", "file3"]
        # and other times like
        # ["file1", "file2", "file3"]
        file_list = zipstream.namelist()
        if file_list[0].endswith('/'):
            # in case directory first name in the list
            basedir_name = file_list.pop(0)
        else:
            basedir_name = ''

        ret_files = {}
        for file_name in file_list:
            # don't copy dirs just files
            if not file_name.endswith('/'):
                if basedir_name:
                    s3_file_name = file_name.replace(basedir_name, dest_dir)
                else:
                    s3_file_name = dest_dir + file_name
                s3_key = "https://s3.amazonaws.com/%s/%s" % (self.outfile_bucket, s3_file_name)
                # just perf optimization so we don't have to copy
                # files twice that we want to further interrogate
                the_file = zipstream.open(file_name, 'r').read()
                file_to_find = os.path.basename(file_name)
                if store_results:
                    ret_files[file_to_find] = {'s3key': s3_key,
                                               'data': the_file}
                self.s3_put(the_file, s3_file_name, acl=acl)

        return ret_files


def find_file(name, zipstream):
    for zipped_filename in zipstream.namelist():
        if zipped_filename.endswith(name):
            return zipped_filename
